{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Suspend            (msDelay)
import           Control.Exception                     (handle, throwIO)
import           Control.Monad                         (forM, join, unless,
                                                        void, when)
import qualified Data.Aeson                            as Aeson
import           Data.Bifunctor
import           Data.ByteString                       (ByteString)
import           Data.Function                         (on, (&))
import           Data.Functor
import qualified Data.HashMap.Strict                   as HM
import           Data.IORef                            (atomicModifyIORef',
                                                        readIORef)
import           Data.Int                              (Int64)
import qualified Data.List                             as L
import qualified Data.Map.Strict                       as Map
import           Data.Maybe                            (catMaybes, fromJust,
                                                        isJust)
import           Data.Scientific
import           Data.String                           (fromString)
import qualified Data.Text                             as T
import qualified Data.Text.Lazy                        as TL
import qualified Data.Time                             as Time
import qualified Data.Vector                           as V
import           Data.Word                             (Word32)
import           HStream.Connector.HStore
import qualified HStream.Logger                        as Log
import           HStream.Processing.Connector          (SourceConnector (..))
import           HStream.Processing.Encoding           (Deserializer (..),
                                                        Serde (..),
                                                        Serializer (..))
import           HStream.Processing.Processor          (getTaskName)
import           HStream.Processing.Store
import           HStream.Processing.Stream             (Materialized (..))
import qualified HStream.Processing.Stream             as Processing
import           HStream.Processing.Stream.TimeWindows (mkTimeWindow,
                                                        mkTimeWindowKey)
import           HStream.Processing.Type               hiding (StreamName,
                                                        Timestamp)
import           HStream.Processing.Util               (getCurrentTimestamp)
import           HStream.SQL                           (parseAndRefine)
import           HStream.SQL.AST
import           HStream.SQL.Codegen                   hiding (StreamName)
import           HStream.SQL.ExecPlan                  (genExecutionPlan)
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector      (createConnector,
                                                        createSinkConnectorHandler,
                                                        deleteConnectorHandler,
                                                        getConnectorHandler,
                                                        listConnectorsHandler,
                                                        restartConnectorHandler,
                                                        terminateConnectorHandler)
import           HStream.Server.Handler.Query          (createQueryHandler,
                                                        deleteQueryHandler,
                                                        getQueryHandler,
                                                        listQueriesHandler,
                                                        restartQueryHandler,
                                                        terminateQueriesHandler)
import           HStream.Server.Handler.StoreAdmin     (getStoreNodeHandler,
                                                        listStoreNodesHandler)
import           HStream.Server.Handler.View           (createViewHandler,
                                                        deleteViewHandler,
                                                        getViewHandler,
                                                        listViewsHandler)
import qualified HStream.Server.Persistence            as P
import           HStream.Store                         (ckpReaderStopReading)
import qualified HStream.Store                         as S
import qualified HStream.Store.Admin.API               as AA
import           HStream.ThirdParty.Protobuf           as PB
import           HStream.Utils
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                          (Enumerated (..),
                                                        HasDefault (def))
import qualified Z.Data.CBytes                         as CB
import           Z.Data.Vector                         (Bytes)
import           Z.Foreign                             (toByteString)
import           Z.IO.LowResTimer                      (registerLowResTimer)
import qualified Z.IO.Time                             as ZT
import           ZooKeeper.Types                       (ZHandle)

--------------------------------------------------------------------------------

handlers
  :: S.LDClient
  -> AA.HeaderConfig AA.AdminAPI
  -> Int
  -> Maybe ZHandle
  -> Int64    -- ^ timer timeout, ms
  -> S.Compression
  -> IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient headerConfig repFactor zkHandle timeout compression = do
  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subscriptions <- newMVar HM.empty
  subscribeRuntimeInfo <- newMVar HM.empty
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = repFactor
      , zkHandle                 = zkHandle
      , runningQueries           = runningQs
      , runningConnectors        = runningCs
      , subscriptions            = subscriptions
      , subscribeRuntimeInfo     = subscribeRuntimeInfo
      , cmpStrategy              = compression
      , headerConfig             = headerConfig
      }
  -- timer <- newTimer
  -- _ <- repeatedStart timer (checkSubscriptions timeout serverContext) (msDelay timeout)
  return HStreamApi {
      hstreamApiEcho = echoHandler

      -- Streams
    , hstreamApiCreateStream = createStreamHandler serverContext
    , hstreamApiDeleteStream = deleteStreamHandler serverContext
    , hstreamApiListStreams  = listStreamsHandler serverContext
    , hstreamApiAppend       = appendHandler serverContext

    -- Subscribe
    , hstreamApiCreateSubscription = createSubscriptionHandler serverContext
    , hstreamApiSubscribe          = subscribeHandler serverContext
    , hstreamApiDeleteSubscription = deleteSubscriptionHandler serverContext
    , hstreamApiListSubscriptions  = listSubscriptionsHandler serverContext
    , hstreamApiCheckSubscriptionExist = checkSubscriptionExistHandler serverContext

    -- Consume
    , hstreamApiFetch        = fetchHandler serverContext
    , hstreamApiSendConsumerHeartbeat = consumerHeartbeatHandler serverContext
    , hstreamApiAcknowledge = ackHandler serverContext
    , hstreamApiStreamingFetch = streamingFetchHandler serverContext

    , hstreamApiExecuteQuery     = executeQueryHandler serverContext
    , hstreamApiExecutePushQuery = executePushQueryHandler serverContext

    -- Query
    , hstreamApiTerminateQueries = terminateQueriesHandler serverContext

    -- Stream with Query
    , hstreamApiCreateQueryStream = createQueryStreamHandler serverContext

      -- FIXME:
    , hstreamApiCreateQuery  = createQueryHandler serverContext
    , hstreamApiGetQuery     = getQueryHandler serverContext
    , hstreamApiListQueries  = listQueriesHandler serverContext
    , hstreamApiDeleteQuery  = deleteQueryHandler serverContext
    , hstreamApiRestartQuery = restartQueryHandler serverContext

    , hstreamApiCreateSinkConnector  = createSinkConnectorHandler serverContext
    , hstreamApiGetConnector         = getConnectorHandler serverContext
    , hstreamApiListConnectors       = listConnectorsHandler serverContext
    , hstreamApiDeleteConnector      = deleteConnectorHandler serverContext
    , hstreamApiTerminateConnector   = terminateConnectorHandler serverContext
    , hstreamApiRestartConnector     = restartConnectorHandler serverContext

    , hstreamApiCreateView       = createViewHandler serverContext
    , hstreamApiGetView          = getViewHandler serverContext
    , hstreamApiListViews        = listViewsHandler serverContext
    , hstreamApiDeleteView       = deleteViewHandler serverContext

    , hstreamApiGetNode          = getStoreNodeHandler serverContext
    , hstreamApiListNodes        = listStoreNodesHandler serverContext
    }

-------------------------------------------------------------------------------

echoHandler
  :: ServerRequest 'Normal EchoRequest EchoResponse
  -> IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest{..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""

-------------------------------------------------------------------------------
-- Stream

createStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamHandler ServerContext{..} (ServerNormalRequest _metadata stream@Stream{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Stream Request: New Stream Name: "
    <> Log.buildString (TL.unpack streamStreamName)
  S.createStream scLDClient (transToStreamName $ TL.toStrict streamStreamName)
    $ S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  returnResp stream

deleteStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteStreamRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc (ServerNormalRequest _metadata DeleteStreamRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete Stream Request: Stream to Delete: "
    <> Log.buildString (TL.unpack deleteStreamRequestStreamName)
  let name = TL.toStrict deleteStreamRequestStreamName
  dropHelper sc name deleteStreamRequestIgnoreNonExist False

listStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListStreamsRequest ListStreamsResponse
  -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext{..} (ServerNormalRequest _metadata ListStreamsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive List Stream Request"
  streams <- S.findStreams scLDClient S.StreamTypeStream True
  res <- V.forM (V.fromList streams) $ \stream -> do
    refactor <- S.getStreamReplicaFactor scLDClient stream
    return $ Stream (TL.pack . S.showStreamName $ stream) (fromIntegral refactor)
  returnResp $ ListStreamsResponse res

appendHandler
  :: ServerContext
  -> ServerRequest 'Normal AppendRequest AppendResponse
  -> IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext{..} (ServerNormalRequest _metadata AppendRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Append Stream Request. Append Data to the Stream: "
    <> Log.buildString (TL.unpack appendRequestStreamName)
  timestamp <- getProtoTimestamp
  let payloads = V.toList $ encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
  S.AppendCompletion{..} <- batchAppend scLDClient appendRequestStreamName payloads cmpStrategy
  let records = V.zipWith (\_ idx -> RecordId appendCompLSN idx) appendRequestRecords [0..]
  returnResp $ AppendResponse appendRequestStreamName records

-------------------------------------------------------------------------------
-- Stream with Select Query

createQueryStreamHandler :: ServerContext
  -> ServerRequest 'Normal CreateQueryStreamRequest CreateQueryStreamResponse
  -> IO (ServerResponse 'Normal CreateQueryStreamResponse)
createQueryStreamHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata CreateQueryStreamRequest {..}) = defaultExceptionHandle $ do
  RQSelect select <- parseAndRefine $ TL.toStrict createQueryStreamRequestQueryStatements
  tName <- genTaskName
  let sName = TL.toStrict . streamStreamName
          <$> createQueryStreamRequestQueryStream
      rFac = maybe 1 (fromIntegral . streamReplicationFactor) createQueryStreamRequestQueryStream
  (builder, source, sink, _)
    <- genStreamBuilderWithStream tName sName select
  S.createStream scLDClient (transToStreamName sink) $ S.LogAttrs (S.HsLogAttrs rFac Map.empty)
  let query = P.StreamQuery (textToCBytes <$> source) (textToCBytes sink)
  void $ handleCreateAsSelect sc (Processing.build builder)
    createQueryStreamRequestQueryStatements query S.StreamTypeStream
  let streamResp = Stream (TL.fromStrict sink) (fromIntegral rFac)
  -- FIXME: The value query returned should have been fully assigned
      queryResp  = def { queryId = TL.fromStrict tName }
  returnResp $ CreateQueryStreamResponse (Just streamResp) (Just queryResp)

--------------------------------------------------------------------------------

executeQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CommandQuery CommandQueryResponse
  -> IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata CommandQuery{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Query Request: " <> Log.buildString (TL.unpack commandQueryStmtText)
  plan' <- streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    SelectPlan{} -> returnErrResp StatusInternal "inconsistent method called"
    -- execute plans that can be executed with this method
    CreateViewPlan schema sources sink taskBuilder _repFactor materialized -> do
      create (transToViewStreamName sink)
      >> handleCreateAsSelect sc taskBuilder commandQueryStmtText
        (P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema) S.StreamTypeView
      >> atomicModifyIORef' groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
      >> returnCommandQueryEmptyResp
    CreateSinkConnectorPlan _cName _ifNotExist _sName _cConfig _ -> do
      createConnector sc (TL.toStrict commandQueryStmtText) >> returnCommandQueryEmptyResp
    SelectViewPlan RSelectView{..} -> do
      hm <- readIORef groupbyStores
      case HM.lookup rSelectViewFrom hm of
        Nothing -> returnErrResp StatusInternal "VIEW not found"
        Just materialized -> do
          let (keyName, keyExpr) = rSelectViewWhere
              (_,keyValue) = genRExprValue keyExpr (HM.fromList [])
          let keySerde   = mKeySerde materialized
              valueSerde = mValueSerde materialized
          let key = runSer (serializer keySerde) (HM.fromList [(keyName, keyValue)])
          case mStateStore materialized of
            KVStateStore store -> do
              queries <- P.withMaybeZHandle zkHandle P.getQueries
              sizeM   <- getFixedWinSize queries rSelectViewFrom
                <&> fmap diffTimeToScientific
              if isJust sizeM
                then do
                  let size = fromJust sizeM & fromJust . toBoundedInteger @Int64
                  subset <- ksDump store
                    <&> Map.filterWithKey
                      (\k _ -> all (`elem` HM.toList k) (HM.toList key))
                    <&> Map.toList
                  let winStarts = subset
                        <&> (lookup "winStart" . HM.toList) . fst
                         &  L.sort . L.nub . catMaybes
                      singlWinStart = subset
                        <&> first (filter (\(k, _) -> k == "winStart") . HM.toList)
                        <&> first HM.fromList
                      grped = winStarts <&> \winStart ->
                        let Aeson.Number winStart'' = winStart
                            winStart' = fromJust . toBoundedInteger @Int64 $ winStart'' in
                        ("winStart = " <>
                          (T.pack . show) winStart' <> " ,winEnd = " <> (T.pack . show) (winStart' + size)
                        , lookup (HM.fromList [("winStart", winStart)]) singlWinStart
                          & fromJust & Aeson.Object)
                  sendResp (Just $ HM.fromList grped) valueSerde
                else ksGet key store >>= flip sendResp valueSerde
            SessionStateStore store -> do
              dropSurfaceTimeStamp <- ssDump store <&> Map.elems
              let subset = dropSurfaceTimeStamp <&> Map.elems .
                    Map.filterWithKey \k _ -> all (`elem` HM.toList k) (HM.toList key)
              let res = subset
                     &  filter (not . null) . join
                    <&> Map.toList
                     &  L.sortBy (compare `on` fst) . filter (not . null) . join
              flip sendResp valueSerde $ Just . HM.fromList $
                res <&> \(k, v) -> ("winStart = " <> (T.pack . show) k, Aeson.Object v)
            TimestampedKVStateStore _ ->
              returnErrResp StatusInternal "Impossible happened"
    ExplainPlan sql -> do
      execPlan <- genExecutionPlan sql
      let object = HM.fromList [("PLAN", Aeson.String . T.pack $ show execPlan)]
      returnCommandQueryResp $ V.singleton (jsonObjectToStruct object)
    _ -> discard
  where
    mkLogAttrs = S.HsLogAttrs scDefaultStreamRepFactor
    create sName = do
      let attrs = mkLogAttrs Map.empty
      Log.debug . Log.buildString
         $ "CREATE: new stream " <> show sName
        <> " with attributes: " <> show attrs
      S.createStream scLDClient sName (S.LogAttrs attrs)
    sendResp ma valueSerde = do
      case ma of
        Nothing -> returnCommandQueryResp V.empty
        Just x  -> do
          let result = runDeser (deserializer valueSerde) x
          returnCommandQueryResp
            (V.singleton $ structToStruct "SELECTVIEW" $ jsonObjectToStruct result)
    discard = (Log.e . Log.buildText) "impossible happened" >> returnErrResp StatusInternal "discarded method called"


executePushQueryHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming CommandPushQuery Struct
  -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..}
  (ServerWriterRequest meta CommandPushQuery{..} streamSend) = defaultStreamExceptionHandle $ do
  Log.debug $ "Receive Push Query Request: " <> Log.buildString (TL.unpack commandPushQueryQueryText)
  plan' <- streamCodegen (TL.toStrict commandPushQueryQueryText)
  case plan' of
    SelectPlan sources sink taskBuilder -> do
      exists <- mapM (S.doesStreamExists scLDClient . transToStreamName) sources
      if (not . and) exists
      then do
        Log.warning $ "At least one of the streams do not exist: "
          <> Log.buildString (show sources)
        throwIO StreamNotExist
      else do
        S.createStream scLDClient (transToTempStreamName sink)
          (S.LogAttrs $ S.HsLogAttrs scDefaultStreamRepFactor Map.empty)
        -- create persistent query
        (qid, _) <- P.createInsertPersistentQuery (getTaskName taskBuilder)
          (TL.toStrict commandPushQueryQueryText) (P.PlainQuery $ textToCBytes <$> sources) zkHandle
        -- run task
        -- FIXME: take care of the life cycle of the thread and global state
        tid <- forkIO $ P.withMaybeZHandle zkHandle (P.setQueryStatus qid Running)
          >> runTaskWrapper S.StreamTypeStream S.StreamTypeTemp taskBuilder scLDClient
        takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
        _ <- forkIO $ handlePushQueryCanceled meta
          (killThread tid >> P.withMaybeZHandle zkHandle (P.setQueryStatus qid Terminated))
        ldreader' <- S.newLDRsmCkpReader scLDClient
          (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
          S.checkpointStoreLogID 5000 1 Nothing 10
        let sc = hstoreSourceConnector scLDClient ldreader' S.StreamTypeTemp
        subscribeToStream sc sink Latest
        sendToClient zkHandle qid sc streamSend
    _ -> do
      Log.fatal "Push Query: Inconsistent Method Called"
      returnStreamingResp StatusInternal "inconsistent method called"

--------------------------------------------------------------------------------

sendToClient :: Maybe ZHandle
             -> CB.CBytes
             -> SourceConnector
             -> (Struct -> IO (Either GRPCIOError ()))
             -> IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid sc@SourceConnector{..} streamSend = do
  let f (e :: P.ZooException) = do
        Log.fatal $ "ZooKeeper Exception: " <> Log.buildString (show e)
        return $ ServerWriterResponse [] StatusAborted "failed to get status"
  handle f $ do
    P.withMaybeZHandle zkHandle $ P.getQueryStatus qid
    >>= \case
      Terminated -> return (ServerWriterResponse [] StatusUnknown "")
      Created    -> return (ServerWriterResponse [] StatusUnknown "")
      Running    -> do
        sourceRecords <- readRecords
        let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
            structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
        streamSendMany structs
  where
    streamSendMany = \case
      []      -> sendToClient zkHandle qid sc streamSend
      (x:xs') -> streamSend (structToStruct "SELECT" x) >>= \case
        Left err -> do Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
                       return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
        Right _  -> streamSendMany xs'

--------------------------------------------------------------------------------
-- Subscribe

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata subscription@Subscription{..}) =  defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString (show subscription)

  let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
  exists <- S.doesStreamExists scLDClient streamName
  if not exists
  then do
     Log.warning $ "Try to create a subscription to a nonexistent stream"
                <> "Stream Name: " <> Log.buildString (show streamName)
     returnErrResp StatusInternal $ StatusDetails "stream not exist"
  else
    modifyMVar
      subscriptions
      (
        \store ->
          if HM.member subscriptionSubscriptionId store
          then do
            errResp <- returnErrResp StatusInternal $ StatusDetails "SubscriptionId has been used."
            return (store, errResp)
          else do
            logId <- S.getUnderlyingLogId scLDClient streamName
            let subOffset = fromJust subscriptionOffset
            recordId <- toConcreteRecordId subOffset scLDClient logId
            let newsub = subscription {subscriptionOffset = Just $ SubscriptionOffset (Just $ SubscriptionOffsetOffsetRecordOffset recordId)}
            let newStore = HM.insert subscriptionSubscriptionId newsub store
            resp <- returnResp subscription
            return (newStore, resp)
      )
  where
    toConcreteRecordId :: SubscriptionOffset -> S.LDClient -> S.C_LogID -> IO RecordId
    toConcreteRecordId SubscriptionOffset{..} client logId =
      case fromJust subscriptionOffsetOffset of
        SubscriptionOffsetOffsetSpecialOffset subOffset ->
          case subOffset of
            Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) ->
              return $ RecordId S.LSN_MIN 0
            Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST)  -> do
              tailLSN <- S.getTailLSN client logId
              return $ RecordId (tailLSN + 1) 0
            Enumerated _                                               ->
              error "Wrong SpecialOffset!"
        SubscriptionOffsetOffsetRecordOffset recordId -> return recordId

subscribeHandler
  :: ServerContext
  -> ServerRequest 'Normal SubscribeRequest SubscribeResponse
  -> IO (ServerResponse 'Normal SubscribeResponse)
subscribeHandler ServerContext{..} (ServerNormalRequest _metadata req@SubscribeRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive subscribe request: " <> Log.buildString (show req)

  modifyMVar
    subscribeRuntimeInfo
    (
      \store ->
        if HM.member subscribeRequestSubscriptionId store
        then do
          resp <- returnResp (SubscribeResponse subscribeRequestSubscriptionId)
          return (store, resp)
        else do
          -- create a new ldreader for subscription
          ldreader <- S.newLDRsmCkpReader scLDClient (textToCBytes $ TL.toStrict subscribeRequestSubscriptionId)
              S.checkpointStoreLogID 5000 1 Nothing 10
          -- seek ldreader to start offset
          streamName <- getStreamName subscribeRequestSubscriptionId subscriptions
          logId <- S.getUnderlyingLogId scLDClient (transToStreamName (TL.toStrict streamName))
          startOffset <- getSubscriptionOffset subscribeRequestSubscriptionId
          let startLSN = getStartLSN startOffset
          S.ckpReaderStartReading ldreader logId startLSN S.LSN_MAX
          Log.d $ Log.buildString "createSubscribe with startLSN: " <> Log.buildInt startLSN
          -- insert to runtime info
          let info = SubscribeRuntimeInfo {
                      sriLdreader = ldreader
                    , sriWindowLowerBound = getStartOffset startOffset
                    , sriWindowUpperBound = maxBound
                    , sriAckedRanges = Map.empty
                    , sriBatchNumMap = Map.empty
                    }
          mvar <- newMVar info
          let newStore = HM.insert subscribeRequestSubscriptionId mvar store
          resp <- returnResp (SubscribeResponse subscribeRequestSubscriptionId)
          return (newStore, resp)
    )
  where
    getSubscriptionOffset :: SubscriptionId -> IO SubscriptionOffset
    getSubscriptionOffset subscriptionId =
      withMVar
        subscriptions
        (
          \store -> do
            let Subscription{..} = fromJust $ HM.lookup subscriptionId store
            return $ fromJust subscriptionOffset
        )

    getStartOffset:: SubscriptionOffset -> RecordId
    getStartOffset SubscriptionOffset{..} = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset _ -> error "shoud not reach here"
      SubscriptionOffsetOffsetRecordOffset r  -> r

    getStartLSN :: SubscriptionOffset -> S.LSN
    getStartLSN SubscriptionOffset{..} = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset _ -> error "shoud not reach here"
      SubscriptionOffsetOffsetRecordOffset RecordId{..} -> recordIdBatchId

getStreamName :: SubscriptionId -> MVar (HM.HashMap SubscriptionId Subscription)-> IO TL.Text
getStreamName subscriptionId mStore =
  withMVar
    mStore
    (
      \store -> do
        let Subscription{..} = fromJust $ HM.lookup subscriptionId store
        return subscriptionStreamName
    )

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString (show req)

  mSubscription <- modifyMVar
    subscriptions
    (
      \store -> do
        case HM.lookup deleteSubscriptionRequestSubscriptionId store of
          Just sub -> do
            let newStore = HM.delete deleteSubscriptionRequestSubscriptionId store
            return (newStore, Just sub)
          Nothing -> return (store, Nothing)
    )

  case mSubscription of
    Just Subscription{..} -> do
      modifyMVar_
        subscribeRuntimeInfo
        (
          \store ->
            case HM.lookup deleteSubscriptionRequestSubscriptionId store of
              Just infoMVar -> do
                withMVar
                  infoMVar
                  (
                    \SubscribeRuntimeInfo{..} -> do
                      -- stop ldreader
                      let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
                      exists <- S.doesStreamExists scLDClient streamName
                      when exists $ do
                        logId <- S.getUnderlyingLogId scLDClient streamName
                        ckpReaderStopReading sriLdreader logId
                  )
                return $ HM.delete deleteSubscriptionRequestSubscriptionId store
              Nothing -> return store
        )
    Nothing -> return ()

  returnResp Empty

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext{..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest{..})= do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)

  withMVar
    subscriptions
    (
        returnResp . CheckSubscriptionExistResponse . HM.member checkSubscriptionExistRequestSubscriptionId
    )

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler ServerContext{..} (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"

  withMVar
    subscriptions
    (
        returnResp . ListSubscriptionsResponse . HM.foldl' V.snoc V.empty
    )

--------------------------------------------------------------------------------
-- Comsumer

-- do nothing now
consumerHeartbeatHandler
  :: ServerContext
  -> ServerRequest 'Normal ConsumerHeartbeatRequest ConsumerHeartbeatResponse
  -> IO (ServerResponse 'Normal ConsumerHeartbeatResponse)
consumerHeartbeatHandler ServerContext{..} (ServerNormalRequest _metadata ConsumerHeartbeatRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive heartbeat msg for " <> Log.buildLazyText consumerHeartbeatRequestSubscriptionId
  returnResp $ ConsumerHeartbeatResponse consumerHeartbeatRequestSubscriptionId

fetchHandler
  :: ServerContext
  -> ServerRequest 'Normal FetchRequest FetchResponse
  -> IO (ServerResponse 'Normal FetchResponse)
fetchHandler ServerContext{..} (ServerNormalRequest _metadata req@FetchRequest{..}) = defaultExceptionHandle $  do
  Log.debug $ "Receive fetch request: " <> Log.buildString (show req)

  mRes <- withMVar
    subscribeRuntimeInfo
    (
        return . HM.lookup fetchRequestSubscriptionId
    )

  case mRes of
    Just infoMVar ->
      modifyMVar
        infoMVar
        (
          \info@SubscribeRuntimeInfo{..} -> do
            void $ S.ckpReaderSetTimeout sriLdreader (fromIntegral fetchRequestTimeout)
            res <- S.ckpReaderReadAllowGap sriLdreader (fromIntegral fetchRequestMaxSize)
            case res of
              Left S.GapRecord{..} -> do
                -- insert gap range to ackedRanges
                let gapLoRecordId = RecordId gapLoLSN minBound
                let gapHiRecordId = RecordId gapHiLSN maxBound
                let newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) sriAckedRanges
                let newInfo = info {sriAckedRanges = newRanges}
                resp <- returnResp $ FetchResponse V.empty
                return (newInfo, resp)
              Right dataRecords -> do
                let groups = L.groupBy ((==) `on` S.recordLSN) dataRecords
                let groupNums = map (\group -> (S.recordLSN $ head group, (fromIntegral $ length group) :: Word32)) groups
                let lastBatch = last groups
                let maxRecordId = RecordId (S.recordLSN $ head lastBatch) (fromIntegral $ length lastBatch - 1)
                -- update window upper bound
                -- update batchNumMap
                let newBatchNumMap = Map.union sriBatchNumMap (Map.fromList groupNums)
                let newInfo = info {sriBatchNumMap = newBatchNumMap, sriWindowUpperBound = maxRecordId}
                resp <- returnResp $ FetchResponse (fetchResult groups)
                return (newInfo, resp)
        )
    Nothing ->
      returnErrResp StatusInternal (StatusDetails "subscription do not exist")
  where
    fetchResult :: [[S.DataRecord Bytes]] -> V.Vector ReceivedRecord
    fetchResult groups = V.fromList $ concatMap (zipWith mkReceivedRecord [0..]) groups

    mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
    mkReceivedRecord index record =
      let recordId = RecordId (S.recordLSN record) (fromIntegral index)
      in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

ackHandler
  :: ServerContext
  -> ServerRequest 'Normal AcknowledgeRequest Empty
  -> IO (ServerResponse 'Normal Empty)
ackHandler ServerContext{..} (ServerNormalRequest _metadata req@AcknowledgeRequest{..}) = defaultExceptionHandle $  do
  Log.debug $ "Receive ack request: " <> Log.buildString (show req)

  mRes <- withMVar
    subscribeRuntimeInfo
    (
        return . HM.lookup acknowledgeRequestSubscriptionId
    )

  case mRes of
    Just infoMVar ->
      modifyMVar
        infoMVar
        (
          \info@SubscribeRuntimeInfo{..} -> do
            let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b a sriBatchNumMap) sriAckedRanges acknowledgeRequestAckIds
            case tryUpdateWindowLowerBound newAckedRanges sriWindowLowerBound sriBatchNumMap of
              Just (ranges, newLowerBound, checkpointRecordId) -> do
                streamName <- getStreamName acknowledgeRequestSubscriptionId subscriptions
                commitCheckpoint scLDClient sriLdreader streamName checkpointRecordId
                let newInfo = info {sriAckedRanges = ranges, sriWindowLowerBound = newLowerBound}
                resp <- returnResp Empty
                return (newInfo, resp)
              Nothing -> do
                let newInfo = info {sriAckedRanges = newAckedRanges}
                resp <- returnResp Empty
                return (newInfo, resp)
        )
    Nothing -> error "should not reach here"
  where
    tryUpdateWindowLowerBound ackedRanges lowerBoundRecordId batchNumMap =
      let (_, RecordIdRange minStartRecordId minEndRecordId) = Map.findMin ackedRanges
      in
          if minStartRecordId == lowerBoundRecordId
          then
            Just (Map.delete minStartRecordId ackedRanges, getSuccessor minEndRecordId batchNumMap, minEndRecordId)
          else
            Nothing

    commitCheckpoint :: S.LDClient -> S.LDSyncCkpReader -> TL.Text -> RecordId -> IO ()
    commitCheckpoint client reader streamName RecordId{..} = do
      logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
      S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming  StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ServerContext{..} (ServerBiDiRequest metadata streamRecv streamSend) = do
  Log.debug "Receive streamingFetch request"
  handleRequest
  where
    handleRequest = do
      eRes <- streamRecv
      case eRes of
        Left grpcIOError -> do
          Log.fatal "streamRecv error"
          -- fixme here
          return $ ServerBiDiResponse [] StatusOk (StatusDetails "")
        Right ma ->
          case ma of
            Just req@StreamingFetchRequest{..} -> do
              -- TODO: check subscription whether exsits first

              isInited <- withMVar
                subscribeRuntimeInfo
                (
                  return . (HM.member streamingFetchRequestSubscriptionId)
                )

              -- avoid nested locks for preventing deadlock
              mRes <-
                if isInited
                then return Nothing
                else do
                  withMVar
                    subscriptions
                    (
                      \store ->
                        case HM.lookup streamingFetchRequestSubscriptionId store of
                          Just Subscription{..} ->
                            return $ Just (subscriptionStreamName, getStartRecordId $ fromJust subscriptionOffset)
                    )

              case mRes of
                Nothing -> return ()
                Just (streamName, startRecordId) ->
                  modifyMVar_
                    subscribeRuntimeInfo
                    (
                      \store ->
                        case HM.lookup streamingFetchRequestSubscriptionId store of
                          Just _ -> return store
                          Nothing -> do
                            newInfoMVar <- initSubscribe scLDClient streamingFetchRequestSubscriptionId streamName startRecordId streamSend
                            return $ HM.insert streamingFetchRequestSubscriptionId newInfoMVar store
                    )

              infoMVar <-
                withMVar
                  subscribeRuntimeInfo
                  (
                    -- At this point, the corresponding subscribeRuntimeInfo must be
                    -- present, unless the subscription has been removed
                    -- forcely, which we will handle later(TODO).
                    return . fromJust . (HM.lookup streamingFetchRequestSubscriptionId)
                  )

              -- TODO: handle ack
              -- modifyMVar_
              --   infoMVar
              --   (
              --     \info -> do
              --       handleAck
              --       return newInfo
              --   )

              handleRequest

            Nothing ->
              -- This means that the consumer finished sending acks actively,
              -- in fact, it should never happen.
              return $ ServerBiDiResponse [] StatusOk (StatusDetails "")

    -- return: IO (MVar SubscribeRuntimeInfo)
    initSubscribe ldclient subscriptionId streamName startRecordId streamSend = do
      -- create a ldCkpReader for reading new records
      ldCkpReader <-
        S.newLDRsmCkpReader
          ldclient
          (textToCBytes $ TL.toStrict subscriptionId)
          S.checkpointStoreLogID 5000 1 Nothing 10
      -- seek ldCkpReader to start offset
      logId <- S.getUnderlyingLogId ldclient (transToStreamName (TL.toStrict streamName))
      let startLSN = recordIdBatchId startRecordId
      S.ckpReaderStartReading ldCkpReader logId startLSN S.LSN_MAX
      -- set ldCkpReader timeout to 0
      S.ckpReaderSetTimeout ldCkpReader 0
      Log.d $ Log.buildString "created a ldCkpReader for subscription {" <> Log.buildLazyText subscriptionId <> "} with startLSN {" <> Log.buildInt startLSN <> "}"

      -- create a ldReader for rereading unacked records(TODO)
      -- ldReader <-
      --   S.newLDReader
      --     ldclient
      --     (textToCBytes $ TL.toStrict subscriptionId)
      -- Log.d $ Log.buildString "created a ldReader for subscription {" <> subscriptionId <> "}"

      -- init SubscribeRuntimeInfo
      let info = SubscribeRuntimeInfo {
                  sriLdreader = ldCkpReader
                , sriWindowLowerBound = startRecordId
                , sriWindowUpperBound = maxBound
                , sriAckedRanges = Map.empty
                , sriBatchNumMap = Map.empty
                , sriStreamSends = V.singleton streamSend
                }

      infoMVar <- newMVar info
      -- create a task for reading and dispatching records periodicly
      forkIO $ readAndDispatchRecords infoMVar
      return infoMVar

    readAndDispatchRecords runtimeInfoMVar = do
      Log.d $ Log.buildString "enter readAndDispatchRecords"
      -- register for next readAndDispatch
      _ <- registerLowResTimer 1
        (
          do
            _ <- forkIO $ readAndDispatchRecords runtimeInfoMVar
            return ()
        )
      modifyMVar_
        runtimeInfoMVar
        (
          \info@SubscribeRuntimeInfo{..} -> do
            res <- S.ckpReaderReadAllowGap sriLdreader 1000
            case res of
              Left S.GapRecord{..} -> do
                -- insert gap range to ackedRanges
                let gapLoRecordId = RecordId gapLoLSN minBound
                let gapHiRecordId = RecordId gapHiLSN maxBound
                let newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) sriAckedRanges
                let newInfo = info {sriAckedRanges = newRanges}
                return newInfo
              Right dataRecords -> do
                let groups = L.groupBy ((==) `on` S.recordLSN) dataRecords
                let groupNums = map (\group -> (S.recordLSN $ head group, (fromIntegral $ length group) :: Word32)) groups
                let lastBatch = last groups
                let maxRecordId = RecordId (S.recordLSN $ head lastBatch) (fromIntegral $ length lastBatch - 1)
                -- update window upper bound
                -- update batchNumMap
                let newBatchNumMap = Map.union sriBatchNumMap (Map.fromList groupNums)
                let newInfo = info {sriBatchNumMap = newBatchNumMap, sriWindowUpperBound = maxRecordId}
                -- dispatch records to consumers
                unless (null dataRecords) $
                  dispatchRecords (fetchResult groups) sriStreamSends
                -- register task for ack timeout resend(TODO)

                return newInfo

        )

    -- round-robin dispatch
    dispatchRecords records streamSends = do
      let slen = V.length streamSends
      let initVec = V.replicate slen V.empty
      let recordGroups =
            V.ifoldl'
              (
                \v i r ->
                  let ci = i `mod` slen
                      og = v V.! ci
                      ng = V.snoc og r
                  in
                      V.update v $ V.singleton (ci, ng)
              )
              initVec
              records

      V.imapM_
        (
          \ i group -> do
            let ss = streamSends V.! i
            ss $ StreamingFetchResponse group
        )
        recordGroups

    getStartRecordId :: SubscriptionOffset -> RecordId
    getStartRecordId SubscriptionOffset{..} = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset _ -> error "shoud not reach here"
      SubscriptionOffsetOffsetRecordOffset r  -> r

    fetchResult :: [[S.DataRecord Bytes]] -> V.Vector ReceivedRecord
    fetchResult groups = V.fromList $ concatMap (zipWith mkReceivedRecord [0..]) groups

    mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
    mkReceivedRecord index record =
      let recordId = RecordId (S.recordLSN record) (fromIntegral index)
      in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

--------------------------------------------------------------------------------
--

batchAppend :: S.LDClient -> TL.Text -> [ByteString] -> S.Compression -> IO S.AppendCompletion
batchAppend client streamName payloads strategy = do
  logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
  S.appendBatchBS client logId payloads strategy Nothing

--------------------------------------------------------------------------------

getFixedWinSize :: [P.PersistentQuery] -> T.Text -> IO (Maybe Time.DiffTime)
getFixedWinSize [] _ = pure Nothing
getFixedWinSize queries viewNameRaw = do
  sizes <- queries <&> P.queryBindedSql
    <&> parseAndRefine . cBytesToText . CB.fromText
     &  sequence
    <&> filter \case
      RQCreate (RCreateView viewNameSQL (RSelect _ _ _ (RGroupBy _ _ (Just rWin)) _)) ->
        viewNameRaw == viewNameSQL && isFixedWin rWin
      _ -> False
    <&> map \case
      RQCreate (RCreateView _ (RSelect _ _ _ (RGroupBy _ _ (Just rWin)) _)) ->
        coeRWindowToDiffTime rWin
      _ -> error "Impossible happened..."
  pure case sizes of
    []       -> Nothing
    size : _ -> Just size
  where
    isFixedWin :: RWindow -> Bool = \case
      RTumblingWindow  _ -> True
      RHoppingWIndow _ _ -> True
      RSessionWindow   _ -> False
    coeRWindowToDiffTime :: RWindow -> Time.DiffTime = \case
      RTumblingWindow  size -> size
      RHoppingWIndow size _ -> size
      RSessionWindow      _ -> error "Impossible happened..."

diffTimeToScientific :: Time.DiffTime -> Scientific
diffTimeToScientific = flip scientific (-9) . Time.diffTimeToPicoseconds
