{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Suspend            (msDelay)
import           Control.Concurrent.Timer              (newTimer, repeatedStart)
import           Control.Exception                     (handle, throwIO)
import           Control.Monad                         (void, when)
import qualified Data.Aeson                            as Aeson
import           Data.ByteString                       (ByteString)
import           Data.Function                         (on)
import qualified Data.HashMap.Strict                   as HM
import           Data.IORef                            (atomicModifyIORef',
                                                        readIORef)
import           Data.Int                              (Int64)
import qualified Data.List                             as L
import qualified Data.Map.Strict                       as Map
import           Data.Maybe                            (fromJust, isJust)
import           Data.String                           (fromString)
import qualified Data.Text                             as T
import qualified Data.Text.Lazy                        as TL
import qualified Data.Vector                           as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                          (Enumerated (..),
                                                        HasDefault (def))
import qualified Z.Data.CBytes                         as CB
import           Z.Data.Vector                         (Bytes)
import           Z.Foreign                             (toByteString)
import           ZooKeeper.Types                       (ZHandle)

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
import           HStream.SQL                           (RSQL (RQSelect),
                                                        parseAndRefine)
import           HStream.SQL.AST                       (RSelectView (..))
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
import qualified HStream.Store.Logger                  as S
import           HStream.ThirdParty.Protobuf           as PB
import           HStream.Utils

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
  subscriptions <- newTVarIO HM.empty
  subscribeRuntimeInfo <- newTVarIO HM.empty
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
    , hstreamApiCommitOffset = commitOffsetHandler serverContext
    , hstreamApiSendConsumerHeartbeat = consumerHeartbeatHandler serverContext

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
            KVStateStore store        -> do
              ma <- ksGet key store
              sendResp ma valueSerde
            SessionStateStore store   -> do
              timestamp <- getCurrentTimestamp
              let ssKey = mkTimeWindowKey key (mkTimeWindow timestamp timestamp)
              ma <- ssGet ssKey store
              sendResp ma valueSerde
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
        Log.fatal $ "At least one of the streams do not exist: "
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
        tid <- forkIO $ P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Running)
          >> runTaskWrapper S.StreamTypeStream S.StreamTypeTemp taskBuilder scLDClient
        takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
        _ <- forkIO $ handlePushQueryCanceled meta
          (killThread tid >> P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Terminated))
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
      P.Terminated -> return (ServerWriterResponse [] StatusUnknown "")
      P.Created    -> return (ServerWriterResponse [] StatusUnknown "")
      P.Running    -> do
        sourceRecords <- readRecords
        let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
            structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
        streamSendMany structs
  where
    streamSendMany = \case
      []      -> sendToClient zkHandle qid sc streamSend
      (x:xs') -> streamSend (structToStruct "SELECT" x) >>= \case
        Left err -> do Log.fatal $ "Send Stream Error: " <> Log.buildString (show err)
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
  then
     returnErrResp StatusInternal $ StatusDetails "stream not exist"
  else do
    logId <- S.getUnderlyingLogId scLDClient streamName
    let subOffset = fromJust subscriptionOffset
    recordId <- toConcreteRecordId subOffset scLDClient logId

    let newsub = subscription {subscriptionOffset = Just $ SubscriptionOffset (Just $ SubscriptionOffsetOffsetRecordOffset recordId)}

    success <- atomically $ do
      store <- readTVar subscriptions
      if HM.member subscriptionSubscriptionId store
      then return False
      else do
        let newStore = HM.insert subscriptionSubscriptionId newsub store
        writeTVar subscriptions newStore
        return True

    if success
    then returnResp subscription
    else
       returnErrResp StatusInternal $ StatusDetails "SubscriptionId has been used."
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

  isInited <- atomically $ do
    store <- readTVar subscribeRuntimeInfo
    if HM.member subscribeRequestSubscriptionId store
    then return True
    else return False

  if isInited
  then returnResp (SubscribeResponse subscribeRequestSubscriptionId)
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
    atomically $ do
      store <- readTVar subscribeRuntimeInfo
      let newStore = HM.insert subscribeRequestSubscriptionId ldreader store
      writeTVar subscribeRuntimeInfo newStore
    -- return resp
    returnResp (SubscribeResponse subscribeRequestSubscriptionId)
  where
    getSubscriptionOffset :: SubscriptionId -> IO SubscriptionOffset
    getSubscriptionOffset subscriptionId = atomically $ do
      store <- readTVar subscriptions
      let Subscription{..} = fromJust $ HM.lookup subscriptionId store
      return $ fromJust subscriptionOffset

    getStartLSN :: SubscriptionOffset -> S.LSN
    getStartLSN SubscriptionOffset{..} = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset _ -> error "shoud not reach here"
      SubscriptionOffsetOffsetRecordOffset RecordId{..} -> recordIdBatchId

getStreamName :: SubscriptionId -> TVar (HM.HashMap SubscriptionId Subscription)-> IO TL.Text
getStreamName subscriptionId tStore = atomically $ do
  store <- readTVar tStore
  let Subscription{..} = fromJust $ HM.lookup subscriptionId store
  return subscriptionStreamName

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString (show req)

  mSubscription <- atomically $ do
    store <- readTVar subscriptions
    case HM.lookup deleteSubscriptionRequestSubscriptionId store of
      Just sub -> do
        let newStore = HM.delete deleteSubscriptionRequestSubscriptionId store
        writeTVar subscriptions newStore
        return (Just sub)
      Nothing -> return Nothing

  case mSubscription of
    Just Subscription{..} -> do
      mRes <- atomically $ do
        store <- readTVar subscribeRuntimeInfo
        case HM.lookup deleteSubscriptionRequestSubscriptionId store of
          Just ldreader -> do
            let newStore = HM.delete deleteSubscriptionRequestSubscriptionId store
            writeTVar subscribeRuntimeInfo newStore
            return (Just ldreader)
          Nothing -> return Nothing

      case mRes of
        Just ldreader -> do
          -- stop ldreader
          let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
          exists <- S.doesStreamExists scLDClient streamName
          when exists $ do
            logId <- S.getUnderlyingLogId scLDClient streamName
            ckpReaderStopReading ldreader logId
        Nothing -> return ()

    Nothing -> return ()

  returnResp Empty

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext{..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest{..})= do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)

  exists <- atomically $ do
    store <- readTVar subscriptions
    if HM.member checkSubscriptionExistRequestSubscriptionId store
    then return True
    else return False

  returnResp $ CheckSubscriptionExistResponse exists

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler ServerContext{..} (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"

  atomically $ do
    store <- readTVar subscriptions
    let resp = ListSubscriptionsResponse $ HM.foldl' V.snoc V.empty store
    returnResp resp

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

  -- get ldreader from subscriptionId
  mRes <- atomically $ do
    store <- readTVar subscribeRuntimeInfo
    return $ HM.lookup fetchRequestSubscriptionId store

  case mRes of
    Just ldreader -> do
      void $ S.ckpReaderSetTimeout ldreader (fromIntegral fetchRequestTimeout)
      res <- S.ckpReaderRead ldreader (fromIntegral fetchRequestMaxSize)
      returnResp $ FetchResponse (fetchResult res)
    Nothing ->
      returnErrResp StatusInternal (StatusDetails "subscription do not exist")
  where
    fetchResult :: [S.DataRecord Bytes] -> V.Vector ReceivedRecord
    fetchResult records =
      let groups = L.groupBy ((==) `on` S.recordLSN) records
      in V.fromList $ concatMap (zipWith mkReceivedRecord [0..]) groups

    mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
    mkReceivedRecord index record =
      let recordId = RecordId (S.recordLSN record) (fromIntegral index)
      in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

-- do nothing for now, later will fix it
commitOffsetHandler
  :: ServerContext
  -> ServerRequest 'Normal CommittedOffset CommittedOffset
  -> IO (ServerResponse 'Normal CommittedOffset)
commitOffsetHandler ServerContext{..} (ServerNormalRequest _metadata offset@CommittedOffset{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive commitOffset request: " <> Log.buildString (show offset)
  returnResp offset
--------------------------------------------------------------------------------

batchAppend :: S.LDClient -> TL.Text -> [ByteString] -> S.Compression -> IO S.AppendCompletion
batchAppend client streamName payloads strategy = do
  logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
  S.appendBatchBS client logId payloads strategy Nothing
