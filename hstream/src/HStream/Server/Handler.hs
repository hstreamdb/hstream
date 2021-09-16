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
import           Control.Exception                 (handle, throwIO)
import           Control.Monad                     (join, unless, when,
                                                    zipWithM)
import qualified Data.Aeson                        as Aeson
import           Data.Bifunctor
import           Data.ByteString                   (ByteString)
import           Data.Function                     (on, (&))
import           Data.Functor
import qualified Data.HashMap.Strict               as HM
import           Data.IORef                        (atomicModifyIORef',
                                                    newIORef, readIORef,
                                                    writeIORef)
import           Data.Int                          (Int64)
import qualified Data.List                         as L
import           Data.Map.Strict                   (Map)
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (catMaybes, fromJust, isJust)
import           Data.Scientific
import           Data.String                       (fromString)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import qualified Data.Time                         as Time
import qualified Data.Vector                       as V
import           Data.Word                         (Word32, Word64)
import           HStream.Connector.HStore
import qualified HStream.Logger                    as Log
import           HStream.Processing.Connector      (SourceConnector (..))
import           HStream.Processing.Encoding       (Deserializer (..),
                                                    Serde (..), Serializer (..))
import           HStream.Processing.Processor      (getTaskName)
import           HStream.Processing.Store
import           HStream.Processing.Stream         (Materialized (..))
import qualified HStream.Processing.Stream         as Processing
import           HStream.Processing.Type           hiding (StreamName,
                                                    Timestamp)
import           HStream.SQL                       (parseAndRefine)
import           HStream.SQL.AST
import           HStream.SQL.Codegen               hiding (StreamName)
import           HStream.SQL.ExecPlan              (genExecutionPlan)
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector  (createConnector,
                                                    createSinkConnectorHandler,
                                                    deleteConnectorHandler,
                                                    getConnectorHandler,
                                                    listConnectorsHandler,
                                                    restartConnectorHandler,
                                                    terminateConnectorHandler)
import           HStream.Server.Handler.Query      (createQueryHandler,
                                                    deleteQueryHandler,
                                                    getQueryHandler,
                                                    listQueriesHandler,
                                                    restartQueryHandler,
                                                    terminateQueriesHandler)
import           HStream.Server.Handler.StoreAdmin (getStoreNodeHandler,
                                                    listStoreNodesHandler)
import           HStream.Server.Handler.View       (createViewHandler,
                                                    deleteViewHandler,
                                                    getViewHandler,
                                                    listViewsHandler)
import qualified HStream.Server.Persistence        as P
import qualified HStream.Store                     as S
import qualified HStream.Store.Admin.API           as AA
import           HStream.ThirdParty.Protobuf       as PB
import           HStream.Utils
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                      (Enumerated (..),
                                                    HasDefault (def))
import qualified Z.Data.CBytes                     as CB
import           Z.Data.Vector                     (Bytes)
import           Z.Foreign                         (toByteString)
import           Z.IO.LowResTimer                  (registerLowResTimer)
import           ZooKeeper.Types                   (ZHandle)

--------------------------------------------------------------------------------

handlers ::
  S.LDClient ->
  AA.HeaderConfig AA.AdminAPI ->
  Int ->
  ZHandle ->
  -- | timer timeout, ms
  Int64 ->
  S.Compression ->
  IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient headerConfig repFactor zkHandle _ compression = do
  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subscribeRuntimeInfo <- newMVar HM.empty
  let serverContext =
        ServerContext
          { scLDClient = ldclient,
            scDefaultStreamRepFactor = repFactor,
            zkHandle = zkHandle,
            runningQueries = runningQs,
            runningConnectors = runningCs,
            subscribeRuntimeInfo = subscribeRuntimeInfo,
            cmpStrategy = compression,
            headerConfig = headerConfig
          }
  -- timer <- newTimer
  -- _ <- repeatedStart timer (checkSubscriptions timeout serverContext) (msDelay timeout)
  return
    HStreamApi
      { hstreamApiEcho = echoHandler,
        -- Streams
        hstreamApiCreateStream = createStreamHandler serverContext,
        hstreamApiDeleteStream = deleteStreamHandler serverContext,
        hstreamApiListStreams = listStreamsHandler serverContext,
        hstreamApiAppend = appendHandler serverContext,
        -- Subscribe
        hstreamApiCreateSubscription = createSubscriptionHandler serverContext,
        hstreamApiSubscribe = subscribeHandler serverContext,
        hstreamApiDeleteSubscription = deleteSubscriptionHandler serverContext,
        hstreamApiListSubscriptions = listSubscriptionsHandler serverContext,
        hstreamApiCheckSubscriptionExist = checkSubscriptionExistHandler serverContext,
        -- Consume
        hstreamApiFetch = fetchHandler serverContext,
        hstreamApiSendConsumerHeartbeat = consumerHeartbeatHandler serverContext,
        hstreamApiAcknowledge = ackHandler serverContext,
        hstreamApiStreamingFetch = streamingFetchHandler serverContext,
        hstreamApiExecuteQuery = executeQueryHandler serverContext,
        hstreamApiExecutePushQuery = executePushQueryHandler serverContext,
        -- Query
        hstreamApiTerminateQueries = terminateQueriesHandler serverContext,
        -- Stream with Query
        hstreamApiCreateQueryStream = createQueryStreamHandler serverContext,
        -- FIXME:
        hstreamApiCreateQuery = createQueryHandler serverContext,
        hstreamApiGetQuery = getQueryHandler serverContext,
        hstreamApiListQueries = listQueriesHandler serverContext,
        hstreamApiDeleteQuery = deleteQueryHandler serverContext,
        hstreamApiRestartQuery = restartQueryHandler serverContext,
        hstreamApiCreateSinkConnector = createSinkConnectorHandler serverContext,
        hstreamApiGetConnector = getConnectorHandler serverContext,
        hstreamApiListConnectors = listConnectorsHandler serverContext,
        hstreamApiDeleteConnector = deleteConnectorHandler serverContext,
        hstreamApiTerminateConnector = terminateConnectorHandler serverContext,
        hstreamApiRestartConnector = restartConnectorHandler serverContext,
        hstreamApiCreateView = createViewHandler serverContext,
        hstreamApiGetView = getViewHandler serverContext,
        hstreamApiListViews = listViewsHandler serverContext,
        hstreamApiDeleteView = deleteViewHandler serverContext,
        hstreamApiGetNode = getStoreNodeHandler serverContext,
        hstreamApiListNodes = listStoreNodesHandler serverContext
      }

-------------------------------------------------------------------------------

echoHandler ::
  ServerRequest 'Normal EchoRequest EchoResponse ->
  IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest {..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""

-------------------------------------------------------------------------------
-- Stream

createStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal Stream Stream ->
  IO (ServerResponse 'Normal Stream)
createStreamHandler ServerContext {..} (ServerNormalRequest _metadata stream@Stream {..}) = defaultExceptionHandle $ do
  let streamName = TL.toStrict streamStreamName
  Log.debug $ "Receive Create Stream Request: New Stream Name: " <> Log.buildText streamName
  S.createStream scLDClient (transToStreamName streamName) $
    S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  returnResp stream

deleteStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal DeleteStreamRequest Empty ->
  IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc (ServerNormalRequest _metadata DeleteStreamRequest {..}) = defaultExceptionHandle $ do
  let streamName = TL.toStrict deleteStreamRequestStreamName
  Log.debug $ "Receive Delete Stream Request: Stream to Delete: " <> Log.buildText streamName
  dropHelper sc streamName deleteStreamRequestIgnoreNonExist False

listStreamsHandler ::
  ServerContext ->
  ServerRequest 'Normal ListStreamsRequest ListStreamsResponse ->
  IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext {..} (ServerNormalRequest _metadata ListStreamsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive List Stream Request"
  streams <- S.findStreams scLDClient S.StreamTypeStream True
  res <- V.forM (V.fromList streams) $ \stream -> do
    refactor <- S.getStreamReplicaFactor scLDClient stream
    return $ Stream (TL.pack . S.showStreamName $ stream) (fromIntegral refactor)
  returnResp $ ListStreamsResponse res

appendHandler ::
  ServerContext ->
  ServerRequest 'Normal AppendRequest AppendResponse ->
  IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext {..} (ServerNormalRequest _metadata AppendRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Append Stream Request. Append Data to the Stream: " <> Log.buildText (TL.toStrict appendRequestStreamName)
  timestamp <- getProtoTimestamp
  let payloads = V.toList $ encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
  S.AppendCompletion {..} <- batchAppend scLDClient appendRequestStreamName payloads cmpStrategy
  let records = V.zipWith (\_ idx -> RecordId appendCompLSN idx) appendRequestRecords [0 ..]
  returnResp $ AppendResponse appendRequestStreamName records

-------------------------------------------------------------------------------
-- Stream with Select Query

createQueryStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal CreateQueryStreamRequest CreateQueryStreamResponse ->
  IO (ServerResponse 'Normal CreateQueryStreamResponse)
createQueryStreamHandler
  sc@ServerContext {..}
  (ServerNormalRequest _metadata CreateQueryStreamRequest {..}) = defaultExceptionHandle $ do
    RQSelect select <- parseAndRefine $ TL.toStrict createQueryStreamRequestQueryStatements
    tName <- genTaskName
    let sName =
          TL.toStrict . streamStreamName
            <$> createQueryStreamRequestQueryStream
        rFac = maybe 1 (fromIntegral . streamReplicationFactor) createQueryStreamRequestQueryStream
    (builder, source, sink, _) <-
      genStreamBuilderWithStream tName sName select
    S.createStream scLDClient (transToStreamName sink) $ S.LogAttrs (S.HsLogAttrs rFac Map.empty)
    let query = P.StreamQuery (textToCBytes <$> source) (textToCBytes sink)
    void $
      handleCreateAsSelect
        sc
        (Processing.build builder)
        createQueryStreamRequestQueryStatements
        query
        S.StreamTypeStream
    let streamResp = Stream (TL.fromStrict sink) (fromIntegral rFac)
        -- FIXME: The value query returned should have been fully assigned
        queryResp = def {queryId = TL.fromStrict tName}
    returnResp $ CreateQueryStreamResponse (Just streamResp) (Just queryResp)

--------------------------------------------------------------------------------

executeQueryHandler ::
  ServerContext ->
  ServerRequest 'Normal CommandQuery CommandQueryResponse ->
  IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext {..} (ServerNormalRequest _metadata CommandQuery {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Query Request: " <> Log.buildString (TL.unpack commandQueryStmtText)
  plan' <- streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    SelectPlan {} -> returnErrResp StatusInternal "inconsistent method called"
    -- execute plans that can be executed with this method
    CreateViewPlan schema sources sink taskBuilder _repFactor materialized ->
      do
        create (transToViewStreamName sink)
        >> handleCreateAsSelect
          sc
          taskBuilder
          commandQueryStmtText
          (P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema)
          S.StreamTypeView
        >> atomicModifyIORef' groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
        >> returnCommandQueryEmptyResp
    CreateSinkConnectorPlan _cName _ifNotExist _sName _cConfig _ -> do
      createConnector sc (TL.toStrict commandQueryStmtText) >> returnCommandQueryEmptyResp
    SelectViewPlan RSelectView {..} -> do
      hm <- readIORef groupbyStores
      case HM.lookup rSelectViewFrom hm of
        Nothing -> returnErrResp StatusInternal "VIEW not found"
        Just materialized -> do
          let (keyName, keyExpr) = rSelectViewWhere
              (_, keyValue) = genRExprValue keyExpr (HM.fromList [])
          let keySerde = mKeySerde materialized
              valueSerde = mValueSerde materialized
          let key = runSer (serializer keySerde) (HM.fromList [(keyName, keyValue)])
          case mStateStore materialized of
            KVStateStore store -> do
              queries <- P.getQueries zkHandle
              sizeM <-
                getFixedWinSize queries rSelectViewFrom
                  <&> fmap diffTimeToScientific
              if isJust sizeM
                then do
                  let size = fromJust sizeM & fromJust . toBoundedInteger @Int64
                  subset <-
                    ksDump store
                      <&> Map.filterWithKey
                        (\k _ -> all (`elem` HM.toList k) (HM.toList key))
                      <&> Map.toList
                  let winStarts =
                        subset
                          <&> (lookup "winStart" . HM.toList) . fst
                            & L.sort . L.nub . catMaybes
                      singlWinStart =
                        subset
                          <&> first (filter (\(k, _) -> k == "winStart") . HM.toList)
                          <&> first HM.fromList
                      grped =
                        winStarts <&> \winStart ->
                          let Aeson.Number winStart'' = winStart
                              winStart' = fromJust . toBoundedInteger @Int64 $ winStart''
                           in ( "winStart = "
                                  <> (T.pack . show) winStart'
                                  <> " ,winEnd = "
                                  <> (T.pack . show) (winStart' + size),
                                lookup (HM.fromList [("winStart", winStart)]) singlWinStart
                                  & fromJust
                                  & Aeson.Object
                              )
                  sendResp (Just $ HM.fromList grped) valueSerde
                else ksGet key store >>= flip sendResp valueSerde
            SessionStateStore store -> do
              dropSurfaceTimeStamp <- ssDump store <&> Map.elems
              let subset =
                    dropSurfaceTimeStamp
                      <&> Map.elems
                        . Map.filterWithKey \k _ -> all (`elem` HM.toList k) (HM.toList key)
              let res =
                    subset
                      & filter (not . null) . join
                      <&> Map.toList
                        & L.sortBy (compare `on` fst) . filter (not . null) . join
              flip sendResp valueSerde $
                Just . HM.fromList $
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
      Log.debug . Log.buildString $
        "CREATE: new stream " <> show sName
          <> " with attributes: "
          <> show attrs
      S.createStream scLDClient sName (S.LogAttrs attrs)
    sendResp ma valueSerde = do
      case ma of
        Nothing -> returnCommandQueryResp V.empty
        Just x -> do
          let result = runDeser (deserializer valueSerde) x
          returnCommandQueryResp
            (V.singleton $ structToStruct "SELECTVIEW" $ jsonObjectToStruct result)
    discard = (Log.warning . Log.buildText) "impossible happened" >> returnErrResp StatusInternal "discarded method called"

executePushQueryHandler ::
  ServerContext ->
  ServerRequest 'ServerStreaming CommandPushQuery Struct ->
  IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler
  ServerContext {..}
  (ServerWriterRequest meta CommandPushQuery {..} streamSend) = defaultStreamExceptionHandle $ do
    Log.debug $ "Receive Push Query Request: " <> Log.buildString (TL.unpack commandPushQueryQueryText)
    plan' <- streamCodegen (TL.toStrict commandPushQueryQueryText)
    case plan' of
      SelectPlan sources sink taskBuilder -> do
        exists <- mapM (S.doesStreamExist scLDClient . transToStreamName) sources
        if (not . and) exists
          then do
            Log.warning $
              "At least one of the streams do not exist: "
                <> Log.buildString (show sources)
            throwIO StreamNotExist
          else do
            S.createStream
              scLDClient
              (transToTempStreamName sink)
              (S.LogAttrs $ S.HsLogAttrs scDefaultStreamRepFactor Map.empty)
            -- create persistent query
            (qid, _) <-
              P.createInsertPersistentQuery
                (getTaskName taskBuilder)
                (TL.toStrict commandPushQueryQueryText)
                (P.PlainQuery $ textToCBytes <$> sources)
                zkHandle
            -- run task
            -- FIXME: take care of the life cycle of the thread and global state
            tid <-
              forkIO $
                P.setQueryStatus qid Running zkHandle
                  >> runTaskWrapper S.StreamTypeStream S.StreamTypeTemp taskBuilder scLDClient
            takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
            _ <-
              forkIO $
                handlePushQueryCanceled
                  meta
                  (killThread tid >> P.setQueryStatus qid Terminated zkHandle)
            ldreader' <-
              S.newLDRsmCkpReader
                scLDClient
                (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
                S.checkpointStoreLogID
                5000
                1
                Nothing
                10
            let sc = hstoreSourceConnector scLDClient ldreader' S.StreamTypeTemp
            subscribeToStream sc sink Latest
            sendToClient zkHandle qid sc streamSend
      _ -> do
        Log.fatal "Push Query: Inconsistent Method Called"
        returnStreamingResp StatusInternal "inconsistent method called"

--------------------------------------------------------------------------------

sendToClient ::
  ZHandle ->
  CB.CBytes ->
  SourceConnector ->
  (Struct -> IO (Either GRPCIOError ())) ->
  IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid sc@SourceConnector {..} streamSend = do
  let f (e :: P.ZooException) = do
        Log.fatal $ "ZooKeeper Exception: " <> Log.buildString (show e)
        return $ ServerWriterResponse [] StatusAborted "failed to get status"
  handle f $
    do
      P.getQueryStatus qid zkHandle
      >>= \case
        Terminated -> return (ServerWriterResponse [] StatusUnknown "")
        Created -> return (ServerWriterResponse [] StatusUnknown "")
        Running -> do
          sourceRecords <- readRecords
          let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
              structs = jsonObjectToStruct . fromJust <$> filter isJust objects'
          streamSendMany structs
  where
    streamSendMany = \case
      [] -> sendToClient zkHandle qid sc streamSend
      (x : xs') ->
        streamSend (structToStruct "SELECT" x) >>= \case
          Left err -> do
            Log.warning $ "Send Stream Error: " <> Log.buildString (show err)
            return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
          Right _ -> streamSendMany xs'

--------------------------------------------------------------------------------
-- Subscribe

createSubscriptionHandler ::
  ServerContext ->
  ServerRequest 'Normal Subscription Subscription ->
  IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ServerContext {..} (ServerNormalRequest _metadata subscription@Subscription {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString (show subscription)

  let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
  streamExists <- S.doesStreamExist scLDClient streamName
  if not streamExists
    then do
      Log.warning $
        "Try to create a subscription to a nonexistent stream"
          <> "Stream Name: "
          <> Log.buildString (show streamName)
      returnErrResp StatusInternal $ StatusDetails "stream not exist"
    else do
      logId <- S.getUnderlyingLogId scLDClient (transToStreamName . TL.toStrict $ subscriptionStreamName)
      offset <- convertOffsetToRecordId logId
      let newSub = subscription {subscriptionOffset = Just . SubscriptionOffset . Just . SubscriptionOffsetOffsetRecordOffset $ offset}
      P.storeSubscription newSub zkHandle
      returnResp subscription
  where
    convertOffsetToRecordId logId = do
      let SubscriptionOffset {..} = fromJust subscriptionOffset
          sOffset = fromJust subscriptionOffsetOffset
      case sOffset of
        SubscriptionOffsetOffsetSpecialOffset subOffset ->
          case subOffset of
            Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) -> do
              return $ RecordId S.LSN_MIN 0
            Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST) -> do
              startLSN <- (+ 1) <$> S.getTailLSN scLDClient logId
              return $ RecordId startLSN 0
            Enumerated _ -> error "Wrong SpecialOffset!"
        SubscriptionOffsetOffsetRecordOffset recordId -> return recordId

subscribeHandler ::
  ServerContext ->
  ServerRequest 'Normal SubscribeRequest SubscribeResponse ->
  IO (ServerResponse 'Normal SubscribeResponse)
subscribeHandler ServerContext {..} (ServerNormalRequest _metadata req@SubscribeRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive subscribe request: " <> Log.buildString (show req)

  -- first, check if the subscription exist. If not, return err
  isExist <- P.checkIfExist sId zkHandle
  unless isExist $ do
    Log.warning . Log.buildString $ "Can not subscribe an unexisted subscription, subscriptionId = " <> show sId
    throwIO SubscriptionIdNotFound

  modifyMVar subscribeRuntimeInfo $ \store ->
    if HM.member subscribeRequestSubscriptionId store
      then do
        -- if the subscription has a reader bind to stream, just return
        Log.debug . Log.buildString $ "subscribe subscription " <> show sId <> " success"
        resp <- returnResp $ SubscribeResponse subscribeRequestSubscriptionId
        return (store, resp)
      else do
        sub <- P.getSubscription sId zkHandle
        doSubscribe sub store
  where
    sId = TL.toStrict subscribeRequestSubscriptionId

    doSubscribe (Just sub@Subscription {..}) store = do
      let rid@RecordId {..} = getStartRecordId sub
      -- if the underlying stream does not exist, the getUnderlyingLogId method will throw an exception,
      -- and all follows steps will not be executed.
      Log.debug $ "get subscription info from zk, streamName: " <> Log.buildLazyText subscriptionStreamName <> " offset: " <> Log.buildString (show rid)
      logId <- S.getUnderlyingLogId scLDClient (transToStreamName $ TL.toStrict subscriptionStreamName)
      ldreader <- S.newLDRsmCkpReader scLDClient (textToCBytes sId) S.checkpointStoreLogID 5000 1 Nothing 10
      Log.debug $ Log.buildString "create ld reader to stream " <> Log.buildLazyText subscriptionStreamName
      S.ckpReaderStartReading ldreader logId recordIdBatchId S.LSN_MAX
      Log.debug $ Log.buildString "createSubscribe with startLSN: " <> Log.buildInt recordIdBatchId
      -- insert to runtime info
      let info =
            SubscribeRuntimeInfo
              { sriLdCkpReader = ldreader,
                sriStreamName = TL.toStrict subscriptionStreamName,
                sriWindowLowerBound = rid,
                sriWindowUpperBound = maxBound,
                sriAckedRanges = Map.empty,
                sriBatchNumMap = Map.empty,
                sriStreamSends = HM.empty,
                sriLdReader = Nothing,
                sriLogId = logId,
                sriAckTimeoutSeconds = 0
              }
      mvar <- newMVar info
      let newStore = HM.insert subscribeRequestSubscriptionId mvar store
      resp <- returnResp (SubscribeResponse subscribeRequestSubscriptionId)
      return (newStore, resp)
    doSubscribe Nothing store = do
      Log.warning . Log.buildString $ "can not get subscription " <> show sId <> " from zk."
      resErr <- returnErrResp StatusInternal $ StatusDetails "Can not get subscription from zk"
      return (store, resErr)

deleteSubscriptionHandler ::
  ServerContext ->
  ServerRequest 'Normal DeleteSubscriptionRequest Empty ->
  IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext {..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString (show req)

  modifyMVar_ subscribeRuntimeInfo $ \store -> do
    case HM.lookup deleteSubscriptionRequestSubscriptionId store of
      Just infoMVar -> do
        shouldDelete <-
          modifyMVar
            infoMVar
            ( \info@SubscribeRuntimeInfo {..} ->
                if HM.null sriStreamSends
                  then do
                    -- stop ldreader
                    S.ckpReaderStopReading sriLdCkpReader sriLogId
                    S.readerStopReading (fromJust sriLdReader) sriLogId
                    -- remove sub from zk
                    P.removeSubscription (TL.toStrict deleteSubscriptionRequestSubscriptionId) zkHandle
                    let newInfo = info {sriValid = False, sriStreamSends = HM.empty}
                    return (newInfo, True)
                  else return (info, False)
            )
        if shouldDelete
          then return $ HM.delete deleteSubscriptionRequestSubscriptionId store
          else return store
      Nothing -> do
        P.removeSubscription (TL.toStrict deleteSubscriptionRequestSubscriptionId) zkHandle
        return store

  returnResp Empty

checkSubscriptionExistHandler ::
  ServerContext ->
  ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse ->
  IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = TL.toStrict checkSubscriptionExistRequestSubscriptionId
  res <- P.checkIfExist sid zkHandle
  returnResp . CheckSubscriptionExistResponse $ res

listSubscriptionsHandler ::
  ServerContext ->
  ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse ->
  IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler ServerContext {..} (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse . V.fromList <$> P.listSubscriptions zkHandle
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res

--------------------------------------------------------------------------------
-- Comsumer

-- do nothing now
consumerHeartbeatHandler ::
  ServerContext ->
  ServerRequest 'Normal ConsumerHeartbeatRequest ConsumerHeartbeatResponse ->
  IO (ServerResponse 'Normal ConsumerHeartbeatResponse)
consumerHeartbeatHandler ServerContext {..} (ServerNormalRequest _metadata ConsumerHeartbeatRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive heartbeat msg for " <> Log.buildLazyText consumerHeartbeatRequestSubscriptionId
  returnResp $ ConsumerHeartbeatResponse consumerHeartbeatRequestSubscriptionId

fetchHandler ::
  ServerContext ->
  ServerRequest 'Normal FetchRequest FetchResponse ->
  IO (ServerResponse 'Normal FetchResponse)
fetchHandler ServerContext {..} (ServerNormalRequest _metadata req@FetchRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive fetch request: " <> Log.buildString (show req)

  withMVar subscribeRuntimeInfo (return . HM.lookup fetchRequestSubscriptionId) >>= \case
    Just infoMVar -> do
      records <- doFetch infoMVar
      returnResp $ FetchResponse records
    Nothing -> do
      Log.warning . Log.buildString $ "fetch request error, subscriptionId " <> show fetchRequestSubscriptionId <> " not exist."
      returnErrResp StatusInternal (StatusDetails "subscription do not exist")

ackHandler ::
  ServerContext ->
  ServerRequest 'Normal AcknowledgeRequest Empty ->
  IO (ServerResponse 'Normal Empty)
ackHandler ServerContext {..} (ServerNormalRequest _metadata req@AcknowledgeRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive ack request: " <> Log.buildString (show req)

  infoMVar <- withMVar subscribeRuntimeInfo $ return . fromJust . HM.lookup acknowledgeRequestSubscriptionId
  doAck scLDClient infoMVar acknowledgeRequestAckIds
  returnResp Empty

streamingFetchHandler ::
  ServerContext ->
  ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse ->
  IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ServerContext {..} (ServerBiDiRequest _ streamRecv streamSend) = do
  Log.debug "Receive streamingFetch request"

  consumerNameRef <- newIORef TL.empty
  subscriptionIdRef <- newIORef TL.empty
  handleRequest True consumerNameRef subscriptionIdRef
  where
    handleRequest isFirst consumerNameRef subscriptionIdRef = do
      streamRecv >>= \case
        Left (err :: grpcIOError) -> do
          Log.fatal . Log.buildString $ "streamRecv error: " <> show err

          cleanupStreamSend isFirst consumerNameRef subscriptionIdRef
          return $ ServerBiDiResponse [] StatusInternal (StatusDetails "")
        Right ma ->
          case ma of
            Just StreamingFetchRequest {..} -> do
              -- if it is the first fetch request from current client, need to do some extra check and add a new streamSender
              if isFirst
                then do
                  Log.debug "stream recive requst, do check in isFirst branch"

                  writeIORef consumerNameRef streamingFetchRequestConsumerName
                  writeIORef subscriptionIdRef streamingFetchRequestSubscriptionId

                  mRes <-
                    modifyMVar
                      subscribeRuntimeInfo
                      ( \store -> do
                          case HM.lookup streamingFetchRequestSubscriptionId store of
                            Just infoMVar -> do
                              modifyMVar_
                                infoMVar
                                ( \info@SubscribeRuntimeInfo {..} -> do
                                    -- bind a new sender to current client
                                    let newSends = HM.insert streamingFetchRequestConsumerName streamSend sriStreamSends
                                    return $ info {sriStreamSends = newSends}
                                )
                              return (store, Nothing)
                            Nothing -> do
                              mSub <- P.getSubscription (TL.toStrict streamingFetchRequestSubscriptionId) zkHandle
                              case mSub of
                                Nothing -> return (store, Just "Subscription has been removed")
                                Just sub@Subscription {..} -> do
                                  let startRecordId = getStartRecordId sub
                                  newInfoMVar <-
                                    initSubscribe
                                      scLDClient
                                      streamingFetchRequestSubscriptionId
                                      subscriptionStreamName
                                      streamingFetchRequestConsumerName
                                      startRecordId
                                      streamSend
                                      subscriptionAckTimeoutSeconds
                                  Log.info $ "Subscription " <> Log.buildString (show subscriptionSubscriptionId) <> " inits done."
                                  let newStore = HM.insert streamingFetchRequestSubscriptionId newInfoMVar store
                                  return (newStore, Nothing)
                      )
                  case mRes of
                    Just errorMsg ->
                      return $ ServerBiDiResponse [] StatusInternal (StatusDetails errorMsg)
                    Nothing ->
                      handleAcks
                        streamingFetchRequestSubscriptionId
                        streamingFetchRequestAckIds
                        consumerNameRef
                        subscriptionIdRef
                else
                  handleAcks
                    streamingFetchRequestSubscriptionId
                    streamingFetchRequestAckIds
                    consumerNameRef
                    subscriptionIdRef
            Nothing -> do
              -- This means that the consumer finished sending acks actively.
              Log.info "consumer closed"
              cleanupStreamSend isFirst consumerNameRef subscriptionIdRef
              return $ ServerBiDiResponse [] StatusOk (StatusDetails "")

    handleAcks subId acks consumerNameRef subscriptionIdRef =
      if V.null acks
        then handleRequest False consumerNameRef subscriptionIdRef
        else do
          withMVar
            subscribeRuntimeInfo
            ( return . HM.lookup subId
            )
            >>= \case
              Just infoMVar -> do
                doAck scLDClient infoMVar acks
                handleRequest False consumerNameRef subscriptionIdRef
              Nothing ->
                return $ ServerBiDiResponse [] StatusInternal (StatusDetails "Subscription has been removed")

    -- We should cleanup according streamSend before returning ServerBiDiResponse.
    cleanupStreamSend isFirst consumerNameRef subscriptionIdRef = do
      unless isFirst $ do
        subscriptionId <- readIORef subscriptionIdRef
        infoMVar <- withMVar subscribeRuntimeInfo $ return . fromJust . HM.lookup subscriptionId
        consumerName <- readIORef consumerNameRef
        modifyMVar_
          infoMVar
          ( \info@SubscribeRuntimeInfo {..} -> do
              if sriValid
                then do
                  let newStreamSends = HM.delete consumerName sriStreamSends
                  return $ info {sriStreamSends = newStreamSends}
                else return info
          )

    initSubscribe ldclient subscriptionId streamName consumerName startRecordId sSend ackTimeout = do
      -- create a ldCkpReader for reading new records
      ldCkpReader <-
        S.newLDRsmCkpReader
          ldclient
          (textToCBytes $ TL.toStrict subscriptionId)
          S.checkpointStoreLogID
          5000
          1
          Nothing
          10
      -- seek ldCkpReader to start offset
      logId <- S.getUnderlyingLogId ldclient (transToStreamName (TL.toStrict streamName))
      let startLSN = recordIdBatchId startRecordId
      S.ckpReaderStartReading ldCkpReader logId startLSN S.LSN_MAX
      -- set ldCkpReader timeout to 0
      _ <- S.ckpReaderSetTimeout ldCkpReader 0
      Log.debug $ Log.buildString "created a ldCkpReader for subscription {" <> Log.buildLazyText subscriptionId <> "} with startLSN {" <> Log.buildInt startLSN <> "}"

      -- create a ldReader for rereading unacked records
      ldReader <- S.newLDReader ldclient 1 Nothing
      Log.debug $ Log.buildString "created a ldReader for subscription {" <> Log.buildLazyText subscriptionId <> "}"

      -- init SubscribeRuntimeInfo
      let info =
            SubscribeRuntimeInfo
              { sriStreamName = TL.toStrict streamName,
                sriLogId = logId,
                sriAckTimeoutSeconds = ackTimeout,
                sriLdCkpReader = ldCkpReader,
                sriLdReader = Just ldReader,
                sriWindowLowerBound = startRecordId,
                sriWindowUpperBound = maxBound,
                sriAckedRanges = Map.empty,
                sriBatchNumMap = Map.empty,
                sriStreamSends = HM.singleton consumerName sSend,
                sriValid = True
              }

      infoMVar <- newMVar info
      -- create a task for reading and dispatching records periodicly
      _ <- forkIO $ readAndDispatchRecords infoMVar
      return infoMVar

    -- read records from logdevice and dispatch them to consumers
    readAndDispatchRecords runtimeInfoMVar = do
      Log.debug $ Log.buildString "enter readAndDispatchRecords"

      modifyMVar_
        runtimeInfoMVar
        ( \info@SubscribeRuntimeInfo {..} ->
            if sriValid
              then do
                void $ registerLowResTimer 10 $ void . forkIO $ readAndDispatchRecords runtimeInfoMVar
                if not (HM.null sriStreamSends)
                  then
                    S.ckpReaderReadAllowGap sriLdCkpReader 1000 >>= \case
                      Left gap@S.GapRecord {..} -> do
                        -- insert gap range to ackedRanges
                        let gapLoRecordId = RecordId gapLoLSN minBound
                            gapHiRecordId = RecordId gapHiLSN maxBound
                            newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) sriAckedRanges
                            newInfo = info {sriAckedRanges = newRanges}
                        Log.debug . Log.buildString $ "reader meet a gapRecord for stream " <> show sriStreamName <> ", the gap is " <> show gap
                        Log.debug . Log.buildString $ "update ackedRanges to " <> show newRanges
                        return newInfo
                      Right dataRecords -> do
                        if null dataRecords
                          then do
                            Log.debug . Log.buildString $ "reader read empty dataRecords from stream " <> show sriStreamName
                            return info
                          else do
                            Log.debug . Log.buildString $ "reader read " <> show (length dataRecords) <> " records"
                            let groups = L.groupBy ((==) `on` S.recordLSN) dataRecords
                                groupNums = map (\group -> (S.recordLSN $ head group, (fromIntegral $ length group) :: Word32)) groups
                                lastBatch = last groups
                                maxRecordId = RecordId (S.recordLSN $ head lastBatch) (fromIntegral $ length lastBatch - 1)
                                -- update window upper bound and batchNumMap
                                newBatchNumMap = Map.union sriBatchNumMap (Map.fromList groupNums)
                                receivedRecords = fetchResult groups

                            newStreamSends <- dispatchRecords receivedRecords sriStreamSends
                            let receivedRecordIds = V.map (fromJust . receivedRecordRecordId) receivedRecords
                                newInfo =
                                  info
                                    { sriBatchNumMap = newBatchNumMap,
                                      sriWindowUpperBound = maxRecordId,
                                      sriStreamSends = newStreamSends
                                    }
                            -- register task for resending timeout records
                            void $
                              registerLowResTimer
                                (fromIntegral sriAckTimeoutSeconds * 10)
                                ( void $ forkIO $ tryResendTimeoutRecords receivedRecordIds sriLogId runtimeInfoMVar
                                )
                            return newInfo
                  else return info
              else return info
        )

    filterUnackedRecordIds recordIds ackedRanges windowLowerBound =
      flip V.filter recordIds $ \recordId ->
        (recordId >= windowLowerBound)
          && case Map.lookupLE recordId ackedRanges of
            Nothing                               -> True
            Just (_, RecordIdRange _ endRecordId) -> recordId > endRecordId

    tryResendTimeoutRecords recordIds logId infoMVar = do
      Log.debug "enter tryResendTimeoutRecords"
      modifyMVar_
        infoMVar
        ( \info@SubscribeRuntimeInfo {..} -> do
            if sriValid
              then do
                let unackedRecordIds = filterUnackedRecordIds recordIds sriAckedRanges sriWindowLowerBound
                if V.null unackedRecordIds
                  then return info
                  else do
                    Log.info $ Log.buildInt (V.length unackedRecordIds) <> " records need to be resend"
                    doResend info unackedRecordIds
              else return info
        )
      where
        registerResend records timeout =
          registerLowResTimer timeout $
            void . forkIO $ tryResendTimeoutRecords records logId infoMVar

        -- TODO: maybe we can read these unacked records concurrently
        doResend info@SubscribeRuntimeInfo {..} unackedRecordIds = do
          let consumerNum = HM.size sriStreamSends
          if consumerNum == 0
            then do
              Log.debug . Log.buildString $ "no consumer to resend unacked msg, try later"
              void $ registerResend unackedRecordIds 10
              return info
            else do
              streamSendValidRef <- newIORef $ V.replicate consumerNum True
              let senders = HM.toList sriStreamSends
              V.iforM_ unackedRecordIds $ \i RecordId {..} -> do
                S.readerStartReading (fromJust sriLdReader) logId recordIdBatchId recordIdBatchId
                let batchSize = fromJust $ Map.lookup recordIdBatchId sriBatchNumMap
                dataRecords <- S.readerRead (fromJust sriLdReader) (fromIntegral batchSize)
                if null dataRecords
                  then do
                    -- TODO: retry or error
                    Log.fatal $ "can not read log " <> Log.buildString (show logId) <> " at " <> Log.buildString (show recordIdBatchId)
                  else do
                    let ci = i `mod` consumerNum
                    streamSendValid <- readIORef streamSendValidRef
                    when (streamSendValid V.! ci) $ do
                      let cs = snd $ senders L.!! ci
                          rr = mkReceivedRecord (fromIntegral recordIdBatchIndex) (dataRecords !! fromIntegral recordIdBatchIndex)
                      cs (StreamingFetchResponse $ V.singleton rr) >>= \case
                        Left grpcIOError -> do
                          -- TODO: maybe we can cache these records so that the next round resend can reuse it without read from logdevice
                          Log.fatal $ "streamSend error:" <> Log.buildString (show grpcIOError)
                          let newStreamSendValid = V.update streamSendValid (V.singleton (ci, False))
                          writeIORef streamSendValidRef newStreamSendValid
                        Right _ -> return ()

              void $ registerResend unackedRecordIds (fromIntegral sriAckTimeoutSeconds * 10)

              valids <- readIORef streamSendValidRef
              let newStreamSends = map snd $ L.filter (\(i, _) -> valids V.! i) $ zip [0 ..] senders
              return $ info {sriStreamSends = HM.fromList newStreamSends}

--------------------------------------------------------------------------------
--

fetchResult :: [[S.DataRecord Bytes]] -> V.Vector ReceivedRecord
fetchResult groups = V.fromList $ concatMap (zipWith mkReceivedRecord [0 ..]) groups

mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
mkReceivedRecord index record =
  let recordId = RecordId (S.recordLSN record) (fromIntegral index)
   in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

commitCheckPoint :: S.LDClient -> S.LDSyncCkpReader -> T.Text -> RecordId -> IO ()
commitCheckPoint client reader streamName RecordId {..} = do
  logId <- S.getUnderlyingLogId client $ transToStreamName streamName
  S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

dispatchRecords ::
  Show a =>
  V.Vector ReceivedRecord ->
  HM.HashMap ConsumerName (StreamingFetchResponse -> IO (Either a ())) ->
  IO (HM.HashMap ConsumerName (StreamingFetchResponse -> IO (Either a ())))
dispatchRecords records streamSends
  | HM.null streamSends = return HM.empty
  | otherwise = do
    let slen = HM.size streamSends
    Log.debug $ Log.buildString "ready to dispatchRecords to " <> Log.buildInt slen <> " consumers"
    let initVec = V.replicate slen V.empty
    -- recordGroups aggregates the data to be sent by each sender
    let recordGroups =
          V.ifoldl'
            ( \vec idx record ->
                let senderIdx = idx `mod` slen -- Assign the idx-th record to the senderIdx-th sender to send
                    dataSet = vec V.! senderIdx -- get the set of data to be sent by snederIdx-th sender
                    newSet = V.snoc dataSet record -- add idx-th record to dataSet
                 in V.update vec $ V.singleton (senderIdx, newSet)
            )
            initVec
            records

    newSenders <- zipWithM doDispatch (HM.toList streamSends) (V.toList recordGroups)
    return . HM.fromList . catMaybes $ newSenders
  where
    doDispatch (name, sender) record = do
      Log.debug $ Log.buildString "dispatch " <> Log.buildInt (V.length record) <> " records to " <> "consumer " <> Log.buildLazyText name
      sender (StreamingFetchResponse record) >>= \case
        Left err -> do
          -- if send record error, this batch of records will resend next round
          Log.fatal . Log.buildString $ "dispatch error, will remove a consumer: " <> show err
          return Nothing
        Right _ -> do
          return $ Just (name, sender)

-- doFetch will fetch records from logdevice, if reader meet gap or no new records to read, doFetch
-- will return an empty vector
doFetch :: MVar SubscribeRuntimeInfo -> IO (V.Vector ReceivedRecord)
doFetch runtimeInfoMVar = modifyMVar runtimeInfoMVar $ \info@SubscribeRuntimeInfo {..} -> do
  S.ckpReaderReadAllowGap sriLdCkpReader 1000 >>= \case
    Left gap@S.GapRecord {..} -> do
      -- insert gap range to ackedRanges
      let gapLoRecordId = RecordId gapLoLSN minBound
          gapHiRecordId = RecordId gapHiLSN maxBound
          newRanges = Map.insert gapLoRecordId (RecordIdRange gapLoRecordId gapHiRecordId) sriAckedRanges
          newInfo = info {sriAckedRanges = newRanges}
      Log.debug . Log.buildString $ "reader meet a gapRecord for stream " <> show sriStreamName <> ", the gap is " <> show gap
      Log.debug . Log.buildString $ "update ackedRanges to " <> show newRanges
      return (newInfo, V.empty)
    Right dataRecords -> do
      if null dataRecords
        then do
          Log.debug . Log.buildString $ "reader read empty dataRecords from stream " <> show sriStreamName
          return (info, V.empty)
        else do
          Log.debug . Log.buildString $ "reader read " <> show (length dataRecords) <> " records"
          let groups = L.groupBy ((==) `on` S.recordLSN) dataRecords
              groupNums = map (\group -> (S.recordLSN $ head group, (fromIntegral $ length group) :: Word32)) groups
              lastBatch = last groups
              maxRecordId = RecordId (S.recordLSN $ head lastBatch) (fromIntegral $ length lastBatch - 1)
              -- update window upper bound and batchNumMap
              newBatchNumMap = Map.union sriBatchNumMap (Map.fromList groupNums)
              receivedRecords = fetchResult groups
          let newInfo =
                info
                  { sriBatchNumMap = newBatchNumMap,
                    sriWindowUpperBound = maxRecordId
                  }
          return (newInfo, receivedRecords)

doAck ::
  S.LDClient ->
  MVar SubscribeRuntimeInfo ->
  V.Vector RecordId ->
  IO ()
doAck client infoMVar ackRecordIds =
  modifyMVar_
    infoMVar
    ( \info@SubscribeRuntimeInfo {..} -> do
        if sriValid
          then do
            Log.e $ "before handle acks, length of ackedRanges is: " <> Log.buildInt (Map.size sriAckedRanges)
            let newAckedRanges = V.foldl' (\a b -> insertAckedRecordId b sriWindowLowerBound a sriBatchNumMap) sriAckedRanges ackRecordIds
            Log.e $ "after handle acks, length of ackedRanges is: " <> Log.buildInt (Map.size newAckedRanges)
            case tryUpdateWindowLowerBound newAckedRanges sriWindowLowerBound sriBatchNumMap of
              Just (ranges, newLowerBound, checkpointRecordId) -> do
                commitCheckPoint client sriLdCkpReader sriStreamName checkpointRecordId
                Log.info $
                  "update window lower bound, from {" <> Log.buildString (show sriWindowLowerBound)
                    <> "} to "
                    <> "{"
                    <> Log.buildString (show newLowerBound)
                    <> "}"
                return $ info {sriAckedRanges = ranges, sriWindowLowerBound = newLowerBound}
              Nothing ->
                return $ info {sriAckedRanges = newAckedRanges}
          else return info
    )

tryUpdateWindowLowerBound ::
  -- | ackedRanges
  Map RecordId RecordIdRange ->
  -- | lower bound record of current window
  RecordId ->
  -- | batchNumMap
  Map Word64 Word32 ->
  Maybe (Map RecordId RecordIdRange, RecordId, RecordId)
tryUpdateWindowLowerBound ackedRanges lowerBoundRecordId batchNumMap =
  Map.lookupMin ackedRanges >>= \(_, RecordIdRange minStartRecordId minEndRecordId) ->
    if minStartRecordId == lowerBoundRecordId
      then Just (Map.delete minStartRecordId ackedRanges, getSuccessor minEndRecordId batchNumMap, minEndRecordId)
      else Nothing

batchAppend :: S.LDClient -> TL.Text -> [ByteString] -> S.Compression -> IO S.AppendCompletion
batchAppend client streamName payloads strategy = do
  logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
  S.appendBatchBS client logId payloads strategy Nothing

--------------------------------------------------------------------------------

getFixedWinSize :: [P.PersistentQuery] -> T.Text -> IO (Maybe Time.DiffTime)
getFixedWinSize [] _ = pure Nothing
getFixedWinSize queries viewNameRaw = do
  sizes <-
    queries <&> P.queryBindedSql
      <&> parseAndRefine . cBytesToText . CB.fromText
        & sequence
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
      RTumblingWindow _  -> True
      RHoppingWIndow _ _ -> True
      RSessionWindow _   -> False
    coeRWindowToDiffTime :: RWindow -> Time.DiffTime = \case
      RTumblingWindow size  -> size
      RHoppingWIndow size _ -> size
      RSessionWindow _      -> error "Impossible happened..."

diffTimeToScientific :: Time.DiffTime -> Scientific
diffTimeToScientific = flip scientific (-9) . Time.diffTimeToPicoseconds
