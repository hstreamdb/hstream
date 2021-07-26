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
import           Control.Concurrent.Timer
import           Control.Exception                     (handle, throwIO, try)
import           Control.Monad                         (unless, void, when)
import qualified Data.Aeson                            as Aeson
import           Data.ByteString                       (ByteString)
import qualified Data.HashMap.Strict                   as HM
import           Data.IORef                            (IORef,
                                                        atomicModifyIORef',
                                                        newIORef, readIORef)
import           Data.Int                              (Int64)
import qualified Data.List                             as L
import           Data.Map.Strict                       (Map)
import qualified Data.Map.Strict                       as Map
import           Data.Maybe                            (fromJust, isJust)
import           Data.String                           (fromString)
import qualified Data.Text                             as T
import qualified Data.Text.Lazy                        as TL
import qualified Data.Vector                           as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                          (Enumerated (..))
import           System.IO.Unsafe                      (unsafePerformIO)
import qualified Z.Data.CBytes                         as CB
import qualified Z.Data.JSON                           as ZJ
import           Z.Data.Vector                         (Bytes)
import           Z.Foreign                             (toByteString)
import           ZooKeeper.Types                       (ZHandle)

import           HStream.Connector.HStore
import           HStream.Processing.Connector
import           HStream.Processing.Encoding
import           HStream.Processing.Processor          (getTaskName)
import           HStream.Processing.Store
import           HStream.Processing.Stream             (Materialized (..))
import           HStream.Processing.Stream.TimeWindows (mkTimeWindow,
                                                        mkTimeWindowKey)
import           HStream.Processing.Type               hiding (StreamName,
                                                        Timestamp)
import           HStream.Processing.Util               (getCurrentTimestamp)
import           HStream.SQL.AST                       (RSelectView (..))
import           HStream.SQL.Codegen                   hiding (StreamName)
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector      (cancelConnectorHandler,
                                                        createSinkConnectorHandler,
                                                        deleteConnectorHandler,
                                                        getConnectorHandler,
                                                        listConnectorsHandler,
                                                        restartConnectorHandler)
import           HStream.Server.Handler.Query          (cancelQueryHandler,
                                                        createQueryHandler,
                                                        deleteQueryHandler,
                                                        getQueryHandler,
                                                        listQueriesHandler,
                                                        restartQueryHandler)
import           HStream.Server.Handler.View           (createViewHandler,
                                                        deleteViewHandler,
                                                        getViewHandler,
                                                        listViewsHandler)
import qualified HStream.Server.Persistence            as P
import           HStream.Store                         (ckpReaderStopReading)
import qualified HStream.Store                         as S
import           HStream.ThirdParty.Protobuf           as PB
import           HStream.Utils

--------------------------------------------------------------------------------

groupbyStores :: IORef (HM.HashMap T.Text (Materialized Aeson.Object Aeson.Object))
groupbyStores = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE groupbyStores #-}

--------------------------------------------------------------------------------

checkSubscriptions
  :: Int64    -- ^ timer timeout, ms
  -> ServerContext
  -> IO ()
checkSubscriptions timeout ServerContext{..} =  do
  currentTime <- getCurrentTimestamp
  atomically $ do
    sHeap <- readTVar subscribeHeap
    let (outDated, remained) = Map.partition (\time -> currentTime - time >= timeout) sHeap
    mapM_ (updateReaderStatus subscribedReaders Released) $ Map.keys outDated
    writeTVar subscribeHeap remained

handlers
  :: S.LDClient
  -> Int
  -> Maybe ZHandle
  -> Int64    -- ^ timer timeout, ms
  -> S.Compression
  -> IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient repFactor zkHandle timeout compression = do
  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  readers <- newTVarIO HM.empty
  readerHeap <- newTVarIO Map.empty
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = repFactor
      , zkHandle                 = zkHandle
      , runningQueries           = runningQs
      , runningConnectors        = runningCs
      , subscribedReaders        = readers
      , subscribeHeap            = readerHeap
      , cmpStrategy              = compression
      }
  timer <- newTimer
  _ <- repeatedStart timer (checkSubscriptions timeout serverContext) (msDelay timeout)
  return HStreamApi {
      hstreamApiEcho = echoHandler

    , hstreamApiExecuteQuery     = executeQueryHandler serverContext
    , hstreamApiExecutePushQuery = executePushQueryHandler serverContext
    , hstreamApiSendConsumerHeartbeat = consumerHeartbeatHandler serverContext
    , hstreamApiAppend           = appendHandler serverContext
    , hstreamApiCreateStream     = createStreamsHandler serverContext
    , hstreamApiSubscribe        = subscribeHandler serverContext
    , hstreamApiDeleteSubscription  = deleteSubscriptionHandler serverContext
    , hstreamApiListSubscriptions = listSubscriptionsHandler serverContext
    , hstreamApiFetch            = fetchHandler serverContext
    , hstreamApiCommitOffset     = commitOffsetHandler serverContext
    , hstreamApiDeleteStream     = deleteStreamsHandler serverContext
    , hstreamApiListStreams      = listStreamsHandler serverContext
    , hstreamApiTerminateQuery   = terminateQueryHandler serverContext

    , hstreamApiCreateQuery      = createQueryHandler serverContext
    , hstreamApiGetQuery         = getQueryHandler serverContext
    , hstreamApiListQueries      = listQueriesHandler serverContext
    , hstreamApiDeleteQuery      = deleteQueryHandler serverContext
    , hstreamApiCancelQuery      = cancelQueryHandler serverContext
    , hstreamApiRestartQuery     = restartQueryHandler serverContext

    , hstreamApiCreateSinkConnector  = createSinkConnectorHandler serverContext
    , hstreamApiGetConnector         = getConnectorHandler serverContext
    , hstreamApiListConnectors       = listConnectorsHandler serverContext
    , hstreamApiDeleteConnector      = deleteConnectorHandler serverContext
    , hstreamApiCancelConnector      = cancelConnectorHandler serverContext
    , hstreamApiRestartConnector     = restartConnectorHandler serverContext

    , hstreamApiCreateView       = createViewHandler serverContext
    , hstreamApiGetView          = getViewHandler serverContext
    , hstreamApiListViews        = listViewsHandler serverContext
    , hstreamApiDeleteView       = deleteViewHandler serverContext
    }

echoHandler
  :: ServerRequest 'Normal EchoRequest EchoResponse
  -> IO (ServerResponse 'Normal EchoResponse)
echoHandler (ServerNormalRequest _metadata EchoRequest{..}) = do
  return $ ServerNormalResponse (Just $ EchoResponse echoRequestMsg) [] StatusOk ""

executeQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CommandQuery CommandQueryResponse
  -> IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata CommandQuery{..}) = defaultExceptionHandle $ do
  plan' <- streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    SelectPlan{}           -> returnErrResp StatusInternal "inconsistent method called"
    -- execute plans that can be executed with this method
    CreatePlan stream _repFactor ->
      create (transToStreamName stream) >> returnCommandQueryEmptyResp
    CreateBySelectPlan sources sink taskBuilder _repFactor ->
      create (transToStreamName sink)
      >> handleCreateAsSelect sc taskBuilder commandQueryStmtText
        (P.StreamQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink)) False
      >> returnCommandQueryEmptyResp
    CreateViewPlan schema sources sink taskBuilder _repFactor materialized -> do
      create (transToViewStreamName sink)
      >> handleCreateAsSelect sc taskBuilder commandQueryStmtText
        (P.ViewQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink) schema) False
      >> atomicModifyIORef' groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
      >> returnCommandQueryEmptyResp
    CreateSinkConnectorPlan cName ifNotExist sName cConfig _ -> do
      streamExists <- S.doesStreamExists scLDClient (transToStreamName sName)
      connectorIds <- P.withMaybeZHandle zkHandle P.getConnectorIds
      let connectorExists = textToCBytes cName `elem` connectorIds
      if streamExists then
        if connectorExists then
          if ifNotExist then returnCommandQueryEmptyResp
                        else returnErrResp StatusInternal "connector exists"
        else handleCreateSinkConnector sc (TL.toStrict commandQueryStmtText) cName sName cConfig
             >> returnCommandQueryEmptyResp
      else returnErrResp StatusInternal"stream does not exist"
    InsertPlan stream insertType payload -> do
      timestamp <- getProtoTimestamp
      let header = case insertType of
            JsonFormat -> buildRecordHeader jsonPayloadFlag Map.empty timestamp TL.empty
            RawFormat  -> buildRecordHeader rawPayloadFlag Map.empty timestamp TL.empty
      let record = encodeRecord $ buildRecord header payload
      void $ batchAppend scLDClient (TL.fromStrict stream) [record] cmpStrategy
      returnCommandQueryEmptyResp
    DropPlan checkIfExist dropObject ->
      handleDropPlan sc checkIfExist dropObject
    ShowPlan showObject -> handleShowPlan sc showObject
    TerminatePlan terminationSelection -> do
      handleTerminate sc terminationSelection
      returnCommandQueryEmptyResp
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
  where
    mkLogAttrs = S.LogAttrs . S.HsLogAttrs scDefaultStreamRepFactor
    create sName = S.createStream scLDClient sName (mkLogAttrs Map.empty)
    sendResp ma valueSerde = do
      case ma of
        Nothing -> returnCommandQueryResp V.empty
        Just x  -> do
          let result = runDeser (deserializer valueSerde) x
          returnCommandQueryResp
            (V.singleton $ structToStruct "SELECTVIEW" $ jsonObjectToStruct result)

executePushQueryHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming CommandPushQuery Struct
  -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..}
  (ServerWriterRequest meta CommandPushQuery{..} streamSend) = defaultStreamExceptionHandle $ do
  plan' <- streamCodegen (TL.toStrict commandPushQueryQueryText)
  case plan' of
    SelectPlan sources sink taskBuilder -> do
      exists <- mapM (S.doesStreamExists scLDClient . transToStreamName) sources
      if (not . and) exists then throwIO StreamNotExist
      else do
        S.createStream scLDClient (transToTempStreamName sink)
          (S.LogAttrs $ S.HsLogAttrs scDefaultStreamRepFactor Map.empty)
        -- create persistent query
        (qid, _) <- P.createInsertPersistentQuery (getTaskName taskBuilder)
          commandPushQueryQueryText (P.PlainQuery $ textToCBytes <$> sources) zkHandle
        -- run task
        tid <- forkIO $ P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Running)
          >> runTaskWrapper True taskBuilder scLDClient
        takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
        _ <- forkIO $ handlePushQueryCanceled meta
          (killThread tid >> P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Terminated))
        ldreader' <- S.newLDRsmCkpReader scLDClient
          (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
          S.checkpointStoreLogID 5000 1 Nothing 10
        let sc = hstoreTempSourceConnector scLDClient ldreader'
        subscribeToStream sc sink Latest
        sendToClient zkHandle qid sc streamSend
    _ -> returnStreamingResp StatusInternal "inconsistent method called"

handleDropPlan :: ServerContext -> Bool -> DropObject
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleDropPlan sc@ServerContext{..} checkIfExist dropObject = defaultExceptionHandle $ do
  case dropObject of
    DStream stream -> handleDrop "stream_" stream transToStreamName
    DView view     -> do
      atomicModifyIORef' groupbyStores (\hm -> (HM.delete view hm, ()))
      handleDrop "view_" view transToViewStreamName
  where
    handleDrop object name toSName = do
      streamExists <- S.doesStreamExists scLDClient (toSName name)
      if streamExists then
        terminateQueryAndRemove (textToCBytes (object <> name))
        >> terminateRelatedQueries (textToCBytes name)
        >> S.removeStream scLDClient (toSName name)
        >> returnCommandQueryEmptyResp
      else if checkIfExist then
        returnCommandQueryEmptyResp
      else
        returnErrResp StatusInternal "Object does not exist"
    terminateQueryAndRemove path = do
      qids <- P.withMaybeZHandle zkHandle P.getQueryIds
      case L.find (== path) qids of
        Just x ->
          handleTerminate sc (OneQuery x)
          >> P.withMaybeZHandle zkHandle (P.removeQuery' x True)
        Nothing -> pure ()
    terminateRelatedQueries name = do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      mapM_ (handleTerminate sc . OneQuery) (getRelatedQueries name queries)
    getRelatedQueries name queries =
      [P.queryId query | query <- queries
                       , name `elem` P.getRelatedStreams (P.queryInfoExtra query)]

handleShowPlan :: ServerContext -> ShowObject
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleShowPlan ServerContext{..} showObject = defaultExceptionHandle $ do
  case showObject of
    SStreams -> do
      names <- S.findStreams scLDClient S.StreamTypeStream True
      let resp = CommandQueryResponse . V.singleton . listToStruct "SHOWSTREAMS" . L.sort $
            stringToValue . S.showStreamName <$> names
      returnResp resp
    SQueries -> do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      let resp =  CommandQueryResponse . V.singleton . listToStruct "SHOWQUERIES" $
            zJsonValueToValue . ZJ.toValue <$> queries
      returnResp resp
    SConnectors -> do
      connectors <- P.withMaybeZHandle zkHandle P.getConnectors
      let resp =  CommandQueryResponse . V.singleton . listToStruct "SHOWCONNECTORS" $
            zJsonValueToValue . ZJ.toValue <$> connectors
      returnResp resp
    SViews -> do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      let views = map ((\(P.ViewQuery _ name _) -> name) . P.queryInfoExtra) $
                    filter P.isViewQuery queries
      let resp =  CommandQueryResponse . V.singleton . listToStruct "SHOWVIEWS" $
                    cBytesToValue <$> views
      returnResp resp

handleTerminate :: ServerContext -> TerminationSelection -> IO ()
handleTerminate ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> pure ()
  P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  void $ swapMVar runningQueries (HM.delete qid hmapQ)
handleTerminate ServerContext{..} AllQuery = do
  hmapQ <- readMVar runningQueries
  mapM_ killThread $ HM.elems hmapQ
  let f qid = P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  mapM_ f $ HM.keys hmapQ
  void $ swapMVar runningQueries HM.empty

--------------------------------------------------------------------------------

sendToClient :: Maybe ZHandle
             -> CB.CBytes
             -> SourceConnector
             -> (Struct -> IO (Either GRPCIOError ()))
             -> IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid sc@SourceConnector {..} ss@streamSend = do
  let f (_ :: P.ZooException) =
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
      []      -> sendToClient zkHandle qid sc ss
      (x:xs') -> streamSend (structToStruct "SELECT" x) >>= \case
        Left err -> do print err
                       return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
        Right _  -> streamSendMany xs'

--------------------------------------------------------------------------------

consumerHeartbeatHandler
  :: ServerContext
  -> ServerRequest 'Normal ConsumerHeartbeatRequest ConsumerHeartbeatResponse
  -> IO (ServerResponse 'Normal ConsumerHeartbeatResponse)
consumerHeartbeatHandler ServerContext{..} (ServerNormalRequest _metadata ConsumerHeartbeatRequest{..}) = do
  timestamp <- getCurrentTimestamp
  atomically $ do
    hm <- readTVar subscribedReaders
    case HM.lookup consumerHeartbeatRequestSubscriptionId hm of
      Nothing -> returnErrResp StatusInternal "SubscriptionId doesn't exist."
      Just _  -> do
        modifyTVar' subscribeHeap $ \hp ->
          Map.insert consumerHeartbeatRequestSubscriptionId timestamp hp
        returnResp $ ConsumerHeartbeatResponse consumerHeartbeatRequestSubscriptionId

appendHandler
  :: ServerContext
  -> ServerRequest 'Normal AppendRequest AppendResponse
  -> IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext{..} (ServerNormalRequest _metadata AppendRequest{..}) = defaultExceptionHandle $ do
  timestamp <- getProtoTimestamp
  let payloads = map (buildHStreamRecord timestamp) $ V.toList appendRequestRecords
  S.AppendCompletion{..} <- batchAppend scLDClient appendRequestStreamName payloads cmpStrategy
  let records = V.zipWith (\_ idx -> RecordId appendCompLSN idx) appendRequestRecords [0..]
  returnResp $ AppendResponse appendRequestStreamName records
  where
    buildHStreamRecord :: PB.Timestamp -> ByteString -> Bytes
    buildHStreamRecord timestamp payload = encodeRecord $
      updateRecordTimestamp (decodeByteStringRecord payload) timestamp

createStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamsHandler ServerContext{..} (ServerNormalRequest _metadata stream@Stream{..}) = defaultExceptionHandle $ do
  S.createStream scLDClient (transToStreamName $ TL.toStrict streamStreamName)
    (S.LogAttrs $ S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  returnResp stream

subscribeHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
subscribeHandler ServerContext{..} (ServerNormalRequest _metadata subscription@Subscription{..}) = defaultExceptionHandle $ do
  res <- insertSubscribedReaders subscribedReaders subscriptionSubscriptionId None
  if res
  then do
    let sName = transToStreamName $ TL.toStrict subscriptionStreamName
    ifExists <- S.doesStreamExists scLDClient sName
    unless ifExists $ do
      atomically $ deleteSubscribedReaders subscribedReaders subscriptionSubscriptionId
      throwIO StreamNotExist
    doSubscribe scLDClient subscribedReaders subscribeHeap subscription sName
  else do
    currentTime <- getCurrentTimestamp
    status <- atomically $ do checkAndUpdateReaderStatus subscribedReaders currentTime
    case status of
      Left err -> returnErrResp StatusInternal err
      Right _  -> returnResp subscription
  where
    getStartLSN :: SubscriptionOffset -> S.LDClient -> S.C_LogID -> IO S.LSN
    getStartLSN SubscriptionOffset{..} client logId = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset subOffset -> do
        case subOffset of
          Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) -> return S.LSN_MIN
          Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST)  -> (+1) <$> S.getTailLSN client logId
          Enumerated _                                               -> error "Wrong SpecialOffset!"
      SubscriptionOffsetOffsetRecordOffset RecordId{..} -> return recordIdBatchId

    checkAndUpdateReaderStatus :: SubscribedReaders -> Int64 -> STM (Either StatusDetails ())
    checkAndUpdateReaderStatus readers  currentTime = do
      status <- getReaderStatus readers subscriptionSubscriptionId
      case status of
        Just Occupied -> return $ Left "SubscriptionID has been used."
        Just Released  -> do
          updateReaderStatus subscribedReaders Occupied subscriptionSubscriptionId
          modifyTVar' subscribeHeap $ \hp -> Map.insert subscriptionSubscriptionId currentTime hp
          return $ Right ()
        _              -> return $ Left "Unknown error"

    doSubscribe :: S.LDClient -> SubscribedReaders -> TVar (Map TL.Text Int64) -> Subscription -> S.StreamId -> IO (ServerResponse 'Normal Subscription)
    doSubscribe client sReaders subscribeHp subscription@Subscription{..} streamName = do
      reader <- S.newLDRsmCkpReader scLDClient (textToCBytes $ TL.toStrict subscriptionSubscriptionId)
        S.checkpointStoreLogID 5000 1 Nothing 10
      logId <- S.getUnderlyingLogId client streamName
      startLSN <- getStartLSN (fromJust subscriptionOffset) client logId
      res <- try $ S.ckpReaderStartReading reader logId startLSN S.LSN_MAX
      case res of
        Right _ -> do
          currentTime <- getCurrentTimestamp
          let readerMap = ReaderMap reader subscription Occupied
          atomically $ do
            updateSubscribedReaders sReaders subscriptionSubscriptionId readerMap
            modifyTVar' subscribeHp $ \hp -> Map.insert subscriptionSubscriptionId currentTime hp
        Left _ -> atomically $ deleteSubscribedReaders sReaders subscriptionSubscriptionId
      eitherToResponse res subscription

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata DeleteSubscriptionRequest{..}) = defaultExceptionHandle $ do
  (reader, Subscription{..}) <- lookupSubscribedReaders subscribedReaders deleteSubscriptionRequestSubscriptionId
  let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
  logId <- S.getUnderlyingLogId scLDClient streamName
  isExist <- S.doesStreamExists scLDClient streamName
  when isExist $ ckpReaderStopReading reader logId
  atomically $ do
    deleteSubscribedReaders subscribedReaders subscriptionSubscriptionId
    modifyTVar' subscribeHeap $ \hm -> Map.delete subscriptionSubscriptionId hm
  returnResp Empty

fetchHandler
  :: ServerContext
  -> ServerRequest 'Normal FetchRequest FetchResponse
  -> IO (ServerResponse 'Normal FetchResponse)
fetchHandler ServerContext{..} (ServerNormalRequest _metadata FetchRequest{..}) = defaultExceptionHandle $  do
  (reader, _) <- lookupSubscribedReaders subscribedReaders fetchRequestSubscriptionId
  void $ S.ckpReaderSetTimeout reader (fromIntegral fetchRequestTimeout)
  res <- S.ckpReaderRead reader (fromIntegral fetchRequestMaxSize)
  returnResp $ FetchResponse (fetchResult res)
  where
    fetchResult :: [S.DataRecord Bytes] -> V.Vector ReceivedRecord
    fetchResult records =
      let groups = L.groupBy (\x y -> S.recordLSN x == S.recordLSN y) records
      in V.fromList $ concatMap (zipWith mkReceivedRecord [0..]) groups

    mkReceivedRecord :: Int -> S.DataRecord Bytes -> ReceivedRecord
    mkReceivedRecord index record =
      let recordId = RecordId (S.recordLSN record) (fromIntegral index)
      in ReceivedRecord (Just recordId) (toByteString . S.recordPayload $ record)

commitOffsetHandler
  :: ServerContext
  -> ServerRequest 'Normal CommittedOffset CommittedOffset
  -> IO (ServerResponse 'Normal CommittedOffset)
commitOffsetHandler ServerContext{..} (ServerNormalRequest _metadata offset@CommittedOffset{..}) = defaultExceptionHandle $ do
  (reader, _) <- lookupSubscribedReaders subscribedReaders committedOffsetSubscriptionId
  commitCheckpoint scLDClient reader committedOffsetStreamName (fromJust committedOffsetOffset)
  returnResp offset
  where
    commitCheckpoint :: S.LDClient -> S.LDSyncCkpReader -> TL.Text -> RecordId -> IO ()
    commitCheckpoint client reader streamName RecordId{..} = do
      logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
      S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

deleteStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteStreamRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteStreamsHandler ServerContext{..} (ServerNormalRequest _metadata DeleteStreamRequest{..}) = defaultExceptionHandle $ do
  S.removeStream scLDClient $ transToStreamName $ TL.toStrict deleteStreamRequestStreamName
  returnResp Empty

listStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal Empty ListStreamsResponse
  -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext{..} (ServerNormalRequest _metadata Empty) = defaultExceptionHandle $ do
  streams <- S.findStreams scLDClient S.StreamTypeStream True
  res <- V.forM (V.fromList streams) $ \stream -> do
    refactor <- S.getStreamReplicaFactor scLDClient stream
    return $ Stream (TL.pack . S.showStreamName $ stream) (fromIntegral refactor)
  returnResp $ ListStreamsResponse res

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal Empty ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler ServerContext{..} (ServerNormalRequest _metadata Empty) = defaultExceptionHandle $ do
  hm <- subscribedReadersToMap subscribedReaders
  let resp = ListSubscriptionsResponse $ HM.foldr' (\(_, s) acc -> V.cons s acc) V.empty hm
  return $ ServerNormalResponse (Just resp) [] StatusOk ""

terminateQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueryRequest TerminateQueryResponse
  -> IO (ServerResponse 'Normal TerminateQueryResponse)
terminateQueryHandler sc (ServerNormalRequest _metadata TerminateQueryRequest{..}) = defaultExceptionHandle $ do
  let queryName = CB.pack $ TL.unpack terminateQueryRequestQueryName
  handleTerminate sc (OneQuery queryName)
  return (ServerNormalResponse (Just (TerminateQueryResponse terminateQueryRequestQueryName)) [] StatusOk  "")

batchAppend :: S.LDClient -> TL.Text -> [Bytes] -> S.Compression -> IO S.AppendCompletion
batchAppend client streamName payloads strategy = do
  logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
  S.appendBatch client logId payloads strategy Nothing
