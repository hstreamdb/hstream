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
      , headerConfig             = headerConfig
      }
  timer <- newTimer
  _ <- repeatedStart timer (checkSubscriptions timeout serverContext) (msDelay timeout)
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
  S.createStream scLDClient (transToStreamName $ TL.toStrict streamStreamName)
    $ S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  returnResp stream

deleteStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteStreamRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc (ServerNormalRequest _metadata DeleteStreamRequest{..}) = defaultExceptionHandle $ do
  let name = TL.toStrict deleteStreamRequestStreamName
  dropHelper sc name deleteStreamRequestIgnoreNonExist False

listStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListStreamsRequest ListStreamsResponse
  -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext{..} (ServerNormalRequest _metadata ListStreamsRequest) = defaultExceptionHandle $ do
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
    mkLogAttrs = S.LogAttrs . S.HsLogAttrs scDefaultStreamRepFactor
    create sName = S.createStream scLDClient sName (mkLogAttrs Map.empty)
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
          (TL.toStrict commandPushQueryQueryText) (P.PlainQuery $ textToCBytes <$> sources) zkHandle
        -- run task
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
    _ -> returnStreamingResp StatusInternal "inconsistent method called"


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
-- Subscribe

checkSubscriptions
  :: Int64    -- ^ timer timeout, ms
  -> ServerContext
  -> IO ()
checkSubscriptions timeout ServerContext{..} = do
  currentTime <- getCurrentTimestamp
  Log.debug $ "Timeout fire, checkSubscriptions begin, currentTimeStamp = " <> Log.buildInt currentTime
  (heapRemain, subReader) <- atomically $ do
    sHeap <- readTVar subscribeHeap
    let (outDated, remained) = Map.partition (\time -> currentTime - time >= timeout) sHeap
    mapM_ (updateReaderStatus subscribedReaders Released) $ Map.keys outDated
    writeTVar subscribeHeap remained
    readers <- readTVar subscribedReaders
    return (remained, readers)
  Log.debug $ "After checkSubscriptions:\n==heapRemain: " <> Log.buildString (show heapRemain) <> "\n==subReader: " <> Log.buildString (show subReader)

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata subscription@Subscription{..}) = defaultExceptionHandle' doClean $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString (show subscription)
  res <- insertSubscribedReaders subscribedReaders subscriptionSubscriptionId None
  if res
    then do
      let sName = transToStreamName $ TL.toStrict subscriptionStreamName
      createSubscribe scLDClient subscribedReaders sName
    else do
    -- subscriptionId has been used, return an error response
    returnErrResp StatusInternal $ StatusDetails "SubscriptionId has been used."
  where
    createSubscribe :: S.LDClient -> SubscribedReaders -> S.StreamId -> IO (ServerResponse 'Normal Subscription)
    createSubscribe client sReaders streamName = do
      reader <- S.newLDRsmCkpReader scLDClient (textToCBytes $ TL.toStrict subscriptionSubscriptionId)
        S.checkpointStoreLogID 5000 1 Nothing 10
      logId <- S.getUnderlyingLogId client streamName
      startLSN <- getStartLSN (fromJust subscriptionOffset) client logId
      Log.d $ Log.buildString "createSubscribe with startLSN: " <> Log.buildInt startLSN
      S.ckpReaderStartReading reader logId startLSN S.LSN_MAX
      -- consumer will fetch record from startLSN, so the start checkpoint should in (startLSN - 1)
      S.writeCheckpoints reader (Map.fromList [(logId, startLSN - 1)])
      -- If createSubscribe success, update the subscriptionId to Released status, and return success response
      let readerMap = ReaderMap reader subscription Released
      atomically $ do
        updateSubscribedReaders sReaders subscriptionSubscriptionId readerMap
      returnResp subscription

    doClean :: IO ()
    doClean = atomically $ deleteSubscribedReaders subscribedReaders subscriptionSubscriptionId

    getStartLSN :: SubscriptionOffset -> S.LDClient -> S.C_LogID -> IO S.LSN
    getStartLSN SubscriptionOffset{..} client logId = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset subOffset -> do
        case subOffset of
          Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) -> return S.LSN_MIN
          Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST)  -> (+1) <$> S.getTailLSN client logId
          Enumerated _                                               -> error "Wrong SpecialOffset!"
      SubscriptionOffsetOffsetRecordOffset RecordId{..} -> return recordIdBatchId

subscribeHandler
  :: ServerContext
  -> ServerRequest 'Normal SubscribeRequest SubscribeResponse
  -> IO (ServerResponse 'Normal SubscribeResponse)
subscribeHandler ServerContext{..} (ServerNormalRequest _metadata req@SubscribeRequest{..}) = defaultExceptionHandle' doClean $ do
  Log.debug $ "Receive subscribe request: " <> Log.buildString (show req)
  (reader, Subscription{..}) <- doCheck subscribedReaders subscribeRequestSubscriptionId
  let sName = transToStreamName $ TL.toStrict subscriptionStreamName
  doSubscribe scLDClient reader sName
  where
    -- Confirm subscriptionId exist, and the reader status should be Released, then update the reader status to Occupied.
    doCheck :: SubscribedReaders -> TL.Text -> IO (S.LDSyncCkpReader, Subscription)
    doCheck readers subscriptionId = atomically $ do
      hm <- readTVar readers
      case HM.lookup subscriptionId hm of
        Just (ReaderMap rd sId Released) -> do
          let newMap = HM.insert subscriptionId (ReaderMap rd sId Occupied) hm
          writeTVar readers newMap
          return (rd, sId)
        Just (ReaderMap _ _ Occupied) -> throwSTM SubscriptionIdOccupied
        Just None -> error "reader status shouldn't be None in subscribeHandler, but get None."
        Nothing -> throwSTM SubscriptionIdNotFound

    doSubscribe :: S.LDClient -> S.LDSyncCkpReader -> S.StreamId -> IO (ServerResponse 'Normal SubscribeResponse)
    doSubscribe client reader streamName = do
      logId <- S.getUnderlyingLogId client streamName
      S.startReadingFromCheckpoint reader logId S.LSN_MAX
      currentTime <- getCurrentTimestamp
      atomically $ modifyTVar' subscribeHeap $ Map.insert subscribeRequestSubscriptionId currentTime
      returnResp $ SubscribeResponse subscribeRequestSubscriptionId

    doClean :: IO ()
    doClean = atomically $ updateReaderStatus subscribedReaders Released subscribeRequestSubscriptionId

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata req@DeleteSubscriptionRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString (show req)
  (reader, Subscription{..}) <- lookupSubscribedReaders subscribedReaders deleteSubscriptionRequestSubscriptionId
  let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
  isExist <- S.doesStreamExists scLDClient streamName
  when isExist $ do
      logId <- S.getUnderlyingLogId scLDClient streamName
      ckpReaderStopReading reader logId
  atomically $ do
    deleteSubscribedReaders subscribedReaders subscriptionSubscriptionId
    modifyTVar' subscribeHeap $ Map.delete subscriptionSubscriptionId
  returnResp Empty

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext{..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest{..})=
  defaultExceptionHandle $ do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  status <- atomically $ getReaderStatus subscribedReaders checkSubscriptionExistRequestSubscriptionId
  returnResp $ CheckSubscriptionExistResponse (isJust status)

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler ServerContext{..} (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  hm <- subscribedReadersToMap subscribedReaders
  let resp = ListSubscriptionsResponse $ HM.foldr' (\(_, s) acc -> V.cons s acc) V.empty hm
  return $ ServerNormalResponse (Just resp) [] StatusOk ""

--------------------------------------------------------------------------------
-- Comsumer

consumerHeartbeatHandler
  :: ServerContext
  -> ServerRequest 'Normal ConsumerHeartbeatRequest ConsumerHeartbeatResponse
  -> IO (ServerResponse 'Normal ConsumerHeartbeatResponse)
consumerHeartbeatHandler ServerContext{..} (ServerNormalRequest _metadata ConsumerHeartbeatRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive heartbeat msg for " <> Log.buildLazyText consumerHeartbeatRequestSubscriptionId
  timestamp <- getCurrentTimestamp
  atomically $ do
    hm <- readTVar subscribeHeap
    case Map.lookup consumerHeartbeatRequestSubscriptionId hm of
      Nothing -> do
        returnErrResp StatusInternal "Can't send hearbeat to an unsubscribed stream."
      Just _  -> do
        modifyTVar' subscribeHeap $ Map.insert consumerHeartbeatRequestSubscriptionId timestamp
        returnResp $ ConsumerHeartbeatResponse consumerHeartbeatRequestSubscriptionId

fetchHandler
  :: ServerContext
  -> ServerRequest 'Normal FetchRequest FetchResponse
  -> IO (ServerResponse 'Normal FetchResponse)
fetchHandler ServerContext{..} (ServerNormalRequest _metadata req@FetchRequest{..}) = defaultExceptionHandle $  do
  Log.debug $ "Receive fetch request: " <> Log.buildString (show req)
  (reader, _) <- lookupSubscribedReaders subscribedReaders fetchRequestSubscriptionId
  void $ S.ckpReaderSetTimeout reader (fromIntegral fetchRequestTimeout)
  res <- S.ckpReaderRead reader (fromIntegral fetchRequestMaxSize)
  returnResp $ FetchResponse (fetchResult res)
  where
    fetchResult :: [S.DataRecord Bytes] -> V.Vector ReceivedRecord
    fetchResult records =
      let groups = L.groupBy ((==) `on` S.recordLSN) records
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
  Log.debug $ "Receive commitOffset request: " <> Log.buildString (show offset)
  (reader, Subscription{..}) <- lookupSubscribedReaders subscribedReaders committedOffsetSubscriptionId
  commitCheckpoint scLDClient reader subscriptionStreamName (fromJust committedOffsetOffset)
  returnResp offset
  where
    commitCheckpoint :: S.LDClient -> S.LDSyncCkpReader -> TL.Text -> RecordId -> IO ()
    commitCheckpoint client reader streamName RecordId{..} = do
      logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
      S.writeCheckpoints reader (Map.singleton logId recordIdBatchId)

--------------------------------------------------------------------------------

batchAppend :: S.LDClient -> TL.Text -> [ByteString] -> S.Compression -> IO S.AppendCompletion
batchAppend client streamName payloads strategy = do
  logId <- S.getUnderlyingLogId client $ transToStreamName $ TL.toStrict streamName
  S.appendBatchBS client logId payloads strategy Nothing
