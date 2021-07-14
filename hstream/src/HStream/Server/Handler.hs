{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Exception                     (SomeException,
                                                        displayException,
                                                        handle, throwIO, try)
import           Control.Monad                         (void, when)
import qualified Data.Aeson                            as Aeson
import           Data.ByteString                       (ByteString)
import qualified Data.ByteString.Char8                 as C
import           Data.Either                           (isRight)
import qualified Data.HashMap.Strict                   as HM
import           Data.IORef                            (IORef,
                                                        atomicModifyIORef',
                                                        newIORef, readIORef)
import qualified Data.List                             as L
import qualified Data.Map.Strict                       as Map
import           Data.Maybe                            (fromJust, isJust)
import           Data.String                           (fromString)
import qualified Data.Text                             as T
import qualified Data.Text.Lazy                        as TL
import qualified Data.Vector                           as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                          (Enumerated (..))
import           System.IO.Unsafe                      (unsafePerformIO)
import           System.Random                         (newStdGen, randomRs)
import           ThirdParty.Google.Protobuf.Empty
import           ThirdParty.Google.Protobuf.Struct     (Struct (Struct),
                                                        Value (Value),
                                                        ValueKind (ValueKindStringValue))
import           ThirdParty.Google.Protobuf.Timestamp
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
                                                        listConnectorHandler,
                                                        restartConnectorHandler)
import           HStream.Server.Handler.Query          (cancelQueryHandler,
                                                        createQueryHandler,
                                                        deleteQueryHandler,
                                                        fetchQueryHandler,
                                                        getQueryHandler,
                                                        restartQueryHandler)
import           HStream.Server.Handler.View           (createViewHandler,
                                                        deleteViewHandler,
                                                        getViewHandler,
                                                        listViewsHandler)
import qualified HStream.Server.Persistence            as P
import           HStream.Store
import           HStream.Utils

--------------------------------------------------------------------------------

newRandomName :: Int -> IO CB.CBytes
newRandomName n = CB.pack . take n . randomRs ('a', 'z') <$> newStdGen

-- | Map: { subscriptionId : (LDSyncCkpReader, Subscription) }
subscribedReaders :: IORef (HM.HashMap TL.Text (LDSyncCkpReader, Subscription))
subscribedReaders = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE subscribedReaders #-}

groupbyStores :: IORef (HM.HashMap T.Text (Materialized Aeson.Object Aeson.Object))
groupbyStores = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE groupbyStores #-}
--------------------------------------------------------------------------------

handlers :: LDClient -> Int -> Maybe ZHandle -> Compression -> IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient repFactor zkHandle compression = do
  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = repFactor
      , zkHandle                 = zkHandle
      , runningQueries           = runningQs
      , runningConnectors        = runningCs
      , cmpStrategy              = compression
      }
  return HStreamApi {
      hstreamApiExecuteQuery     = executeQueryHandler serverContext
    , hstreamApiExecutePushQuery = executePushQueryHandler serverContext
    , hstreamApiAppend           = appendHandler serverContext
    , hstreamApiCreateStream     = createStreamsHandler serverContext
    , hstreamApiSubscribe        = subscribeHandler serverContext
    , hstreamApiDeleteSubscription  = deleteSubscriptionHandler serverContext
    , hstreamApiListSubscriptions = listSubscriptionsHandler
    , hstreamApiFetch            = fetchHandler serverContext
    , hstreamApiCommitOffset     = commitOffsetHandler serverContext
    , hstreamApiDeleteStream     = deleteStreamsHandler serverContext
    , hstreamApiListStreams      = listStreamsHandler serverContext
    , hstreamApiTerminateQuery   = terminateQueryHandler serverContext

    , hstreamApiCreateQuery      = createQueryHandler serverContext
    , hstreamApiGetQuery         = getQueryHandler serverContext
    , hstreamApiFetchQuery       = fetchQueryHandler serverContext
    , hstreamApiDeleteQuery      = deleteQueryHandler serverContext
    , hstreamApiCancelQuery      = cancelQueryHandler serverContext
    , hstreamApiRestartQuery     = restartQueryHandler serverContext

    , hstreamApiCreateSinkConnector  = createSinkConnectorHandler serverContext
    , hstreamApiGetConnector     = getConnectorHandler serverContext
    , hstreamApiListConnector    = listConnectorHandler serverContext
    , hstreamApiDeleteConnector  = deleteConnectorHandler serverContext
    , hstreamApiCancelConnector  = cancelConnectorHandler serverContext
    , hstreamApiRestartConnector = restartConnectorHandler serverContext

    , hstreamApiCreateView       = createViewHandler serverContext
    , hstreamApiGetView          = getViewHandler serverContext
    , hstreamApiListViews        = listViewsHandler serverContext
    , hstreamApiDeleteView       = deleteViewHandler serverContext
    }

genErrorStruct :: TL.Text -> Struct
genErrorStruct =
  Struct . Map.singleton "Error Message:" . Just . Value . Just . ValueKindStringValue

genErrorQueryResponse :: TL.Text -> CommandQueryResponse
genErrorQueryResponse = genQueryResultResponse . V.singleton . genErrorStruct

genSuccessQueryResponse :: CommandQueryResponse
genSuccessQueryResponse = CommandQueryResponse $
  Just . CommandQueryResponseKindSuccess $ CommandSuccess

genQueryResultResponse :: V.Vector Struct -> CommandQueryResponse
genQueryResultResponse = CommandQueryResponse .
  Just . CommandQueryResponseKindResultSet . CommandQueryResultSet

batchAppend :: LDClient -> TL.Text -> [Bytes] -> Compression -> IO (Either SomeException AppendCompletion)
batchAppend client streamName payloads strategy = do
  logId <- getCLogIDByStreamName client $ transToStreamName $ TL.toStrict streamName
  try $ appendBatch client logId payloads strategy Nothing

executeQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal CommandQuery CommandQueryResponse
  -> IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata CommandQuery{..}) = handle
  (\(e :: ServerHandlerException) -> returnErrRes $ getKeyWordFromException e) $ do
  plan' <- mark FrontSQLException $ streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    SelectPlan{}           -> returnErrRes "inconsistent method called"
    -- execute plans that can be executed with this method
    CreatePlan stream _repFactor -> mark LowLevelStoreException $
      create stream >> returnOkRes
    CreateBySelectPlan sources sink taskBuilder _repFactor -> mark LowLevelStoreException $
      create sink
      >> handleCreateAsSelect sc taskBuilder commandQueryStmtText
        (P.StreamQuery (textToCBytes <$> sources) (CB.pack . T.unpack $ sink))
      >> returnOkRes
    CreateViewPlan schema sources sink taskBuilder _repFactor materialized -> mark LowLevelStoreException $ do
      create sink
      >> handleCreateAsSelect sc taskBuilder commandQueryStmtText
        (P.ViewQuery (textToCBytes <$> sources) (CB.pack . drop 2 . T.unpack $ sink) schema)
      >> atomicModifyIORef' groupbyStores (\hm -> (HM.insert sink materialized hm, ()))
      >> returnOkRes
    CreateSinkConnectorPlan cName ifNotExist sName cConfig _ -> do
      streamExists <- doesStreamExists scLDClient (transToStreamName sName)
      connectorIds <- P.withMaybeZHandle zkHandle P.getConnectorIds
      let connectorExists = elem (T.unpack cName) $ map P.getSuffix connectorIds
      if streamExists then
        if connectorExists then if ifNotExist then returnOkRes else returnErrRes "connector exists"
        else handleCreateSinkConnector sc commandQueryStmtText cName sName cConfig >> returnOkRes
      else returnErrRes "stream does not exist"
    InsertPlan stream insertType payload             -> mark LowLevelStoreException $ do
      timestamp <- getProtoTimestamp
      let header = case insertType of
            JsonFormat -> buildRecordHeader jsonPayloadFlag Map.empty timestamp TL.empty
            RawFormat  -> buildRecordHeader rawPayloadFlag Map.empty timestamp TL.empty
      let record = encodeRecord $ buildRecord header payload
      void $ batchAppend scLDClient (TL.fromStrict stream) [record] cmpStrategy
      returnOkRes
    DropPlan checkIfExist dropObject -> mark LowLevelStoreException $
      handleDropPlan sc checkIfExist dropObject
    ShowPlan showObject -> handleShowPlan sc showObject
    TerminatePlan terminationSelection -> do
      handleTerminate sc terminationSelection
      return (ServerNormalResponse (Just genSuccessQueryResponse) [] StatusOk  "")
    SelectViewPlan RSelectView{..} -> do
      hm <- readIORef groupbyStores
      case HM.lookup ("__" <> rSelectViewFrom) hm of
        Nothing -> returnErrRes $ "No VIEW named " <> TL.fromStrict rSelectViewFrom <> " found"
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
              returnErrRes "Impossible happened"
  where
    mkLogAttrs = LogAttrs . HsLogAttrs scDefaultStreamRepFactor
    create sName = createStream scLDClient (transToStreamName sName) (mkLogAttrs Map.empty)
    sendResp ma valueSerde = do
      case ma of
        Nothing -> returnResultSetRes V.empty
        Just x  -> do
          let result = runDeser (deserializer valueSerde) x
          returnResultSetRes (V.singleton $ structToStruct "SELECTVIEW" $ jsonObjectToStruct result)

executePushQueryHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming CommandPushQuery Struct
  -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..}
  (ServerWriterRequest meta CommandPushQuery{..} streamSend) = handle
  (\(e :: ServerHandlerException) -> returnRes $ fromString (show e)) $ do
  plan' <-  mark FrontSQLException $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  case plan' of
    SelectPlan sources sink taskBuilder -> do
      exists <- mapM (doesStreamExists scLDClient . transToStreamName) sources
      if (not . and) exists then returnRes "some source stream do not exist"
      else mark LowLevelStoreException $ do
        createTempStream scLDClient (transToTempStreamName sink)
          (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
        -- create persistent query
        (qid, _) <- P.createInsertPersistentQuery (getTaskName taskBuilder)
          commandPushQueryQueryText (P.PlainQuery $ textToCBytes <$> sources) zkHandle
        -- run task
        tid <- forkIO $ P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Running)
          >> runTaskWrapper True taskBuilder scLDClient
        takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
        _ <- forkIO $ handlePushQueryCanceled meta
          (killThread tid >> P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Terminated))
        ldreader' <- newLDRsmCkpReader scLDClient
          (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
          checkpointStoreLogID 5000 1 Nothing 10
        let sc = hstoreTempSourceConnector scLDClient ldreader'
        subscribeToStream sc sink Latest
        sendToClient zkHandle qid sc streamSend
    _ -> returnRes "inconsistent method called"

returnErrRes :: TL.Text -> IO (ServerResponse 'Normal CommandQueryResponse)
returnErrRes x = return (ServerNormalResponse  (Just (genErrorQueryResponse x)) [] StatusUnknown "")

returnOkRes :: IO (ServerResponse 'Normal CommandQueryResponse)
returnOkRes = return (ServerNormalResponse (Just genSuccessQueryResponse) [] StatusOk "")

returnResultSetRes :: V.Vector Struct -> IO (ServerResponse 'Normal CommandQueryResponse)
returnResultSetRes v = do
  let resp = genQueryResultResponse v
  return (ServerNormalResponse (Just resp) [] StatusOk "")

returnRes :: StatusDetails -> IO (ServerResponse 'ServerStreaming Struct)
returnRes = return . ServerWriterResponse [] StatusAborted

handleDropPlan :: ServerContext -> Bool -> DropObject
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleDropPlan sc@ServerContext{..} checkIfExist dropObject =
  case dropObject of
    DStream stream -> handleDrop "stream_" stream
    DView view     -> handleDrop "view_" ("__" <> view)
  where
    handleDrop object name = do
      streamExists <- doesStreamExists scLDClient (transToStreamName name)
      if streamExists then
        terminateQueryAndRemove (T.unpack (object <> name))
        >> terminateRelatedQueries (textToCBytes name)
        >> removeStream scLDClient (transToStreamName name)
        >> returnOkRes
      else if checkIfExist then returnOkRes
      else returnErrRes "Object does not exist"

    terminateQueryAndRemove path = do
      qids <- P.withMaybeZHandle zkHandle P.getQueryIds
      case L.find ((== path) . P.getSuffix) qids of
        Just x ->
          handleStatusException (handleTerminate sc (OneQuery x))
          >> P.withMaybeZHandle zkHandle (P.removeQuery' x True)
        Nothing -> pure ()

    terminateRelatedQueries name = do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      mapM_ (handleStatusException . handleTerminate sc . OneQuery) (getRelatedQueries name queries)

    getRelatedQueries name queries = [ P.queryId query | query <- queries, name `elem` P.getRelatedStreams (P.queryInfoExtra query)]
    handleStatusException = handle (\(_::QueryTerminatedOrNotExist) -> pure ())

handleShowPlan :: ServerContext -> ShowObject
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleShowPlan ServerContext{..} showObject =
  case showObject of
    SStreams -> mark LowLevelStoreException $ do
      names <- findStreams scLDClient True
      let implicitNames = filter ((/= "__") . take 2 . CB.unpack) . L.sort . map getStreamName $ names
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWSTREAMS"  $
            cbytesToValue <$> implicitNames
      return (ServerNormalResponse (Just resp) [] StatusOk "")
    SQueries -> do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWQUERIES" $ zJsonValueToValue . ZJ.toValue <$> queries
      return (ServerNormalResponse (Just resp) [] StatusOk "")
    SConnectors -> do
      connectors <- P.withMaybeZHandle zkHandle P.getConnectors
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWCONNECTORS" $ zJsonValueToValue . ZJ.toValue <$> connectors
      return (ServerNormalResponse (Just resp) [] StatusOk "")
    SViews -> do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      let views = map ((\(P.ViewQuery _ name _) -> name) . P.queryInfoExtra) . filter (P.isViewQuery . P.queryId) $ queries
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWVIEWS" $ cbytesToValue <$> views
      return (ServerNormalResponse (Just resp) [] StatusOk "")

handleTerminate :: ServerContext -> TerminationSelection
  -> IO ()
handleTerminate ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> throwIO QueryTerminatedOrNotExist
  P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  void $ swapMVar runningQueries (HM.delete qid hmapQ)
handleTerminate ServerContext{..} AllQuery = do
  hmapQ <- readMVar runningQueries
  mapM_ killThread $ HM.elems hmapQ
  let f qid = P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  mapM_ f $ HM.keys hmapQ
  void $ swapMVar runningQueries HM.empty

--------------------------------------------------------------------------------

sendToClient :: Maybe ZHandle -> CB.CBytes -> SourceConnector -> (Struct -> IO (Either GRPCIOError ()))
             -> IO (ServerResponse 'ServerStreaming Struct)
sendToClient zkHandle qid sc@SourceConnector {..} ss@streamSend = handle
  (\(_ :: P.ZooException) -> return $ ServerWriterResponse [] StatusAborted "failed to get status") $ do
  P.withMaybeZHandle zkHandle $ P.getQueryStatus qid
  >>= \case
    P.Terminated -> return (ServerWriterResponse [] StatusUnknown "")
    P.Created    -> return (ServerWriterResponse [] StatusUnknown "")
    P.Running    -> do
      sourceRecords <- readRecords
      let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
          structs                            = jsonObjectToStruct . fromJust <$> filter isJust objects'
      streamSendMany structs
  where
    streamSendMany xs = case xs of
      []      -> sendToClient zkHandle qid sc ss
      (x:xs') -> streamSend (structToStruct "SELECT" x) >>= \case
        Left err -> print err >> return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
        Right _  -> streamSendMany xs'

--------------------------------------------------------------------------------
appendHandler :: ServerContext
              -> ServerRequest 'Normal AppendRequest AppendResponse
              -> IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext{..} (ServerNormalRequest _metadata AppendRequest{..}) = do
  timestamp <- getProtoTimestamp
  let payloads = map (buildHStreamRecord timestamp) $ V.toList appendRequestRecords
  e' <- batchAppend scLDClient appendRequestStreamName payloads cmpStrategy
  case e' :: Either SomeException AppendCompletion of
    Left err                   -> do
      return $ ServerNormalResponse Nothing [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
    Right AppendCompletion{..} -> do
      let records = V.zipWith (\_ idx -> RecordId appendCompLSN idx) appendRequestRecords [0..]
          resp = AppendResponse appendRequestStreamName records
      return $ ServerNormalResponse (Just resp) [] StatusOk ""
  where
    buildHStreamRecord :: Timestamp -> ByteString -> Bytes
    buildHStreamRecord timestamp payload = encodeRecord $
      updateRecordTimestamp (decodeByteStringRecord payload) timestamp

createStreamsHandler :: ServerContext
                     -> ServerRequest 'Normal Stream Stream
                     -> IO (ServerResponse 'Normal Stream)
createStreamsHandler ServerContext{..} (ServerNormalRequest _metadata stream@Stream{..}) = do
  e' <- try $ createStream scLDClient (transToStreamName $ TL.toStrict streamStreamName)
    (LogAttrs $ HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  eitherToResponse e' stream

subscribeHandler :: ServerContext
                 -> ServerRequest 'Normal Subscription Subscription
                 -> IO (ServerResponse 'Normal Subscription)
subscribeHandler ServerContext{..} (ServerNormalRequest _metadata subscription@Subscription{..}) = do
  hm <- readIORef subscribedReaders
  case HM.lookup subscriptionSubscriptionId hm of
    Just _  -> return $ ServerNormalResponse Nothing [] StatusInternal "SubscriptionID has been used."
    Nothing -> do
      e' <- checkExist scLDClient subscriptionStreamName
      either (return . ServerNormalResponse Nothing [] StatusInternal)
        (doSubscribe scLDClient subscription) e'
  where
    getStartLSN :: SubscriptionOffset -> LDClient -> C_LogID -> IO LSN
    getStartLSN SubscriptionOffset{..} client logId = case fromJust subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset subOffset -> do
        case subOffset of
          Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) -> return LSN_MIN
          Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST)  -> (+1) <$> getTailLSN client logId
          Enumerated _                                               -> error "Wrong SpecialOffset!"
      SubscriptionOffsetOffsetRecordOffset RecordId{..} -> return recordIdBatchId

    checkExist :: LDClient -> TL.Text -> IO (Either StatusDetails StreamName)
    checkExist client name = do
      let streamName = transToStreamName $ TL.toStrict name
      isExist <- doesStreamExists client streamName
      if isExist
      then return $ Right streamName
      else return $ Left "Stream doesn't exist"

    doSubscribe :: LDClient -> Subscription -> StreamName -> IO (ServerResponse 'Normal Subscription)
    doSubscribe client subscription@Subscription{..} streamName = do
      reader <- newLDRsmCkpReader scLDClient (textToCBytes $ TL.toStrict subscriptionSubscriptionId)
        checkpointStoreLogID 5000 1 Nothing 10
      logId <- getCLogIDByStreamName client streamName
      startLSN <- getStartLSN (fromJust subscriptionOffset) client logId
      res <- try $ ckpReaderStartReading reader logId startLSN LSN_MAX
      when (isRight res) $ do
        atomicModifyIORef' subscribedReaders
          (\mp -> (HM.insert subscriptionSubscriptionId (reader, subscription) mp, ()))
      eitherToResponse res subscription

deleteSubscriptionHandler :: ServerContext
                          -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
                          -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ServerContext{..} (ServerNormalRequest _metadata DeleteSubscriptionRequest{..}) = do
  hm <- readIORef subscribedReaders
  case HM.lookup deleteSubscriptionRequestSubscriptionId hm of
    Nothing                         -> return $ ServerNormalResponse (Just Empty) [] StatusOk ""
    Just (reader, Subscription{..}) -> do
      let streamName = transToStreamName $ TL.toStrict subscriptionStreamName
      logId <- getCLogIDByStreamName scLDClient streamName
      isExist <- doesStreamExists scLDClient streamName
      if isExist
         then do
          res <- try $ ckpReaderStopReading reader logId
          when (isRight res) $ do
            atomicModifyIORef' subscribedReaders
              (\mp -> (HM.delete subscriptionSubscriptionId mp, ()))
          eitherToResponse res Empty
         else do
          atomicModifyIORef' subscribedReaders
            (\mp -> (HM.delete subscriptionSubscriptionId mp, ()))
          return $ ServerNormalResponse Nothing [] StatusInternal "Stream doesn't exist"

fetchHandler :: ServerContext
             -> ServerRequest 'Normal FetchRequest FetchResponse
             -> IO (ServerResponse 'Normal FetchResponse)
fetchHandler _ (ServerNormalRequest _metadata FetchRequest{..}) = do
  hm <- readIORef subscribedReaders
  case HM.lookup fetchRequestSubscriptionId hm of
    Nothing          -> do
      return (ServerNormalResponse Nothing [] StatusInternal "SubscriptionId not found.")
    Just (reader, _) -> do
      void $ ckpReaderSetTimeout reader (fromIntegral fetchRequestTimeout)
      res <- ckpReaderRead reader (fromIntegral fetchRequestMaxSize)
      let resp = fetchResult res
      return (ServerNormalResponse (Just (FetchResponse resp)) [] StatusOk "")
  where
    fetchResult :: [DataRecord] -> V.Vector ReceivedRecord
    fetchResult records =
      let groups = L.groupBy (\x y -> recordLSN x == recordLSN y) records
      in V.fromList $ concatMap (zipWith mkReceivedRecord [0..]) groups

    mkReceivedRecord :: Int -> DataRecord -> ReceivedRecord
    mkReceivedRecord index record =
      let recordId = RecordId (recordLSN record) (fromIntegral index)
      in ReceivedRecord (Just recordId) (toByteString . recordPayload $ record)

commitOffsetHandler :: ServerContext
                    -> ServerRequest 'Normal CommittedOffset CommittedOffset
                    -> IO (ServerResponse 'Normal CommittedOffset)
commitOffsetHandler ServerContext{..} (ServerNormalRequest _metadata offset@CommittedOffset{..}) = do
  hm <- readIORef subscribedReaders
  case HM.lookup committedOffsetSubscriptionId hm of
    Nothing          -> do
      return (ServerNormalResponse Nothing [] StatusInternal "SubscriptionId not found.")
    Just (reader, _) -> do
      e' <- try $ commitCheckpoint scLDClient reader committedOffsetStreamName (fromJust committedOffsetOffset)
      eitherToResponse e' offset
  where
    commitCheckpoint :: LDClient -> LDSyncCkpReader -> TL.Text -> RecordId -> IO ()
    commitCheckpoint client reader streamName RecordId{..} = do
      logId <- getCLogIDByStreamName client $ transToStreamName $ TL.toStrict streamName
      writeCheckpoints reader (Map.singleton logId recordIdBatchId)

deleteStreamsHandler :: ServerContext
                     -> ServerRequest 'Normal DeleteStreamRequest Empty
                     -> IO (ServerResponse 'Normal Empty)
deleteStreamsHandler ServerContext{..} (ServerNormalRequest _metadata DeleteStreamRequest{..}) = do
  let streamName = transToStreamName $ TL.toStrict deleteStreamRequestStreamName
  isExist <- doesStreamExists scLDClient streamName
  if isExist
  then do
    e' <- try $ removeStream scLDClient streamName
    putStrLn $ "the result of removeStream: " <> show e'
    eitherToResponse e' Empty
  else return $ ServerNormalResponse Nothing [] StatusInternal "Stream doesn't exist"

listStreamsHandler :: ServerContext
                   -> ServerRequest 'Normal Empty ListStreamsResponse
                   -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext{..} (ServerNormalRequest _metadata Empty) = do
  e' <- try $ findStreams scLDClient True
  case e' :: Either SomeException [StreamName] of
    Left err -> return $
      ServerNormalResponse Nothing [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
    Right streams -> do
      res <- V.forM (V.fromList streams) $ \stream -> do
        refactor <- getStreamReplicaFactor scLDClient stream
        return $ Stream (TL.fromStrict . cbytesToText . getStreamName $ stream) (fromIntegral refactor)
      return $ ServerNormalResponse (Just (ListStreamsResponse res)) [] StatusOk ""

listSubscriptionsHandler :: ServerRequest 'Normal Empty ListSubscriptionsResponse
                         -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler (ServerNormalRequest _metadata Empty) = do
  hm <- readIORef subscribedReaders
  let resp = ListSubscriptionsResponse $ HM.foldr' (\(_, s) acc -> V.cons s acc) V.empty hm
  return $ ServerNormalResponse (Just resp) [] StatusOk ""

terminateQueryHandler :: ServerContext
                      -> ServerRequest 'Normal TerminateQueryRequest TerminateQueryResponse
                      -> IO (ServerResponse 'Normal TerminateQueryResponse)
terminateQueryHandler sc (ServerNormalRequest _metadata TerminateQueryRequest{..}) = do
  let queryName = CB.pack $ TL.unpack terminateQueryRequestQueryName
  handleTerminate sc (OneQuery queryName)
  return (ServerNormalResponse (Just (TerminateQueryResponse terminateQueryRequestQueryName)) [] StatusOk  "")
