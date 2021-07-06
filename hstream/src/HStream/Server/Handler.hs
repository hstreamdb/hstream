{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Exception                    (Exception, SomeException,
                                                       displayException, handle,
                                                       throwIO, try)
import           Control.Monad                        (void, when)
import qualified Data.Aeson                           as Aeson
import           Data.ByteString                      (ByteString)
import qualified Data.ByteString.Char8                as C
import           Data.Either                          (isRight)
import qualified Data.HashMap.Strict                  as HM
import           Data.IORef                           (IORef,
                                                       atomicModifyIORef',
                                                       newIORef, readIORef)
import qualified Data.List                            as L
import qualified Data.Map.Strict                      as Map
import           Data.Maybe                           (fromJust, isJust)
import           Data.String                          (fromString)
import qualified Data.Text                            as T
import qualified Data.Text.Lazy                       as TL
import qualified Data.Vector                          as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                         (Enumerated (..))
import           System.IO.Unsafe                     (unsafePerformIO)
import           System.Random                        (newStdGen, randomRs)
import           ThirdParty.Google.Protobuf.Empty
import           ThirdParty.Google.Protobuf.Struct    (Struct (Struct),
                                                       Value (Value),
                                                       ValueKind (ValueKindStringValue))
import           ThirdParty.Google.Protobuf.Timestamp
import qualified Z.Data.CBytes                        as CB
import qualified Z.Data.JSON                          as ZJ
import           Z.Data.Vector                        (Bytes)
import           Z.Foreign                            (toByteString)
import           ZooKeeper.Types                      (ZHandle)

import           HStream.Connector.HStore
import           HStream.Processing.Connector
import           HStream.Processing.Processor         (TaskBuilder, getTaskName)
import           HStream.Processing.Type              hiding (StreamName,
                                                       Timestamp)
import           HStream.SQL.Codegen                  hiding (StreamName)
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common
import           HStream.Server.Handler.Connector     (cancelConnectorHandler,
                                                       createSinkConnectorHandler,
                                                       deleteConnectorHandler,
                                                       getConnectorHandler,
                                                       listConnectorHandler,
                                                       restartConnectorHandler)
import           HStream.Server.Handler.Query         (cancelQueryHandler,
                                                       createQueryHandler,
                                                       deleteQueryHandler,
                                                       fetchQueryHandler,
                                                       getQueryHandler,
                                                       restartQueryHandler)
import           HStream.Server.Handler.View          (createViewHandler,
                                                       deleteViewHandler,
                                                       getViewHandler,
                                                       listViewsHandler)
import qualified HStream.Server.Persistence           as P
import           HStream.Store                        hiding (e)
import           HStream.Utils

--------------------------------------------------------------------------------

newRandomName :: Int -> IO CB.CBytes
newRandomName n = CB.pack . take n . randomRs ('a', 'z') <$> newStdGen

-- | Map: { subscriptionId : LDSyncCkpReader }
subscribedReaders :: IORef (HM.HashMap TL.Text LDSyncCkpReader)
subscribedReaders = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE subscribedReaders #-}

--------------------------------------------------------------------------------

handlers :: LDClient -> Int -> Maybe ZHandle -> Compression -> IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient repFactor handle compression = do
  runningQs <- newMVar HM.empty
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = repFactor
      , zkHandle                 = handle
      , runningQueries           = runningQs
      , cmpStrategy              = compression
      }
  return HStreamApi {
      hstreamApiExecuteQuery     = executeQueryHandler serverContext
    , hstreamApiExecutePushQuery = executePushQueryHandler serverContext
    , hstreamApiAppend           = appendHandler serverContext
    , hstreamApiCreateStream     = createStreamsHandler serverContext
    , hstreamApiSubscribe        = subscribeHandler serverContext
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
    CreateBySelectPlan _sources sink taskBuilder _repFactor -> mark LowLevelStoreException $ do
      create sink
      handleCreateAsSelect sc taskBuilder commandQueryStmtText (P.StreamQuery . CB.pack . T.unpack $ sink)
      returnOkRes
    CreateViewPlan schema _sources sink taskBuilder _repFactor -> mark LowLevelStoreException $ do
      create sink
      handleCreateAsSelect sc taskBuilder commandQueryStmtText (P.ViewQuery (CB.pack . T.unpack $ sink) schema)
      returnOkRes
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
      return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")
  where
    mkLogAttrs = LogAttrs . HsLogAttrs scDefaultStreamRepFactor
    create sName = createStream scLDClient (transToStreamName sName) (mkLogAttrs Map.empty)

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
        (qid, _) <- createInsertPersistentQuery (getTaskName taskBuilder)
          commandPushQueryQueryText P.PlainQuery zkHandle
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
returnErrRes x = return (ServerNormalResponse (genErrorQueryResponse x) [] StatusUnknown "")

returnOkRes :: IO (ServerResponse 'Normal CommandQueryResponse)
returnOkRes = return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")

returnRes :: StatusDetails -> IO (ServerResponse 'ServerStreaming Struct)
returnRes = return . ServerWriterResponse [] StatusAborted

handleDropPlan :: ServerContext -> Bool -> DropObject
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleDropPlan sc@ServerContext{..} checkIfExist dropObject =
  case dropObject of
    DStream stream -> handleDrop "stream_" stream
    DView view     -> handleDrop "view_" view
  where
    terminateQueryAndRemove path = do
      qids <- P.withMaybeZHandle zkHandle P.getQueryIds
      case L.find ((== path) . P.getSuffix) qids of
        Just x -> handle (\(_::QueryTerminatedOrNotExist) -> pure ()) (
          handleTerminate sc (OneQuery x) >> P.withMaybeZHandle zkHandle (P.removeQuery' x True))
        Nothing -> pure ()

    handleDrop object name = do
      streamExists <- doesStreamExists scLDClient (transToStreamName name)
      if streamExists then
        terminateQueryAndRemove (T.unpack (object <> name))
        >> removeStream scLDClient (transToStreamName name)
        >> returnOkRes
      else if checkIfExist then returnOkRes
      else returnErrRes "Object does not exist"

handleShowPlan :: ServerContext -> ShowObject
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleShowPlan ServerContext{..} showObject =
  case showObject of
    SStreams -> mark LowLevelStoreException $ do
      names <- findStreams scLDClient True
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWSTREAMS"  $ cbytesToValue . getStreamName <$> L.sort names
      return (ServerNormalResponse resp [] StatusOk "")
    SQueries -> do
      queries <- P.withMaybeZHandle zkHandle P.getQueries
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWQUERIES" $ zJsonValueToValue . ZJ.toValue <$> queries
      return (ServerNormalResponse resp [] StatusOk "")
    SConnectors -> do
      connectors <- P.withMaybeZHandle zkHandle P.getConnectors
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWCONNECTORS" $ zJsonValueToValue . ZJ.toValue <$> connectors
      return (ServerNormalResponse resp [] StatusOk "")
    SViews -> do
      qids <- P.withMaybeZHandle zkHandle P.getQueryIds
      let views = filter P.isViewQuery qids
      let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWVIEWS" $ cbytesToValue <$> views
      return (ServerNormalResponse resp [] StatusOk "")
    _ -> returnErrRes "not Supported"

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
      let resp = AppendResponse appendRequestStreamName V.empty
      return $ ServerNormalResponse resp [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
    Right AppendCompletion{..} -> do
      let records = V.zipWith (\_ idx -> RecordId appendCompLSN idx) appendRequestRecords [0..]
          resp = AppendResponse appendRequestStreamName records
      return $ ServerNormalResponse resp [] StatusOk ""
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
  e' <- try $ do
    newLDRsmCkpReader scLDClient (textToCBytes $ TL.toStrict subscriptionSubscriptionId)
      checkpointStoreLogID 5000 1 Nothing 10
  case e' :: Either SomeException LDSyncCkpReader of
    Left err     -> do
      return $ ServerNormalResponse subscription [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
    Right reader -> do
      logId <- getCLogIDByStreamName scLDClient $ transToStreamName $ TL.toStrict subscriptionStreamName
      startLSN <- getStartLSN (fromJust $ subscriptionOffset) scLDClient logId
      res <- try $ ckpReaderStartReading reader logId startLSN LSN_MAX
      when (isRight res) $ do atomicModifyIORef' subscribedReaders (\hm -> (HM.insert subscriptionSubscriptionId reader hm, ()))
      eitherToResponse res subscription
  where
    getStartLSN :: SubscriptionOffset -> LDClient -> C_LogID -> IO LSN
    getStartLSN SubscriptionOffset{..} client logId = case fromJust $ subscriptionOffsetOffset of
      SubscriptionOffsetOffsetSpecialOffset subOffset -> do
        case subOffset of
          Enumerated (Right SubscriptionOffset_SpecialOffsetEARLIST) -> return LSN_MIN
          Enumerated (Right SubscriptionOffset_SpecialOffsetLATEST)  -> fmap (+1) $ getTailLSN client logId
          Enumerated _                                               -> error $ "Wrong SpecialOffset!"
      SubscriptionOffsetOffsetRecordOffset RecordId{..} -> return $ recordIdBatchId

fetchHandler :: ServerContext
             -> ServerRequest 'Normal FetchRequest FetchResponse
             -> IO (ServerResponse 'Normal FetchResponse)
fetchHandler ServerContext{..} (ServerNormalRequest _metadata FetchRequest{..}) = do
  hm <- readIORef subscribedReaders
  case HM.lookup fetchRequestSubscriptionId hm of
    Nothing     -> do
      let resp = FetchResponse V.empty
      return (ServerNormalResponse resp [] StatusInternal "SubscriptionId not found.")
    Just reader -> do
      void $ ckpReaderSetTimeout reader (fromIntegral fetchRequestTimeout)
      res <- ckpReaderRead reader (fromIntegral fetchRequestMaxSize)
      resp <- V.mapM fetchResult $ V.fromList res
      return (ServerNormalResponse (FetchResponse resp) [] StatusOk "")
  where
    fetchResult :: DataRecord -> IO (ReceivedRecord)
    fetchResult record = do
      let recordId = RecordId (recordLSN record) 0
      return $ ReceivedRecord (Just recordId) (toByteString . recordPayload $ record)

commitOffsetHandler :: ServerContext
                    -> ServerRequest 'Normal CommittedOffset CommittedOffset
                    -> IO (ServerResponse 'Normal CommittedOffset)
commitOffsetHandler ServerContext{..} (ServerNormalRequest _metadata offset@CommittedOffset{..}) = do
  hm <- readIORef subscribedReaders
  case HM.lookup committedOffsetSubscriptionId hm of
    Nothing     -> do
      return (ServerNormalResponse offset [] StatusInternal "SubscriptionId not found.")
    Just reader -> do
      e' <- try $ commitCheckpoint scLDClient reader committedOffsetStreamName (fromJust committedOffsetOffset)
      eitherToResponse e' offset
  where
    commitCheckpoint :: LDClient -> LDSyncCkpReader -> TL.Text -> RecordId -> IO ()
    commitCheckpoint client reader streamName RecordId{..} = do
      logId <- getCLogIDByStreamName client $ transToStreamName $ TL.toStrict streamName
      writeCheckpoints reader (Map.singleton logId recordIdBatchId)

deleteStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteStreamRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteStreamsHandler ServerContext{..} (ServerNormalRequest _metadata DeleteStreamRequest{..}) = do
  e' <- try $ removeStream scLDClient (transToStreamName $ TL.toStrict deleteStreamRequestStreamName)
  eitherToResponse e' Empty

listStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal Empty ListStreamsResponse
  -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext{..} (ServerNormalRequest _metadata Empty) = do
  e' <- try $ findStreams scLDClient True
  case e' :: Either SomeException [StreamName] of
    Left err -> return $
      ServerNormalResponse (ListStreamsResponse V.empty) [] StatusInternal $ StatusDetails (C.pack . displayException $ err)
    Right streams -> do
      res <- V.forM (V.fromList streams) $ \stream -> do
        refactor <- getStreamReplicaFactor scLDClient stream
        return $ Stream (TL.fromStrict . cbytesToText . getStreamName $ stream) (fromIntegral refactor)
      return $ ServerNormalResponse (ListStreamsResponse res) [] StatusOk ""

terminateQueryHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateQueryRequest TerminateQueryResponse
  -> IO (ServerResponse 'Normal TerminateQueryResponse)
terminateQueryHandler sc@ServerContext{..} (ServerNormalRequest _metadata TerminateQueryRequest{..}) = do
  let queryName = CB.pack $ TL.unpack terminateQueryRequestQueryName
  handleTerminate sc (OneQuery queryName)
  return (ServerNormalResponse (TerminateQueryResponse terminateQueryRequestQueryName) [] StatusOk  "")
