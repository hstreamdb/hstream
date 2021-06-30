{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Exception                 (Exception, SomeException,
                                                    catch, handle, throwIO, try)
import           Control.Monad                     (when)
import qualified Data.Aeson                        as Aeson
import qualified Data.ByteString.Char8             as C
import qualified Data.ByteString.Lazy              as BSL
import           Data.Either                       (fromRight, isRight)
import qualified Data.HashMap.Strict               as HM
import           Data.IORef
import qualified Data.List                         as L
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (fromJust, fromMaybe, isJust)
import           Data.String                       (fromString)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import qualified Data.Vector                       as V
import           Database.ClickHouseDriver.Client
import           Database.ClickHouseDriver.Types
import qualified Database.MySQL.Base               as MySQL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op          (Op (OpRecvCloseOnServer),
                                                    OpRecvResult (OpRecvCloseOnServerResult),
                                                    runOps)
import           Numeric                           (showHex)
import           System.IO.Unsafe                  (unsafePerformIO)
import           System.Random                     (newStdGen, randomRs)
import           ThirdParty.Google.Protobuf.Struct
import qualified Z.Data.CBytes                     as CB
import qualified Z.Data.JSON                       as ZJ
import qualified Z.Data.Text                       as ZT
import           Z.Foreign                         (fromByteString)
import           Z.IO.Time                         (SystemTime (..),
                                                    getSystemTime')
import           ZooKeeper.Types

import           HStream.Connector.ClickHouse
import           HStream.Connector.HStore
import           HStream.Connector.MySQL
import           HStream.Processing.Connector
import           HStream.Processing.Processor      (TaskBuilder, getTaskName,
                                                    runTask)
import           HStream.Processing.Type
import           HStream.Processing.Util           (getCurrentTimestamp)
import           HStream.SQL.AST
import           HStream.SQL.Codegen
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence        as P
import           HStream.Store                     hiding (e)
import           HStream.Utils
import           Proto3.Suite                      (Enumerated (..))
import           RIO                               (async, forM_, forever)
--------------------------------------------------------------------------------

newRandomName :: Int -> IO CB.CBytes
newRandomName n = CB.pack . take n . randomRs ('a', 'z') <$> newStdGen

data ServerContext = ServerContext {
    scLDClient               :: LDClient
  , scDefaultStreamRepFactor :: Int
  , zkHandle                 :: Maybe ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
}

subscribedConnectors :: IORef (HM.HashMap TL.Text (V.Vector (TL.Text,SourceConnector)))
subscribedConnectors = unsafePerformIO $ newIORef HM.empty
{-# NOINLINE subscribedConnectors #-}

--------------------------------------------------------------------------------

handlers :: LDClient -> Int -> Maybe ZHandle -> IO (HStreamApi ServerRequest ServerResponse)
handlers ldclient repFactor handle = do
  runningQs <- newMVar HM.empty
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = repFactor
      , zkHandle                 = handle
      , runningQueries           = runningQs
      }
  return HStreamApi {
      hstreamApiExecuteQuery     = executeQueryHandler serverContext
    , hstreamApiExecutePushQuery = executePushQueryHandler serverContext
    , hstreamApiAppend           = appendHandler serverContext
    , hstreamApiCreateStreams    = createStreamsHandler serverContext
    , hstreamApiSubscribe        = subscribeHandler serverContext
    , hstreamApiFetch            = fetchHandler serverContext
    , hstreamApiCommitOffset     = commitOffsetHandler serverContext
    , hstreamApiRemoveStreams    = removeStreamsHandler serverContext
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
    CreateConnectorPlan cName ifNotExist (RConnectorOptions cOptions) -> do
      let streamM = lookup "streamname" cOptions
          typeM   = lookup "type" cOptions
          fromCOptionString          = \case Just (ConstantString s) -> Just $ C.pack s;    _ -> Nothing
          fromCOptionStringToString  = \case Just (ConstantString s) -> Just s;             _ -> Nothing
          fromCOptionIntToPortNumber = \case Just (ConstantInt s) -> Just $ fromIntegral s; _ -> Nothing
      MkSystemTime timestamp _ <- getSystemTime'
      let cid = CB.pack $ T.unpack cName
          cinfo = P.Info (ZT.pack $ T.unpack $ TL.toStrict commandQueryStmtText) timestamp
      shouldCreate <- case ifNotExist of
          True -> do
              s <- try $ P.withMaybeZHandle zkHandle $ P.getConnectorStatus cid
              case s of
                Left FailedToGet -> return True -- Not Exists
                Right _          -> return False -- Exists
          False -> return True -- Don't care
      case shouldCreate of
        False -> returnErrRes "connector already exists"
        True -> do
          sk <- case typeM of
            Just (ConstantString cType) -> do
              case cType of
                "clickhouse" -> do
                  cli <- createClient $ ConnParams
                    (fromMaybe "default"   $ fromCOptionString (lookup "username" cOptions))
                    (fromMaybe "127.0.0.1" $ fromCOptionString (lookup "host"     cOptions))
                    (fromMaybe "9000"      $ fromCOptionString (lookup "port"     cOptions))
                    (fromMaybe ""          $ fromCOptionString (lookup "password" cOptions))
                    False
                    (fromMaybe "default"   $ fromCOptionString (lookup "database" cOptions))
                  return $ Right $ clickHouseSinkConnector cli
                "mysql" -> do
                  conn <- MySQL.connect $ MySQL.ConnectInfo
                    (fromMaybe "127.0.0.1" $ fromCOptionStringToString   (lookup "host" cOptions))
                    (fromMaybe 3306        $ fromCOptionIntToPortNumber  (lookup "port" cOptions))
                    (fromMaybe "mysql"     $ fromCOptionString       (lookup "database" cOptions))
                    (fromMaybe "root"      $ fromCOptionString       (lookup "username" cOptions))
                    (fromMaybe "password"  $ fromCOptionString       (lookup "password" cOptions))
                    33
                  return $ Right $ mysqlSinkConnector conn
                _ -> return $ Left "unsupported sink connector type"
            _ -> return $ Left "invalid type in connector options"
          case sk of
            Left err -> returnErrRes err
            Right connector -> case streamM of
              Just (ConstantString stream) -> do
                streamExists <- doesStreamExists scLDClient (transToStreamName $ T.pack stream)
                case streamExists of
                  False -> returnErrRes $ "Stream " <> TL.pack stream <> " doesn't exist"
                  True -> do
                    ldreader <- newLDReader scLDClient 1000 Nothing
                    let sc = hstoreSourceConnectorWithoutCkp scLDClient ldreader
                    P.withMaybeZHandle zkHandle $ P.insertConnector cid cinfo
                    subscribeToStreamWithoutCkp sc (T.pack stream) Latest
                    _ <- async $ do
                      P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.Running
                      forever $ do
                        records <- readRecordsWithoutCkp sc
                        forM_ records $ \SourceRecord {..} ->
                          writeRecord connector $ SinkRecord (T.pack stream) srcKey srcValue srcTimestamp
                    returnOkRes
              _ -> returnErrRes "stream name missed in connector options"

    InsertPlan stream payload             -> mark LowLevelStoreException $ do
      let key = Aeson.encode $ Aeson.Object $ HM.fromList [("key", "demokey")]
      timestamp <- getCurrentTimestamp
      writeRecord (hstoreSinkConnector scLDClient)
        (SinkRecord stream (Just key) payload timestamp)
      returnOkRes
    DropPlan checkIfExist dropObject -> mark LowLevelStoreException $
      handleDropPlan sc checkIfExist dropObject
    ShowPlan showObject -> handleShowPlan sc showObject
    TerminatePlan terminationSelection -> handleTerminatePlan sc terminationSelection
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
        qid <- createInsertPersistentQuery (getTaskName taskBuilder)
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
          handleTerminatePlan sc (OneQuery x) >> P.withMaybeZHandle zkHandle (P.removeQuery' x True))
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

handleTerminatePlan :: ServerContext -> TerminationSelection
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleTerminatePlan ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> throwIO QueryTerminatedOrNotExist
  P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  swapMVar runningQueries (HM.delete qid hmapQ)
  return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")
handleTerminatePlan ServerContext{..} AllQuery = do
  hmapQ <- readMVar runningQueries
  mapM_ killThread $ HM.elems hmapQ
  let f qid = P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated
  mapM_ f $ HM.keys hmapQ
  swapMVar runningQueries HM.empty
  return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")

handleCreateAsSelect :: ServerContext -> TaskBuilder -> TL.Text -> P.QueryType -> IO ()
handleCreateAsSelect ServerContext{..} taskBuilder commandQueryStmtText extra = do
  qid <- createInsertPersistentQuery (getTaskName taskBuilder) commandQueryStmtText extra zkHandle
              --(P.StreamQuery . CB.pack . T.unpack $ sink)
  tid <- forkIO $ P.withMaybeZHandle zkHandle (P.setQueryStatus qid P.Running)
        >> runTaskWrapper False taskBuilder scLDClient
  takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
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

handlePushQueryCanceled :: ServerCall () -> IO () -> IO ()
handlePushQueryCanceled ServerCall{..} handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left err   -> print err
    Right []   -> putStrLn "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b]
      -> when b handle
    _ -> putStrLn "impossible happened"

runTaskWrapper :: Bool -> TaskBuilder -> LDClient -> IO ()
runTaskWrapper isTemp taskBuilder ldclient = do
  -- create a new ckpReader from ldclient
  let readerName = textToCBytes (getTaskName taskBuilder)
  -- FIXME: We are not sure about the number of logs we are reading here, so currently the max number of log is set to 1000
  ldreader <- newLDRsmCkpReader ldclient readerName checkpointStoreLogID 5000 1000 Nothing 10
  -- create a new sourceConnector
  let sourceConnector = hstoreSourceConnector ldclient ldreader
  -- create a new sinkConnector
  let sinkConnector = if isTemp then hstoreTempSinkConnector ldclient else hstoreSinkConnector ldclient
  -- RUN TASK
  runTask sourceConnector sinkConnector taskBuilder

createInsertPersistentQuery :: TaskName -> TL.Text -> P.QueryType -> Maybe ZHandle -> IO CB.CBytes
createInsertPersistentQuery taskName queryText extraInfo zkHandle = do
  MkSystemTime timestamp _ <- getSystemTime'
  let qid = case extraInfo of
        P.PlainQuery             -> ""
        P.StreamQuery streamName -> "stream_" <> streamName <> "-"
        P.ViewQuery   viewName _ -> "view_" <> viewName <> "-"
        <> CB.pack (T.unpack taskName)
      qinfo = P.Info (ZT.pack $ T.unpack $ TL.toStrict queryText) timestamp
  P.withMaybeZHandle zkHandle $ P.insertQuery qid qinfo extraInfo
  return qid

checkpointRootPath :: CB.CBytes
checkpointRootPath = "/tmp/checkpoint"

mark :: (Exception e, Exception f) => (e -> f) -> IO a -> IO a
mark mke = handle (throwIO . mke)

--------------------------------------------------------------------------------
appendHandler :: ServerContext
              -> ServerRequest 'Normal AppendRequest AppendResponse
              -> IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext{..} (ServerNormalRequest _metadata AppendRequest{..}) = do
  singleResps <- mapM (fmap eitherToResponse . handleSingleRequest) appendRequestRequests
  let resp = AppendResponse singleResps
  return (ServerNormalResponse resp [] StatusOk "")
  where
    handleSingleRequest :: AppendSingleRequest -> IO (TL.Text, Either SomeException AppendCompletion)
    handleSingleRequest AppendSingleRequest{..} = do
      logId <- getCLogIDByStreamName scLDClient $ transToStreamName $ TL.toStrict appendSingleRequestStreamName
      let payloads = map fromByteString $ V.toList appendSingleRequestPayload
      e' <- try $ appendBatch scLDClient logId payloads CompressionNone Nothing
      return (appendSingleRequestStreamName, e')
    eitherToResponse :: (TL.Text, Either SomeException AppendCompletion) -> AppendSingleResponse
    eitherToResponse (stream, e') =
      let serverError = case e' of
            Left _  -> HStreamServerErrorUnknownError
            Right _ -> HStreamServerErrorNoError
      in AppendSingleResponse stream (Enumerated $ Right serverError)

createStreamsHandler :: ServerContext
                     -> ServerRequest 'Normal CreateStreamsRequest CreateStreamsResponse
                     -> IO (ServerResponse 'Normal CreateStreamsResponse)
createStreamsHandler ServerContext{..} (ServerNormalRequest _metadata CreateStreamsRequest{..}) = do
  singleResps <- mapM (fmap eitherToResponse . handleSingleRequest) createStreamsRequestRequests
  let resp = CreateStreamsResponse singleResps
  return (ServerNormalResponse resp [] StatusOk "")
  where
    handleSingleRequest :: CreateStreamRequest -> IO (TL.Text, Either SomeException ())
    handleSingleRequest CreateStreamRequest{..} = do
      e' <- try $ createStream scLDClient (transToStreamName $ TL.toStrict createStreamRequestStreamName)
        (LogAttrs $ HsLogAttrs (fromIntegral createStreamRequestReplicationFactor) Map.empty)
      return (createStreamRequestStreamName, e')
    eitherToResponse :: (TL.Text, Either SomeException ()) -> CreateStreamResponse
    eitherToResponse (stream, e') =
      let serverError = case e' of
            Left _  -> HStreamServerErrorUnknownError
            Right _ -> HStreamServerErrorNoError
       in CreateStreamResponse stream (Enumerated $ Right serverError)

subscribeHandler :: ServerContext
                 -> ServerRequest 'Normal SubscribeRequest SubscribeResponse
                 -> IO (ServerResponse 'Normal SubscribeResponse)
subscribeHandler ServerContext{..} (ServerNormalRequest _metadata SubscribeRequest{..}) = do
  es' <- mapM getSourceConnector subscribeRequestSubscriptions
  case V.all isRight es' of
    False -> do
      let resp = SubscribeResponse subscribeRequestSubscriptionId (Enumerated $ Right HStreamServerErrorUnknownError)
      return (ServerNormalResponse resp [] StatusUnknown "")
    True  -> do
      let connectorsWithStreamName = fromRight undefined <$> es'
      atomicModifyIORef' subscribedConnectors (\hm -> (HM.insert subscribeRequestSubscriptionId connectorsWithStreamName hm, ()))
      let resp = SubscribeResponse subscribeRequestSubscriptionId (Enumerated $ Right HStreamServerErrorNoError)
      return (ServerNormalResponse resp [] StatusOk "")
  where
    getSourceConnector :: StreamSubscription -> IO (Either SomeException (TL.Text,SourceConnector))
    getSourceConnector StreamSubscription{..} = do
      e' <- try $ do
        newLDRsmCkpReader scLDClient (textToCBytes $ TL.toStrict streamSubscriptionStreamName)
          checkpointStoreLogID 5000 1 Nothing 10
      case e' of
        Left err       -> return (Left err)
        Right ldreader -> do
          let sc = hstoreSourceConnector scLDClient ldreader
          a' <- try $ subscribeToStream sc (TL.toStrict streamSubscriptionStreamName) (Offset streamSubscriptionStartOffset)
          case a' of
            Left a  -> return (Left a)
            Right _ -> return (Right (streamSubscriptionStreamName, sc))

fetchHandler :: ServerContext
             -> ServerRequest 'Normal FetchRequest FetchResponse
             -> IO (ServerResponse 'Normal FetchResponse)
fetchHandler ServerContext{..} (ServerNormalRequest _metadata FetchRequest{..}) = do
  hm <- readIORef subscribedConnectors
  case HM.lookup fetchRequestSubscriptionId hm of
    Nothing                       -> do
      let resp = FetchResponse V.empty
      return (ServerNormalResponse resp [] StatusOk "")
    Just connectorsWithStreamName -> do
      payloads <- mapM fetchSingleConnector (snd <$> connectorsWithStreamName)
      let records = V.map payloadToFetchedRecord $ V.zip (fst <$> connectorsWithStreamName) payloads
      let resp = FetchResponse records
      return (ServerNormalResponse resp [] StatusOk "")
  where
    fetchSingleConnector :: SourceConnector -> IO [BSL.ByteString]
    fetchSingleConnector SourceConnector{..} = do
      records <- readRecords
      return $ srcValue <$> records

    payloadToFetchedRecord :: (TL.Text, [BSL.ByteString]) -> FetchedRecord
    payloadToFetchedRecord (streamName, payloads) =
      FetchedRecord streamName (V.fromList $ BSL.toStrict <$> payloads)

commitOffsetHandler :: ServerContext
                    -> ServerRequest 'Normal CommitOffsetRequest CommitOffsetResponse
                    -> IO (ServerResponse 'Normal CommitOffsetResponse)
commitOffsetHandler ServerContext{..} (ServerNormalRequest _metadata CommitOffsetRequest{..}) = do
  hm <- readIORef subscribedConnectors
  case HM.lookup commitOffsetRequestSubscriptionId hm of
    Nothing -> do
      let resp = CommitOffsetResponse V.empty
      return (ServerNormalResponse resp [] StatusOk "")
    Just connectorsWithStreamName -> do
      singleResps <- mapM (fmap eitherToResponse . commitSingle connectorsWithStreamName) commitOffsetRequestStreamOffsets
      let resp = CommitOffsetResponse singleResps
      return (ServerNormalResponse resp [] StatusOk "")
  where
    commitSingle :: V.Vector (TL.Text, SourceConnector) -> StreamOffset -> IO (TL.Text, Either SomeException ())
    commitSingle v StreamOffset{..} = do
      case V.find (\(stream,_) -> stream == streamOffsetStreamName) v of
        Nothing                  -> return (streamOffsetStreamName, Left undefined)
        Just (stream, connector) -> do
          e' <- try $ commitCheckpoint connector (TL.toStrict stream) (Offset streamOffsetOffset)
          case e' of
            Left e  -> return (stream, Left e)
            Right _ -> return (stream, Right ())
    eitherToResponse :: (TL.Text, Either SomeException ()) -> StreamCommitOffsetResponse
    eitherToResponse (stream, e') =
      let serverError = case e' of
            Left _  -> HStreamServerErrorUnknownError
            Right _ -> HStreamServerErrorNoError
       in StreamCommitOffsetResponse stream (Enumerated $ Right serverError)

removeStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal RemoveStreamsRequest RemoveStreamsResponse
  -> IO (ServerResponse 'Normal RemoveStreamsResponse)
removeStreamsHandler ServerContext{..} (ServerNormalRequest _metadata RemoveStreamsRequest{..}) = do
  singleResps <- mapM (fmap eitherToResponse . handleSingleRequest) removeStreamsRequestRequests
  let resp = RemoveStreamsResponse singleResps
  return (ServerNormalResponse resp [] StatusOk "")
  where
    handleSingleRequest :: RemoveStreamRequest -> IO (TL.Text, Either SomeException ())
    handleSingleRequest RemoveStreamRequest{..} = do
      e' <- try $ removeStream scLDClient (transToStreamName $ TL.toStrict removeStreamRequestStreamName)
      return (removeStreamRequestStreamName, e')
    eitherToResponse :: (TL.Text, Either SomeException ()) -> RemoveStreamResponse
    eitherToResponse (stream, e') =
      let serverError = case e' of
            Left _  -> HStreamServerErrorUnknownError
            Right _ -> HStreamServerErrorNoError
       in RemoveStreamResponse stream (Enumerated $ Right serverError)
