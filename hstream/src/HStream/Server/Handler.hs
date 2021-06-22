{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Exception                 (Exception (..),
                                                    SomeException, catch,
                                                    handle, throwIO, try)
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
import           HStream.SQL.Exception
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence        as P
import           HStream.Store
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

handlers :: CB.CBytes -> Maybe ZHandle -> IO (HStreamApi ServerRequest ServerResponse)
handlers logDeviceConfigPath handle = do
  ldclient <- newLDClient logDeviceConfigPath
  runningQs <- newMVar HM.empty
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = 3
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
  (\(e :: ServerHandlerException) -> returnErrRes $ fromString (show e)) $ do
  plan' <- try $ streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    Left (e :: SomeSQLException) -> returnErrRes ("error on parsing or codegen, " <> getKeyWordFromException e)
    Right SelectPlan{}           -> returnErrRes "inconsistent method called"
    -- execute plans that can be executed with this method
    Right (CreatePlan stream _repFactor)                     -> do
      try (createStream scLDClient (transToStreamName stream)
          (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty))
      >>= \case
        Left (e :: SomeException) -> returnErrRes ("error when creating stream, " <> getKeyWordFromException e)
        Right ()                  -> returnOkRes
    Right (CreateBySelectPlan _sources sink taskBuilder _repFactor) ->
      try (createStream scLDClient (transToStreamName sink)
            (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty))
      >>= \case
        Left (e :: SomeException) -> returnErrRes ("error when creating stream, " <> getKeyWordFromException e)
        Right ()                  -> forkIO (runTaskWrapper taskBuilder scLDClient)
          >> returnOkRes
    Right (CreateConnectorPlan cName (RConnectorOptions cOptions)) -> do
      let streamM = lookup "streamname" cOptions
          typeM   = lookup "type" cOptions
          fromCOptionString          = \case Just (ConstantString s) -> Just $ C.pack s;    _ -> Nothing
          fromCOptionStringToString  = \case Just (ConstantString s) -> Just s;             _ -> Nothing
          fromCOptionIntToPortNumber = \case Just (ConstantInt s) -> Just $ fromIntegral s; _ -> Nothing
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
      MkSystemTime timestamp _ <- getSystemTime'
      let cid = CB.pack $ showHex timestamp (T.unpack cName)
          cinfo = P.Info (ZT.pack $ T.unpack $ TL.toStrict commandQueryStmtText) timestamp
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
                catchZkException (P.withMaybeZHandle zkHandle $ P.insertConnector cid cinfo) FailedToRecordInfo
                subscribeToStreamWithoutCkp sc (T.pack stream) Latest
                _ <- async $ do
                  catchZkException (P.withMaybeZHandle zkHandle $ P.setConnectorStatus cid P.Running) FailedToSetStatus
                  forever $ do
                    records <- readRecordsWithoutCkp sc
                    forM_ records $ \SourceRecord {..} ->
                      writeRecord connector $ SinkRecord (T.pack stream) srcKey srcValue srcTimestamp
                returnOkRes
          _ -> returnErrRes "stream name missed in connector options"
    Right (InsertPlan stream payload)             -> do
      let key = Aeson.encode $ Aeson.Object $ HM.fromList [("key", "demokey")]
      timestamp <- getCurrentTimestamp
      try (writeRecord (hstoreSinkConnector scLDClient)
          (SinkRecord stream (Just key) payload timestamp))
      >>= \case
        Left (e :: SomeException) -> returnErrRes ("error when inserting msg, " <> getKeyWordFromException e)
        Right ()                  -> returnOkRes
    Right (DropPlan checkIfExist (DStream stream)) -> do
      streamExists <- doesStreamExists scLDClient (transToStreamName stream)
      if streamExists then try (removeStream scLDClient (transToStreamName stream))
        >>= \case
          Left (e :: SomeException) -> returnErrRes ("error removing stream, " <> getKeyWordFromException e)
          Right ()                  -> returnOkRes
      else if checkIfExist then returnOkRes
      else returnErrRes "stream does not exist"
    Right (ShowPlan showObject) ->
      case showObject of
        SStreams -> try (findStreams scLDClient True) >>= \case
          Left (e :: SomeException) -> returnErrRes ("failed to get log names from directory" <> getKeyWordFromException e)
          Right names -> do
            let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWSTREAMS"  $ cbytesToValue . getStreamName <$> L.sort names
            return (ServerNormalResponse resp [] StatusOk "")
        SQueries -> try (P.withMaybeZHandle zkHandle P.getQueries) >>= \case
          Left (e :: SomeException) -> returnErrRes ("failed to get queries from zookeeper, " <> getKeyWordFromException e)
          Right queries -> do
            let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWQUERIES" $ zJsonValueToValue . ZJ.toValue <$> queries
            return (ServerNormalResponse resp [] StatusOk "")
        SConnectors -> try (P.withMaybeZHandle zkHandle P.getConnectors) >>= \case
          Left (e :: SomeException) -> returnErrRes ("failed to get connectors from zookeeper, " <> getKeyWordFromException e)
          Right connectors -> do
            let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWCONNECTORS" $ zJsonValueToValue . ZJ.toValue <$> connectors
            return (ServerNormalResponse resp [] StatusOk "")
        _ -> returnErrRes "not Supported"
    Right (TerminatePlan terminationSelection) -> handleTerminatePlan sc terminationSelection
  where
    returnErrRes x = return (ServerNormalResponse (genErrorQueryResponse x) [] StatusUnknown "")
    returnOkRes    = return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")

executePushQueryHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming CommandPushQuery Struct
  -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..}
  (ServerWriterRequest meta CommandPushQuery{..} streamSend) = handle
  (\(e :: ServerHandlerException) -> returnRes $ fromString (show e)) $ do
  plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  case plan' of
    Left  (_ :: SomeSQLException) -> returnRes "exception on parsing or codegen"
    Right (SelectPlan sources sink taskBuilder) -> do
      exists <- mapM (doesStreamExists scLDClient . transToStreamName) sources
      if (not . and) exists then returnRes "some source stream do not exist"
      else do
        e' <- try $ createStream scLDClient (transToStreamName sink)
          (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
        case e' of
          Left (_ :: SomeException) -> returnRes "error when creating sink stream."
          Right _                   -> do
            -- create persistent query
            MkSystemTime timestamp _ <- getSystemTime'
            let qid = CB.pack $ T.unpack $ getTaskName taskBuilder
                qinfo = P.Info (ZT.pack $ T.unpack $ TL.toStrict commandPushQueryQueryText) timestamp
            catchZkException (P.withMaybeZHandle zkHandle $ P.insertQuery qid qinfo) FailedToRecordInfo
            -- run task
            tid <- forkIO $ catchZkException (P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Running) FailedToSetStatus
              >> runTaskWrapper taskBuilder scLDClient
            takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
            -- isCancelled <- newMVar False
            _ <- forkIO $ handlePushQueryCanceled meta
              (killThread tid >> catchZkException (P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated) FailedToSetStatus)
            ldreader' <- newLDFileCkpReader scLDClient
              (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
              checkpointRootPath 1 Nothing 3
            let sc = hstoreSourceConnector scLDClient ldreader'
            subscribeToStream sc sink Latest
            -- sendToClient isCancelled sc streamSend
            sendToClient zkHandle qid sc streamSend
    Right _ -> returnRes "inconsistent method called"
  where
    returnRes = return . ServerWriterResponse [] StatusAborted

handleTerminatePlan :: ServerContext -> TerminationSelection
  -> IO (ServerResponse 'Normal CommandQueryResponse)
handleTerminatePlan ServerContext{..} (OneQuery qid) = handle
  (\(e :: SomeException) -> return (ServerNormalResponse (genErrorQueryResponse $ fromString (show e)) [] StatusUnknown "")) $ do
  hmapQ <- readMVar runningQueries
  killThread (hmapQ HM.! qid)
  catchZkException (P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated) FailedToSetStatus
  swapMVar runningQueries (HM.delete qid hmapQ)
  return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")
handleTerminatePlan ServerContext{..} AllQuery = handle
  (\(e :: SomeException) -> return (ServerNormalResponse (genErrorQueryResponse $ fromString (show e)) [] StatusUnknown "")) $ do
  hmapQ <- readMVar runningQueries
  mapM_ killThread $ HM.elems hmapQ
  let f qid = catchZkException (P.withMaybeZHandle zkHandle $ P.setQueryStatus qid P.Terminated) FailedToSetStatus
  mapM_ f $ HM.keys hmapQ
  swapMVar runningQueries HM.empty
  return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")

--------------------------------------------------------------------------------

-- sendToClient :: MVar Bool -> SourceConnector -> (Struct -> IO (Either GRPCIOError ()))
sendToClient :: Maybe ZHandle -> CB.CBytes -> SourceConnector -> (Struct -> IO (Either GRPCIOError ()))
             -> IO (ServerResponse 'ServerStreaming Struct)
-- sendToClient isCancelled sc@SourceConnector {..} ss@streamSend = do
sendToClient zkHandle qid sc@SourceConnector {..} ss@streamSend = handle
  (\(e :: P.ZooException) -> return $ ServerWriterResponse [] StatusAborted "failed to get status") $ do
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

runTaskWrapper :: TaskBuilder -> LDClient -> IO ()
runTaskWrapper taskBuilder ldclient = do
  -- create a new ckpReader from ldclient
  let readerName = textToCBytes (getTaskName taskBuilder)
  ldreader <- newLDFileCkpReader ldclient readerName checkpointRootPath 1000 Nothing 3
  -- create a new sourceConnector
  let sourceConnector = hstoreSourceConnector ldclient ldreader
  -- create a new sinkConnector
  let sinkConnector = hstoreSinkConnector ldclient
  -- RUN TASK
  runTask sourceConnector sinkConnector taskBuilder

checkpointRootPath :: CB.CBytes
checkpointRootPath = "/tmp/checkpoint"

catchZkException :: Exception e => IO a -> e -> IO a
catchZkException action str = catch action (\(e::SomeException) -> throwIO e)

--------------------------------------------------------------------------------
appendHandler :: ServerContext
              -> ServerRequest 'Normal AppendRequest AppendResponse
              -> IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext{..} (ServerNormalRequest _metadata AppendRequest{..}) = do
  singleResps <- mapM (fmap eitherToResponse . handleSingleRequest) appendRequestRequests
  let resp = AppendResponse singleResps
  return (ServerNormalResponse resp [] StatusOk "")
  where
    handleSingleRequest :: AppendSingleRequest -> IO (TL.Text, Either SomeException ())
    handleSingleRequest AppendSingleRequest{..} = do
      timestamp <- getCurrentTimestamp
      e' <- try $
        writeRecord
          (hstoreSinkConnector scLDClient)
          SinkRecord
            { snkStream = TL.toStrict appendSingleRequestStreamName,
              snkKey = Nothing,
              snkValue = BSL.fromStrict appendSingleRequestPayload,
              snkTimestamp = timestamp
            }
      return (appendSingleRequestStreamName, e')
    eitherToResponse :: (TL.Text, Either SomeException ()) -> AppendSingleResponse
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
        newLDFileCkpReader scLDClient (textToCBytes $ TL.toStrict streamSubscriptionStreamName)
          checkpointRootPath 1 Nothing 3
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
