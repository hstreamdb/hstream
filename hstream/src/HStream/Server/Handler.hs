{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           Control.Concurrent
import           Control.Exception                 (SomeException, catch, try)
import           Control.Monad                     (when)
import qualified Data.Aeson                        as Aeson
import qualified Data.ByteString                   as B
import qualified Data.ByteString.Char8             as C
import qualified Data.ByteString.Lazy              as BL
import qualified Data.HashMap.Strict               as HM
import qualified Data.List                         as L
import qualified Data.Map                          as M
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (fromJust, fromMaybe, isJust)
import           Data.String                       (fromString)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import qualified Data.Vector                       as V
import           Database.ClickHouseDriver.Client
import           Database.ClickHouseDriver.Types
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op          (Op (OpRecvCloseOnServer),
                                                    OpRecvResult (OpRecvCloseOnServerResult),
                                                    runOps)
import           RIO                               (async, forM_, forever,
                                                    logOptionsHandle, stderr,
                                                    withLogFunc)
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
import           HStream.Processing.Connector
import           HStream.Processing.Processor      (TaskBuilder, getTaskName,
                                                    runTask)
import           HStream.Processing.Type
import           HStream.Processing.Util           (getCurrentTimestamp)
import           HStream.SQL.AST
import           HStream.SQL.Codegen
import           HStream.SQL.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence
import           HStream.Store
import           HStream.Utils

--------------------------------------------------------------------------------

newRandomName :: Int -> IO CB.CBytes
newRandomName n = CB.pack . take n . randomRs ('a', 'z') <$> newStdGen

data ServerContext = ServerContext {
    scLDClient               :: LDClient
  , scDefaultStreamRepFactor :: Int
  , zkHandle                 :: Maybe ZHandle
}

--------------------------------------------------------------------------------

handlers :: CB.CBytes -> Maybe ZHandle -> IO (HStreamApi ServerRequest ServerResponse)
handlers logDeviceConfigPath handle = do
  ldclient <- newLDClient logDeviceConfigPath
  let serverContext = ServerContext {
        scLDClient               = ldclient
      , scDefaultStreamRepFactor = 3
      , zkHandle                 = handle
      }
  return HStreamApi {
      hstreamApiExecuteQuery     = executeQueryHandler serverContext
    , hstreamApiExecutePushQuery = executePushQueryHandler serverContext
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
executeQueryHandler ServerContext{..} (ServerNormalRequest _metadata CommandQuery{..}) = do
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
          ldreader <- newLDFileCkpReader scLDClient (textToCBytes (T.append cName "_reader")) checkpointRootPath 1000 Nothing 3
          let sc = hstoreSourceConnector scLDClient ldreader
          let streamM = lookup "streamname" cOptions
          let typeM = lookup "type" cOptions
          let fromCOptionString m = case m of
                Just (ConstantString s) -> Just $ C.pack s
                _                       -> Nothing
          -- build sink connector by type
          let sk = case typeM of
                Just (ConstantString cType) ->
                  do
                    case cType of
                        "clickhouse" -> do
                          let username = fromMaybe "default" $ fromCOptionString (lookup "username" cOptions)
                          let host = fromMaybe "127.0.0.1" $ fromCOptionString (lookup "host" cOptions)
                          let port = fromMaybe "9000" $ fromCOptionString (lookup "port" cOptions)
                          let password = fromMaybe "" $ fromCOptionString (lookup "password" cOptions)
                          let database = fromMaybe "default" $ fromCOptionString (lookup "database" cOptions)
                          let cli = clickHouseSinkConnector $ createClient ConnParams{
                            username'     = username
                            ,host'        = host
                            ,port'        = port
                            ,password'    = password
                            ,compression' = False
                            ,database'    = database
                          }
                          Right cli
                        _ -> Left "unsupported sink connector type"
                _ -> Left "invalid type in connector options"
          case sk of
            Left err -> do
                let resp = genErrorQueryResponse err
                return (ServerNormalResponse resp [] StatusUnknown "")
            Right sk -> do
              case streamM of
                Just (ConstantString stream) -> do
                  subscribeToStream sc (T.pack stream) Latest
                  _ <- async $
                      forever $
                        do
                          records <- readRecords sc
                          forM_ records $ \SourceRecord {..} ->
                            writeRecord
                              sk
                              SinkRecord
                                { snkStream = T.pack stream,
                                  snkKey = srcKey,
                                  snkValue = srcValue,
                                  snkTimestamp = srcTimestamp
                                }
                  let resp = genSuccessQueryResponse
                  return (ServerNormalResponse resp [] StatusOk "")
                _ ->  do
                  let resp = genErrorQueryResponse "stream name missed in connector options"
                  return (ServerNormalResponse resp [] StatusUnknown "")
    Right (InsertPlan stream payload)             -> do
      timestamp <- getCurrentTimestamp
      try (writeRecord (hstoreSinkConnector scLDClient)
          (SinkRecord stream Nothing payload timestamp))
      >>= \case
        Left (e :: SomeException) -> returnErrRes ("error when inserting msg, " <> getKeyWordFromException e)
        Right ()                  -> returnOkRes
    Right (DropPlan checkIfExist stream) -> do
      streamExists <- doesStreamExists scLDClient (transToStreamName stream)
      if streamExists then try (removeStream scLDClient (transToStreamName stream))
        >>= \case
          Left (e :: SomeException) -> returnErrRes ("error removing stream, " <> getKeyWordFromException e)
          Right ()                  -> returnOkRes
      else if checkIfExist then returnOkRes
      else returnErrRes "stream does not exist"
    Right (ShowPlan showObject) ->
      case showObject of
        Streams -> try (findStreams scLDClient True) >>= \case
          Left (e :: SomeException) -> returnErrRes ("failed to get log names from directory" <> getKeyWordFromException e)
          Right names -> do
            let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWSTREAMS"  $ cbytesToValue . getStreamName <$> L.sort names
            return (ServerNormalResponse resp [] StatusOk "")
        Queries -> try (withMaybeZHandle zkHandle getQueries) >>= \case
          Left (e :: SomeException) -> returnErrRes ("failed to get queries from zookeeper, " <> getKeyWordFromException e)
          Right queries -> do
            let resp = genQueryResultResponse . V.singleton . listToStruct "SHOWQUERIES" $ zJsonValueToValue . ZJ.toValue <$> queries
            return (ServerNormalResponse resp [] StatusOk "")
        _ -> returnErrRes "not Supported"
  where
    returnErrRes x = return (ServerNormalResponse (genErrorQueryResponse x) [] StatusUnknown "")
    returnOkRes    = return (ServerNormalResponse genSuccessQueryResponse [] StatusOk  "")

executePushQueryHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming CommandPushQuery Struct
  -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..}
  (ServerWriterRequest meta CommandPushQuery{..} streamSend) = do
  plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  let returnRes = return . ServerWriterResponse [] StatusAborted
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
                qinfo = HStream.Server.Persistence.QueryInfo (ZT.pack $ T.unpack $ TL.toStrict commandPushQueryQueryText) timestamp
            catchZkException (withMaybeZHandle zkHandle $ insertQuery qid qinfo) "Failed to record query"
            -- run task
            tid <- forkIO $ catchZkException (withMaybeZHandle zkHandle $ setStatus qid QRunning) "Failed to change query status"
              >> runTaskWrapper taskBuilder scLDClient
            isCancelled <- newMVar False
            _ <- forkIO $ handlePushQueryCanceled meta isCancelled
              (killThread tid >> catchZkException (withMaybeZHandle zkHandle $ setStatus qid QTerminated) "Failed to change query status")
            ldreader' <- newLDFileCkpReader scLDClient
              (textToCBytes (T.append (getTaskName taskBuilder) "-result"))
              checkpointRootPath 1 Nothing 3
            let sc = hstoreSourceConnector scLDClient ldreader'
            subscribeToStream sc sink Latest
            sendToClient isCancelled sc streamSend
    Right _ -> returnRes "inconsistent method called"

sendToClient :: MVar Bool -> SourceConnector -> (Struct -> IO (Either GRPCIOError ()))
             -> IO (ServerResponse 'ServerStreaming Struct)
sendToClient isCancelled sc@SourceConnector {..} ss@streamSend = do
  cancelled <- readMVar isCancelled
  if cancelled
  then return (ServerWriterResponse [] StatusUnknown "")
  else do
    sourceRecords <- readRecords
    let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
        structs                            = jsonObjectToStruct . fromJust <$> filter isJust objects'
    streamSendMany structs
  where
    streamSendMany xs = case xs of
      []      -> sendToClient isCancelled sc ss
      (x:xs') -> streamSend (structToStruct "SELECT" x) >>= \case
        Left err -> print err >> return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
        Right _  -> streamSendMany xs'

handlePushQueryCanceled :: ServerCall () -> MVar Bool -> IO () -> IO ()
handlePushQueryCanceled ServerCall{..} isCancelled handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left err   -> print err
    Right [] -> putStrLn "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b]
      -> when b $ swapMVar isCancelled True >> handle
    _ -> putStrLn "impossible happened"

runTaskWrapper :: TaskBuilder -> LDClient -> IO ()
runTaskWrapper taskBuilder ldclient = do
  -- create a new ckpReader from ldclient
  let readerName = textToCBytes (getTaskName taskBuilder )
  ldreader <- newLDFileCkpReader ldclient readerName checkpointRootPath 1000 Nothing 3
  -- create a new sourceConnector
  let sourceConnector = hstoreSourceConnector ldclient ldreader
  -- create a new sinkConnector
  let sinkConnector = hstoreSinkConnector ldclient
  -- RUN TASK
  runTask sourceConnector sinkConnector taskBuilder

checkpointRootPath :: CB.CBytes
checkpointRootPath = "/tmp/checkpoint"

catchZkException :: IO () -> String -> IO ()
catchZkException action str = catch action (\e -> return (e::SomeException) >> putStrLn str)
