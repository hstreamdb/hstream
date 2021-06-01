{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

import           HStream.Server.HStoreConnector
import           HStream.Server.HStreamApi
import           Network.GRPC.HighLevel.Generated
import           ThirdParty.Google.Protobuf.Struct

import           Control.Concurrent
import           Control.Exception
import           Control.Monad                     (when)
import qualified Data.Aeson                        as Aeson
import qualified Data.ByteString                   as B
import qualified Data.ByteString.Lazy              as BL
import qualified Data.HashMap.Strict               as HM
import qualified Data.List                         as L
import qualified Data.Map                          as M
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (fromJust, isJust)
import           Data.String                       (fromString)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import qualified Data.Vector                       as V
import           HStream.Processing.Connector
import           HStream.Processing.Processor      (TaskBuilder, getTaskName,
                                                    runTask)
import           HStream.Processing.Type
import           HStream.Processing.Util           (getCurrentTimestamp)
import           HStream.SQL.Codegen
import           HStream.SQL.Exception
import           HStream.Server.Utils
import           HStream.Store
import           Network.GRPC.LowLevel.Op          (Op (OpRecvCloseOnServer),
                                                    OpRecvResult (OpRecvCloseOnServerResult),
                                                    runOps)
import           RIO                               (logOptionsHandle, stderr,
                                                    withLogFunc)
import           System.IO.Unsafe                  (unsafePerformIO)
import           System.Random                     (newStdGen, randomRs)
import qualified Z.Data.CBytes                     as CB
import qualified Z.Foreign                         as ZF

--------------------------------------------------------------------------------
newRandomName :: Int -> IO CB.CBytes
newRandomName n = CB.pack . take n . randomRs ('a', 'z') <$> newStdGen

data ServerContext = ServerContext {
  scLDClient               :: LDClient,
  scDefaultStreamRepFactor :: Int
}
--------------------------------------------------------------------------------

handlers :: CB.CBytes -> IO (HStreamApi ServerRequest ServerResponse)
handlers logDeviceConfigPath = do
  ldclient <- newLDClient logDeviceConfigPath
  let serverContext =
        ServerContext {
          scLDClient = ldclient,
          scDefaultStreamRepFactor = 3
        }
  return
    HStreamApi { hstreamApiExecuteQuery     = executeQueryHandler serverContext
               , hstreamApiExecutePushQuery = executePushQueryHandler serverContext
               }

genErrorStruct :: TL.Text -> Struct
genErrorStruct = Struct . Map.singleton "errMsg" . Just . Value . Just . ValueKindStringValue

genErrorQueryResponse :: TL.Text -> CommandQueryResponse
genErrorQueryResponse msg = CommandQueryResponse (Just . CommandQueryResponseKindResultSet . CommandQueryResultSet $ V.singleton (genErrorStruct msg))

genSuccessQueryResponse :: CommandQueryResponse
genSuccessQueryResponse = CommandQueryResponse (Just . CommandQueryResponseKindSuccess $ CommandSuccess)

executeQueryHandler :: ServerContext
                    -> ServerRequest 'Normal CommandQuery CommandQueryResponse
                    -> IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler ServerContext{..} (ServerNormalRequest _metadata CommandQuery{..}) = do
  plan' <- try $ streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    Left (e :: SomeSQLException) -> do
      let resp = genErrorQueryResponse "error on parsing or codegen"
      return (ServerNormalResponse resp [] StatusUnknown "")
    Right SelectPlan{}           -> do
      let resp = genErrorQueryResponse "inconsistent method called"
      return (ServerNormalResponse resp [] StatusUnknown "")
    Right ShowPlan{}           -> do
      let resp = genErrorQueryResponse "inconsistent method called"
      return (ServerNormalResponse resp [] StatusUnknown "")
    Right (CreatePlan stream repFactor)                     -> do
      e' <- try $ createStream scLDClient (transToStreamName stream) (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when creating stream"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (CreateBySelectPlan sources sink taskBuilder repFactor) -> do
      e' <- try $ createStream scLDClient (transToStreamName sink) (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when creating stream"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          _ <- forkIO $ runTaskWrapper taskBuilder scLDClient
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (InsertPlan stream payload)             -> do
      timestamp <- getCurrentTimestamp
      e' <-  try $
        writeRecord
          (hstoreSinkConnector scLDClient)
          SinkRecord
            { snkStream = stream,
              snkKey = Nothing,
              snkValue = payload,
              snkTimestamp = timestamp
            }
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when inserting msg"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (DropPlan checkIfExist stream) -> do
      streamExists <- doesStreamExists scLDClient (transToStreamName stream)
      if streamExists then do
        remove' <- try $ removeStream scLDClient (transToStreamName stream)
        case remove' of
          Left (e :: SomeException) -> do
            let resp = genErrorQueryResponse "error removing stream"
            return (ServerNormalResponse resp [] StatusUnknown "")
          Right ()                  -> do
            let resp = genSuccessQueryResponse
            return (ServerNormalResponse resp [] StatusOk "")
      else do
        if checkIfExist then do
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk  "")
        else do
          let resp = genErrorQueryResponse "stream does not exist"
          return (ServerNormalResponse resp [] StatusUnknown "")

executePushQueryHandler :: ServerContext
                        -> ServerRequest 'ServerStreaming CommandPushQuery Struct
                        -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..} (ServerWriterRequest meta CommandPushQuery{..} streamSend) = do
  plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  let returnRes = return . ServerWriterResponse [] StatusAborted
  case plan' of
    Left (e :: SomeSQLException)         -> returnRes "exception on parsing or codegen"
    Right (SelectPlan sources sink taskBuilder) -> do
      exists <- mapM (doesStreamExists scLDClient . transToStreamName) sources
      if (not . and) exists then returnRes "some source stream do not exist"
      else do
        e' <- try $ createStream scLDClient (transToStreamName sink) (LogAttrs $ HsLogAttrs scDefaultStreamRepFactor Map.empty)
        case e' of
          Left (e :: SomeException) -> returnRes "error when creating sink stream"
          Right _                   -> do
            tid <- forkIO $ runTaskWrapper taskBuilder scLDClient
            -- FETCH RESULT TO CLIENT
            isCancelled <- newMVar False
            _ <- forkIO $ handlePushQueryCanceled meta isCancelled (killThread tid)
            ldreader' <- newLDFileCkpReader scLDClient (textToCBytes (T.append (getTaskName taskBuilder) "-result")) checkpointRootPath 1 Nothing 3
            let sc = hstoreSourceConnector scLDClient ldreader'
            subscribeToStream sc sink Latest
            sendToClient isCancelled sc streamSend
    Right (ShowPlan showObject) -> do
      case showObject of
        Streams -> do
          names' <- try $ findStreams scLDClient True
          case names' of
            Left (e :: SomeException) -> returnRes "getting log names from directory"
            Right names -> do
              streamSend (listToStruct "SHOW" $ cbytesToValue . getStreamName <$> L.sort names) >>= \case
                Left err -> print err >> return (ServerWriterResponse [] StatusUnknown (fromString (show err)))
                Right _  -> return (ServerWriterResponse [] StatusOk "names of streams are returned")
        _ -> returnRes "not Supported"
    Right _                              -> returnRes "inconsistent method called"

sendToClient :: MVar Bool -> SourceConnector -> (Struct -> IO (Either GRPCIOError ())) -> IO (ServerResponse 'ServerStreaming Struct)
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
    Left e                              -> print e
    Right []                            -> print "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b] -> when b $ do
                                            swapMVar isCancelled True
                                            handle

runTaskWrapper :: TaskBuilder ->  LDClient -> IO ()
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
