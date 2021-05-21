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
import qualified Data.ByteString.Lazy              as BL
import qualified Data.HashMap.Strict               as HM
import qualified Data.List                         as L
import qualified Data.Map                          as M
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (fromJust, isJust)
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
  scLDClient              :: LDClient,
  scDefaultTopicRepFactor :: Int
}
--------------------------------------------------------------------------------

handlers :: CB.CBytes -> IO (HStreamApi ServerRequest ServerResponse)
handlers logDeviceConfigPath = do
  ldclient <- newLDClient logDeviceConfigPath
  let serverContext =
        ServerContext {
          scLDClient = ldclient,
          scDefaultTopicRepFactor = 3
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
    Right (CreatePlan topic repFactor)                     -> do
      e' <- try $ createStream scLDClient (textToCBytes topic) (LogAttrs $ HsLogAttrs scDefaultTopicRepFactor Map.empty)
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when creating topic"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (CreateBySelectPlan sources sink taskBuilder repFactor) -> do
      e' <- try $ createStream scLDClient (textToCBytes sink) (LogAttrs $ HsLogAttrs scDefaultTopicRepFactor Map.empty)
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when creating topic"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          _ <- forkIO $ runTaskWrapper taskBuilder scLDClient
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (InsertPlan topic payload)             -> do
      timestamp <- getCurrentTimestamp
      e' <-  try $
        writeRecord
          (hstoreSinkConnector scLDClient)
          SinkRecord
            { snkStream = topic,
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

-- executePushQueryHandler :: DefaultInfo
--                         -> ServerRequest 'ServerStreaming CommandPushQuery Struct
--                         -> IO (ServerResponse 'ServerStreaming Struct)
-- executePushQueryHandler DefaultInfo{..} (ServerWriterRequest meta CommandPushQuery{..} streamSend) = do
--   plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
--   let returnRes = return . ServerWriterResponse [] StatusAborted
--   case plan' of
--     Left (e :: SomeSQLException)         -> returnRes "exception on parsing or codegen"
--     Right (SelectPlan sources sink task) -> do
--       exists <- mapM (doesTopicExists_ defaultAdmin) (CB.pack . T.unpack <$> sources)
--       if (not . and) exists then returnRes "some source topic do not exist"
--       else do
--         e' <- try $ createTopic_ defaultAdmin (CB.pack . T.unpack $ sink) (LogAttrs $ HsLogAttrs defaultTopicRepFactor Map.empty)
--         case e' of
--           Left (e :: SomeException) -> returnRes "error when creating sink topic"
--           Right _                   -> do
--             -- RUN TASK
--             tid <- forkIO $ do
--               name       <- newRandomName 10
--               logOptions <- logOptionsHandle stderr True
--               let producerConfig = defaultProducerConfig
--                   consumerConfig = defaultConsumerConfig
--                                    { consumerName = name
--                                    , consumerCheckpointUri = "/tmp/checkpoint/" <> name
--                                    }
--               withLogFunc logOptions $ \lf -> do
--                 let taskConfig = TaskConfig
--                       { tcMessageStoreType = LogDevice producerConfig consumerConfig
--                       , tcLogFunc = lf
--                       }
--                 runTask taskConfig task
--             -- FETCH RESULT TO CLIENT
--             name     <- newRandomName 10
--             consumer <- mkConsumer (defaultConsumerConfig
--                                     { consumerName = name
--                                     , consumerCheckpointUri = "/tmp/checkpoint" <> name
--                                     }) (CB.pack . T.unpack <$> [sink])
--             _ <- forkIO $ handleClientCallCanceled meta (killThread tid)
--             loop (sendToClient consumer streamSend)
--
--     Right _                              -> returnRes  "inconsistent method called"
--   where
--     loop f = f >>= \case
--         Just x  -> return x
--         Nothing -> loop f

executePushQueryHandler :: ServerContext
                        -> ServerRequest 'ServerStreaming CommandPushQuery Struct
                        -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler ServerContext{..} (ServerWriterRequest meta CommandPushQuery{..} streamSend) = do
  plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  let returnRes = return . ServerWriterResponse [] StatusAborted
  case plan' of
    Left (e :: SomeSQLException)         -> returnRes "exception on parsing or codegen"
    Right (SelectPlan sources sink taskBuilder) -> do
      exists <- mapM (doesStreamExists scLDClient) (CB.pack . T.unpack <$> sources)
      if (not . and) exists then returnRes "some source topic do not exist"
      else do
        e' <- try $ createStream scLDClient (textToCBytes sink) (LogAttrs $ HsLogAttrs scDefaultTopicRepFactor Map.empty)
        case e' of
          Left (e :: SomeException) -> returnRes "error when creating sink topic"
          Right _                   -> do
            tid <- forkIO $ runTaskWrapper taskBuilder scLDClient
            -- FETCH RESULT TO CLIENT
            _ <- forkIO $ handleClientCallCanceled meta (killThread tid)
            ldreader' <- newLDFileCkpReader scLDClient (textToCBytes (T.append (getTaskName taskBuilder) "-result")) checkpointRootPath 1 Nothing 3
            let sc = hstoreSourceConnector scLDClient ldreader'
            subscribeToStream sc sink Latest
            loop (sendToClient sc streamSend)


    Right _                              -> returnRes  "inconsistent method called"
  where
    loop f = f >>= \case
        Just x  -> return x
        Nothing -> loop f

sendToClient :: SourceConnector -> (Struct -> IO (Either GRPCIOError ())) -> IO (Maybe (ServerResponse 'ServerStreaming Struct))
sendToClient SourceConnector {..} streamSend = do
  sourceRecords <- readRecords
  let (objects' :: [Maybe Aeson.Object]) = Aeson.decode' . srcValue <$> sourceRecords
      structs                            = jsonObjectToStruct . fromJust <$> filter isJust objects'
  if L.null structs then return Nothing
  else
    let streamSendMany xs = case xs of
          []      -> return (Right ())
          (x:xs') -> streamSend x >>= \case
            Left err -> return (Left err)
            Right _  -> streamSendMany xs'
    in streamSendMany structs >>= \case
      Left err -> print err >> return (Just (ServerWriterResponse [] StatusAborted  "aborted"))
      Right _  -> return Nothing

handleClientCallCanceled :: ServerCall () -> IO () -> IO ()
handleClientCallCanceled ServerCall{..} handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left e                              -> print e
    Right []                            -> print "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b] -> when b handle

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

