{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler where

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
import           HStream.Processing.Processor      (MessageStoreType (LogDevice),
                                                    TaskConfig (..), runTask)
import           HStream.Processing.Util           (getCurrentTimestamp)
import           HStream.SQL.Codegen
import           HStream.SQL.Exception
import           HStream.Server.Utils
import           HStream.Store                     (AdminClient,
                                                    AdminClientConfig (..),
                                                    Consumer,
                                                    ConsumerConfig (..),
                                                    Producer,
                                                    ProducerConfig (..),
                                                    ProducerRecord (..),
                                                    TopicAttrs (..),
                                                    createTopics, dataOutValue,
                                                    doesTopicExists,
                                                    mkAdminClient, mkConsumer,
                                                    mkProducer, pollMessages,
                                                    sendMessage)
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

data DefaultInfo = DefaultInfo
  { defaultConsumerConfig :: ConsumerConfig
  , defaultProducerConfig :: ProducerConfig
  , defaultProducer       :: Producer
  , defaultAdmin          :: AdminClient
  , defaultTopicRepFactor :: Int
  }

--------------------------------------------------------------------------------

handlers :: CB.CBytes -> HStreamApi ServerRequest ServerResponse
handlers logDeviceConfigPath =
  let defaultInfo = DefaultInfo
        { defaultConsumerConfig = ConsumerConfig
                                  { consumerConfigUri = logDeviceConfigPath
                                  , consumerName = "defaultConsumerName"
                                  , consumerBufferSize = -1
                                  , consumerCheckpointUri = "defaultCheckpointUri"
                                  , consumerCheckpointRetries = 3
                                  }
      , defaultProducerConfig = ProducerConfig
                                  { producerConfigUri = logDeviceConfigPath
                                  }
      , defaultProducer = unsafePerformIO $ mkProducer    (ProducerConfig logDeviceConfigPath)
      , defaultAdmin    = unsafePerformIO $ mkAdminClient (AdminClientConfig logDeviceConfigPath)
      , defaultTopicRepFactor = 3
      }
   in HStreamApi { hstreamApiExecuteQuery     = executeQueryHandler defaultInfo
                 , hstreamApiExecutePushQuery = executePushQueryHandler defaultInfo
                 }

genErrorStruct :: TL.Text -> Struct
genErrorStruct = Struct . Map.singleton "errMsg" . Just . Value . Just . ValueKindStringValue

genErrorQueryResponse :: TL.Text -> CommandQueryResponse
genErrorQueryResponse msg = CommandQueryResponse (Just . CommandQueryResponseKindResultSet . CommandQueryResultSet $ V.singleton (genErrorStruct msg))

genSuccessQueryResponse :: CommandQueryResponse
genSuccessQueryResponse = CommandQueryResponse (Just . CommandQueryResponseKindSuccess $ CommandSuccess)

executeQueryHandler :: DefaultInfo
                    -> ServerRequest 'Normal CommandQuery CommandQueryResponse
                    -> IO (ServerResponse 'Normal CommandQueryResponse)
executeQueryHandler DefaultInfo{..} (ServerNormalRequest _metadata CommandQuery{..}) = do
  plan' <- try $ streamCodegen (TL.toStrict commandQueryStmtText)
  case plan' of
    Left (e :: SomeSQLException) -> do
      let resp = genErrorQueryResponse "error on parsing or codegen"
      return (ServerNormalResponse resp [] StatusUnknown "")
    Right SelectPlan{}           -> do
      let resp = genErrorQueryResponse "inconsistent method called"
      return (ServerNormalResponse resp [] StatusUnknown "")
    Right (CreatePlan topic repFactor)                     -> do
      e' <- try $ createTopics defaultAdmin
            (M.fromList [(CB.pack . T.unpack $ topic, TopicAttrs repFactor Map.empty)])
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when creating topic"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (CreateBySelectPlan sources sink task repFactor) -> do
      e' <- try $ createTopics defaultAdmin
            (M.fromList [(CB.pack . T.unpack $ sink, TopicAttrs repFactor Map.empty)])
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when creating topic"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          _ <- forkIO $ do
            name       <- newRandomName 10
            logOptions <- logOptionsHandle stderr True
            let producerConfig = defaultProducerConfig
                consumerConfig = defaultConsumerConfig
                                 { consumerName = name
                                 , consumerCheckpointUri = "/tmp/checkpoint/" <> name
                                 }
            withLogFunc logOptions $ \lf -> do
              let taskConfig = TaskConfig
                    { tcMessageStoreType = LogDevice producerConfig consumerConfig
                    , tcLogFunc = lf
                    }
              runTask taskConfig task
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")
    Right (InsertPlan topic payload)             -> do
      time <- getCurrentTimestamp
      e'   <- try $ sendMessage defaultProducer $
                    ProducerRecord
                      (CB.pack . T.unpack $ topic)
                      (Just . CB.fromBytes . ZF.fromByteString . BL.toStrict . Aeson.encode $
                        HM.fromList [("key" :: T.Text, Aeson.String "demoKey")])
                      (ZF.fromByteString . BL.toStrict $ payload)
                      time
      case e' of
        Left (e :: SomeException) -> do
          let resp = genErrorQueryResponse "error when inserting msg"
          return (ServerNormalResponse resp [] StatusUnknown "")
        Right ()                  -> do
          let resp = genSuccessQueryResponse
          return (ServerNormalResponse resp [] StatusOk "")

executePushQueryHandler :: DefaultInfo
                        -> ServerRequest 'ServerStreaming CommandPushQuery Struct
                        -> IO (ServerResponse 'ServerStreaming Struct)
executePushQueryHandler DefaultInfo{..} (ServerWriterRequest meta CommandPushQuery{..} streamSend) = do
  plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  let returnRes = return . ServerWriterResponse [] StatusAborted
  case plan' of
    Left (e :: SomeSQLException)         -> returnRes "exception on parsing or codegen"
    Right (SelectPlan sources sink task) -> do
      exists <- mapM (doesTopicExists defaultAdmin) (CB.pack . T.unpack <$> sources)
      if (not . and) exists then returnRes "some source topic do not exist"
      else do
        e' <- try $ createTopics defaultAdmin (M.fromList [(CB.pack . T.unpack $ sink, TopicAttrs defaultTopicRepFactor Map.empty)])
        case e' of
          Left (e :: SomeException) -> returnRes "error when creating sink topic"
          Right _                   -> do
            -- RUN TASK
            tid <- forkIO $ do
              name       <- newRandomName 10
              logOptions <- logOptionsHandle stderr True
              let producerConfig = defaultProducerConfig
                  consumerConfig = defaultConsumerConfig
                                   { consumerName = name
                                   , consumerCheckpointUri = "/tmp/checkpoint/" <> name
                                   }
              withLogFunc logOptions $ \lf -> do
                let taskConfig = TaskConfig
                      { tcMessageStoreType = LogDevice producerConfig consumerConfig
                      , tcLogFunc = lf
                      }
                runTask taskConfig task
            -- FETCH RESULT TO CLIENT
            name     <- newRandomName 10
            consumer <- mkConsumer (defaultConsumerConfig
                                    { consumerName = name
                                    , consumerCheckpointUri = "/tmp/checkpoint" <> name
                                    }) (CB.pack . T.unpack <$> [sink])
            _ <- forkIO $ handleClientCallCanceled meta (killThread tid)
            loop (sendToClient consumer streamSend)

    Right _                              -> returnRes  "inconsistent method called"
  where
    loop f = f >>= \case
        Just x  -> return x
        Nothing -> loop f

sendToClient :: Consumer -> (Struct -> IO (Either GRPCIOError ())) -> IO (Maybe (ServerResponse 'ServerStreaming Struct))
sendToClient consumer streamSend = do
  ms <- pollMessages consumer 1 1000
  let (objects' :: [Maybe Aeson.Object]) = Aeson.decode . BL.fromStrict . ZF.toByteString . dataOutValue <$> ms
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
