{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
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
executePushQueryHandler DefaultInfo{..} (ServerWriterRequest _metadata CommandPushQuery{..} streamSend) = do
  plan' <- try $ streamCodegen (TL.toStrict commandPushQueryQueryText)
  case plan' of
    Left (e :: SomeSQLException)         -> return (ServerWriterResponse [] StatusAborted "exception on parsing or codegen")
    Right (SelectPlan sources sink task) -> do
      exists <- mapM (doesTopicExists defaultAdmin) (CB.pack . T.unpack <$> sources)
      case and exists of
          False -> return (ServerWriterResponse [] StatusAborted "some source topic do not exist")
          True  -> do
            e' <- try $ createTopics defaultAdmin (M.fromList [(CB.pack . T.unpack $ sink, TopicAttrs defaultTopicRepFactor Map.empty)])
            case e' of
              Left (e :: SomeException) -> return (ServerWriterResponse [] StatusAborted "error when creating sink topic")
              Right _                   -> do
                -- RUN TASK
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
                -- FETCH RESULT TO CLIENT
                name     <- newRandomName 10
                consumer <- mkConsumer (defaultConsumerConfig
                                        { consumerName = name
                                        , consumerCheckpointUri = "/tmp/checkpoint" <> name
                                        }) (CB.pack . T.unpack <$> [sink])
                loop consumer
    Right _                              -> return (ServerWriterResponse [] StatusAborted "inconsistent method called")
  where loop consumer = do
          ms <- pollMessages consumer 1 1000
          let valBs = ZF.toByteString . dataOutValue <$> ms
          let maybeObjectsWithBs = [ (Aeson.decode . BL.fromStrict $ bs :: Maybe Aeson.Object, bs)| bs <- valBs ]
          let objects = [ fromJust o' | (o',_) <- maybeObjectsWithBs, isJust o' ]
              structs = jsonObjectToStruct <$> objects
          case L.null structs of
            True  -> loop consumer
            False -> do
              let streamSendMany xs = do
                    case xs of
                      []      -> return (Right ())
                      (x:xs') -> do
                        res <- streamSend x
                        case res of
                          Left err -> return (Left err)
                          Right _  -> streamSendMany xs'
              sendRes <- streamSendMany structs
              case sendRes of
                Left err -> print err >> return (ServerWriterResponse [] StatusAborted  "aborted")
                Right _  -> threadDelay 1000000 >> loop consumer
