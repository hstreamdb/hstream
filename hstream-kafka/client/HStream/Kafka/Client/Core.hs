module HStream.Kafka.Client.Core where

import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                as BS
import           Data.Int
import qualified Data.Text                      as T
import qualified Data.Vector                    as V
import qualified HStream.Logger                 as Log
import qualified Kafka.Protocol.Encoding        as K
import qualified Kafka.Protocol.Message         as K
import qualified Network.Socket                 as NW
import qualified Network.Socket.ByteString      as NW
import           Options.Applicative            as AP

import           HStream.Base.Table             as Table
import           HStream.Kafka.Client.CliParser
import           HStream.Kafka.Client.Network

requestClientId  = Right $ Just "kafka-hstream-dev"
defaultTimeoutMs = 5000

getAllMetadata :: Options -> IO K.MetadataResponseV1
getAllMetadata opts = do
  Log.debug . Log.buildString $ "getAllMetadata: begin"
  let reqHeader = K.RequestHeader
                    { K.requestApiKey        = 3
                    , K.requestApiVersion    = 1
                    , K.requestCorrelationId = 1 -- FIXME
                    , K.requestClientId      = requestClientId
                    , K.requesteTaggedFields = Left K.Unsupported
                    }
      reqBody   = K.MetadataRequestV0
                    (K.KaArray $ Just V.empty)
      reqBs'    = K.runPut reqHeader <> K.runPut @K.MetadataRequestV1 reqBody
      reqLen    = K.runPut @Int32 $ fromIntegral (BS.length reqBs')
      reqBs     = reqLen <> reqBs'
  Log.debug . Log.buildString $ "getAllMetadata: ready to send"
  ret <- withSock opts $ \sock -> do
    Log.debug . Log.buildString $ "getAllMetadata: conn"
    NW.sendAll sock reqBs
    Log.debug . Log.buildString $ "getAllMetadata: req sent"
    recvResp @K.MetadataResponseV1 sock
  Log.debug . Log.buildString $ "getAllMetadata: end"
  pure ret

-- describeConfigs :: Options -> IO K.DescribeConfigsV4
-- describeConfigs opts = undefined

handleCmdNodes :: Options -> IO ()
handleCmdNodes opts = do
  Log.debug . Log.buildString $ "handleCmdNodes: begin"
  K.MetadataResponseV1 brokers controllerId _topics <- getAllMetadata opts
  let titles   = ["ID", "ADDRESS", "CONTROLLER"]
      lenses   = [ \(K.MetadataResponseBrokerV1 nodeId host port rack) -> show nodeId                       -- broker.nodeId
                 , \(K.MetadataResponseBrokerV1 nodeId host port rack) -> T.unpack host <> ":" <> show port -- broker.host <> broker.port
                 , \(K.MetadataResponseBrokerV1 nodeId host port rack) -> show (nodeId == controllerId)     -- broker.nodeId == controllerId
                 ]
      brokers' = let K.KaArray (Just xs) = brokers
                 in V.toList xs
      stats    = (\s -> ($ s) <$> lenses) <$> brokers'
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) stats

-- "NAME\tPARTITIONS\tREPLICAS\t\n"
-- topic.name, topic.NumPartitions, topic.ReplicationFactor
handleCmdTopics :: Options -> IO ()
handleCmdTopics = undefined

handleCmdDescribeTopic :: Options -> String -> IO ()
handleCmdDescribeTopic = undefined

handleCmdGroups :: Options -> IO ()
handleCmdGroups = undefined

handleCmdDescribeGroup :: Options -> String -> IO ()
handleCmdDescribeGroup = undefined

handleCreateTopic :: Options -> String -> Int -> Int -> IO ()
handleCreateTopic opts topicName numPartitions replicationFactor = do
  let topic = K.CreatableTopicV0
                (T.pack topicName)
                (fromIntegral numPartitions)
                (fromIntegral replicationFactor)
                (K.KaArray $ Just [])
                (K.KaArray $ Just [])
  let reqHeader = K.RequestHeader
                    { K.requestApiKey        = 19
                    , K.requestApiVersion    = 0
                    , K.requestCorrelationId = 1 -- FIXME
                    , K.requestClientId      = requestClientId
                    , K.requesteTaggedFields = Left K.Unsupported
                    }
      reqBody   = K.CreateTopicsRequestV0
                    (K.KaArray $ Just [topic])
                    defaultTimeoutMs
  resp <- sendAndRecv @K.CreateTopicsRequestV0 @K.CreateTopicsResponseV0
            opts reqHeader reqBody
  let K.CreateTopicsResponseV0 (K.KaArray (Just rets)) = resp
  let titles = ["TOPIC NAME", "ERROR CODE"]
      lenses = [ \(K.CreatableTopicResultV0 name errorCode) -> show name
               , \(K.CreatableTopicResultV0 name errorCode) -> show errorCode
               ]
      stats  = (\s -> ($ s) <$> lenses) <$> rets
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) (V.toList stats)

handleDeleteTopic :: Options -> String -> IO ()
handleDeleteTopic opts topicName = do
  let reqHeader = K.RequestHeader
                    { K.requestApiKey        = 20
                    , K.requestApiVersion    = 0
                    , K.requestCorrelationId = 1 -- FIXME
                    , K.requestClientId      = requestClientId
                    , K.requesteTaggedFields = Left K.Unsupported
                    }
      reqBody   = K.DeleteTopicsRequestV0
                    (K.KaArray $ Just [T.pack topicName])
                    defaultTimeoutMs
  resp <- sendAndRecv @K.DeleteTopicsRequestV0 @K.DeleteTopicsResponseV0
            opts reqHeader reqBody
  let K.DeleteTopicsResponseV0 (K.KaArray (Just rets)) = resp
  let titles = ["TOPIC NAME", "ERROR CODE"]
      lenses = [ \(K.DeletableTopicResultV0 name errorCode) -> show name
               , \(K.DeletableTopicResultV0 name errorCode) -> show errorCode
               ]
      stats  = (\s -> ($ s) <$> lenses) <$> rets
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) (V.toList stats)
