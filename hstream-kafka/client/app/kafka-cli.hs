module Main where

import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                as BS
import           Data.Int
import qualified Data.Vector                    as V
import qualified HStream.Logger                 as Log
import qualified Kafka.Protocol.Encoding        as K
import qualified Kafka.Protocol.Message         as K
import qualified Network.Socket                 as NW
import qualified Network.Socket.ByteString      as NW
import           Options.Applicative            as AP

import           HStream.Kafka.Client.CliParser
import           HStream.Kafka.Client.Core
import           HStream.Kafka.Client.Network


main :: IO ()
main = do
  parsed@(cmd, opts) <- runKafkaCliParser
  Log.debug . Log.buildString $ "command and options: " <> show parsed
  case cmd of
    Nodes                 -> handleCmdNodes opts
    Topics                -> handleCmdTopics opts
    DescribeTopic topic   -> handleCmdDescribeTopic opts topic
    Groups                -> handleCmdGroups opts
    DescribeGroup group   -> handleCmdDescribeGroup opts group
    DeleteTopic topicName -> handleDeleteTopic opts topicName
    CreateTopic topicName numPartitions replicationFactor ->
      handleCreateTopic opts topicName numPartitions replicationFactor
    Produce request       -> handleProduce opts request
