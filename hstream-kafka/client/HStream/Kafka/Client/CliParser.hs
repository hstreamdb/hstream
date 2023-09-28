{-# LANGUAGE LambdaCase #-}

module HStream.Kafka.Client.CliParser where

import           Control.Exception
import           Control.Monad
import qualified Data.ByteString           as BS
import           Data.Int
import qualified Data.Vector               as V
import qualified Kafka.Protocol.Encoding   as K
import qualified Kafka.Protocol.Message    as K
import qualified Network.Socket            as NW
import qualified Network.Socket.ByteString as NW
import           Options.Applicative       as AP

data Command
  = Nodes
  | Topics
  | DescribeTopic String
  | CreateTopic String Int Int -- TODO: assignments, configs
  | DeleteTopic String
  | Groups
  | DescribeGroup String
  | Produce K.ProduceRequestV2
  deriving (Show)

data Options = Options
  { brokers        :: Maybe String
  , cluster        :: Maybe String
  , config         :: Maybe String
  , schemaRegistry :: Maybe String
  , verbose        :: Bool
  }
  deriving (Show)

parseCommand :: Parser Command
parseCommand =
  subparser $
    command "nodes"
      (info (pure Nodes) (progDesc "List nodes in a cluster"))
  <> command "topics"
      (info (pure Topics) (progDesc "List topics, partitions, and replicas"))
  <> command "topic"
      (info (DescribeTopic <$> argument str (metavar "TOPIC")) (progDesc "Describe a given topic"))
  <> command "create-topic"
      (info (CreateTopic <$> argument str (metavar "TOPIC")
                         <*> argument auto (metavar "PARTITIONS")
                         <*> argument auto (metavar "REPLICAS"))
        (progDesc "Create a new topic"))
  <> command "delete-topic"
      (info (DeleteTopic <$> argument str (metavar "TOPIC"))
        (progDesc "Delete a topic"))
  <> command "groups"
      (info (pure Groups) (progDesc "List consumer groups"))
  <> command "group"
      (info (DescribeGroup <$> argument str (metavar "GROUP"))
        (progDesc "Describe a given consumer group"))
  <> command "produce"
      (info (Produce <$> produceOptions)
        (progDesc "Produce data to a topic"))

produceOptions :: Parser K.ProduceRequestV2
produceOptions =
  K.ProduceRequestV2
    <$> (( \case
             "no"     -> 0
             "leader" -> 1
             "full"   -> -1
         )
          <$> option auto (short 'a' <> long "acks" <> metavar "ACKS" <> value "leader" <> help "The number of acknowledgments the producer requires the leader to have received"))
    <*> option auto (short 't' <> long "timeout" <> metavar "TIMEOUT" <> value 5000 <> help "The timeout to await a response in milliseconds")
    <*> (K.KaArray . Just . V.fromList <$> some produceTopicDataOptions)

produceTopicDataOptions :: Parser K.TopicProduceDataV2
produceTopicDataOptions =
  K.TopicProduceDataV2
    <$> argument str (metavar "TOPIC")
    <*> (K.KaArray . Just . V.fromList <$> some producePartitionDataOptions)

producePartitionDataOptions :: Parser K.PartitionProduceDataV2
producePartitionDataOptions =
  K.PartitionProduceDataV2
    <$> argument auto (metavar "PARTITION")
    <*> (Just <$> argument str (metavar "RECORD_DATA"))

parseOptions :: Parser Options
parseOptions =
  Options
    <$> optional (strOption (short 'b' <> long "brokers" <> metavar "BROKERS" <> help "Comma separated list of broker ip:port pairs"))
    <*> optional (strOption (short 'c' <> long "cluster" <> metavar "CLUSTER" <> help "Set a temporary current cluster"))
    <*> optional (strOption (long "config" <> metavar "CONFIG" <> help "Config file (default is $HOME/.kaf/config)"))
    <*> optional (strOption (long "schema-registry" <> metavar "SCHEMA_REGISTRY" <> help "URL to a Confluent schema registry. Used for attempting to decode Avro-encoded messages"))
    <*> switch (short 'v' <> long "verbose" <> help "Whether to turn on verbose logging")

parseCommandWithOptions :: Parser (Command, Options)
parseCommandWithOptions = (,) <$> parseCommand <*> parseOptions

runKafkaCliParser :: IO (Command, Options)
runKafkaCliParser = execParser $ info (parseCommandWithOptions <**> helper) mempty
