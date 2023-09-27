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
  deriving (Show)

data Options = Options
  { brokers        :: Maybe String,
    cluster        :: Maybe String,
    config         :: Maybe String,
    schemaRegistry :: Maybe String,
    verbose        :: Bool
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
