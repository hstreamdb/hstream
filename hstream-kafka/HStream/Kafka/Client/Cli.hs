{-# LANGUAGE LambdaCase #-}

module HStream.Kafka.Client.Cli
  ( Command (..)
  , runCliParser
  , handleCmdNodes
  , handleCmdTopics
  , handleCmdDescribeTopic
  , handleCmdGroups
  , handleCmdDescribeGroup
  , handleCreateTopic
  , handleDeleteTopic
  , handleProduce
  ) where

import           Data.Int
import qualified Data.Text                as T
import qualified Data.Vector              as V
import           Options.Applicative

import           HStream.Base.Table       as Table
import qualified HStream.Kafka.Client.Api as KA
import qualified Kafka.Protocol.Encoding  as K
import qualified Kafka.Protocol.Message   as K

-------------------------------------------------------------------------------

data Options = Options
  { host :: !String
  , port :: !Int
  } deriving (Show)

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

-------------------------------------------------------------------------------

parseOptions :: Parser Options
parseOptions =
  Options
    <$> strOption (long "host" <> metavar "HOST" <> value "127.0.0.1" <> help "Server host")
    <*> option auto (long "port" <> metavar "PORT" <> value 9092 <> help "Server port")

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
          <$> option auto (short 'a' <> long "acks" <> metavar "ACKS" <> value "full" <> help "The number of acknowledgments the producer requires the leader to have received"))
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

parseCommandWithOptions :: Parser (Command, Options)
parseCommandWithOptions = (,) <$> parseCommand <*> parseOptions

runCliParser :: IO (Command, Options)
runCliParser = execParser $ info (parseCommandWithOptions <**> helper) mempty

-------------------------------------------------------------------------------

-- For cli, we use a fixed correlationId
correlationId :: Int32
correlationId = 1

timeoutMs :: Int32
timeoutMs = 5000

handleCmdNodes :: Options -> IO ()
handleCmdNodes Options{..} = do
  let req = K.MetadataRequestV0 (K.KaArray $ Just V.empty)
  K.MetadataResponseV1 brokers controllerId _topics <-
    KA.withSendAndRecv host port (KA.metadata correlationId req)
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
handleCreateTopic Options{..} topicName numPartitions replicationFactor = do
  let topic = K.CreatableTopicV0
                (T.pack topicName)
                (fromIntegral numPartitions)
                (fromIntegral replicationFactor)
                (K.KaArray $ Just V.empty)
                (K.KaArray $ Just V.empty)
      req = K.CreateTopicsRequestV0
              (K.KaArray $ Just $ V.singleton topic)
              timeoutMs
  K.CreateTopicsResponseV0 (K.KaArray (Just rets)) <-
    KA.withSendAndRecv host port (KA.createTopics correlationId req)
  let titles = ["TOPIC NAME", "ERROR CODE"]
      lenses = [ \(K.CreatableTopicResultV0 name errorCode) -> show name
               , \(K.CreatableTopicResultV0 name errorCode) -> show errorCode
               ]
      stats  = (\s -> ($ s) <$> lenses) <$> rets
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) (V.toList stats)

handleDeleteTopic :: Options -> String -> IO ()
handleDeleteTopic Options{..} topicName = do
  let req = K.DeleteTopicsRequestV0
              (K.KaArray $ Just $ V.singleton (T.pack topicName))
              timeoutMs
  K.DeleteTopicsResponseV0 (K.KaArray (Just rets)) <-
    KA.withSendAndRecv host port (KA.deleteTopics correlationId req)
  let titles = ["TOPIC NAME", "ERROR CODE"]
      lenses = [ \(K.DeletableTopicResultV0 name errorCode) -> show name
               , \(K.DeletableTopicResultV0 name errorCode) -> show errorCode
               ]
      stats  = (\s -> ($ s) <$> lenses) <$> rets
  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) (V.toList stats)

-- TODO
handleProduce :: Options -> K.ProduceRequestV2 -> IO ()
handleProduce = undefined
--handleProduce :: Options -> K.ProduceRequestV2 -> IO ()
--handleProduce Options{..} req = do
--  K.ProduceResponseV2 responses _ <-
--    KA.withSendAndRecv host port (KA.produce correlationId req)
--  let K.KaArray (Just responses') = responses
--      (K.TopicProduceResponseV2 _ response) = responses' V.! 0
--      K.KaArray (Just (response' :: V.Vector K.PartitionProduceResponseV2)) = (response :: K.KaArray K.PartitionProduceResponseV2)
--  let titles :: [String]
--      titles = ["INDEX", "ERROR CODE", "BASE OFFSET", "LOG APPEND TIME (ms)"]
--      lenses :: [K.PartitionProduceResponseV2 -> String]
--      lenses = [ \(K.PartitionProduceResponseV2 index errorCode baseOffset logAppendTimeMs) -> show index
--               , \(K.PartitionProduceResponseV2 index errorCode baseOffset logAppendTimeMs) -> show errorCode
--               , \(K.PartitionProduceResponseV2 index errorCode baseOffset logAppendTimeMs) -> show baseOffset
--               , \(K.PartitionProduceResponseV2 index errorCode baseOffset logAppendTimeMs) -> show logAppendTimeMs
--               ]
--      stats  = (\s -> ($ s) <$> lenses) <$> (response' :: V.Vector K.PartitionProduceResponseV2)
--  putStrLn $ simpleShowTable (map (, 30, Table.left) titles) (V.toList stats)
