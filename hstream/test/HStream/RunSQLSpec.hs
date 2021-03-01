{-# LANGUAGE OverloadedStrings #-}

module HStream.RunSQLSpec (spec) where

import           Control.Monad                (replicateM)
import           Data.Aeson                   (Value (..), encode)
import           Data.Scientific              (scientific)
import           HStream.Processing.Processor (TaskConfig (..), runTask)
import           HStream.Processing.Topic
import           HStream.Processing.Util      (getCurrentTimestamp)
import           HStream.SQL.Codegen
import           HStream.Store                (AdminClient,
                                               AdminClientConfig (..), Consumer,
                                               Producer, createTopics,
                                               mkAdminClient)
import           HStream.Store.Logger
import           HStream.Store.Stream         (TopicAttrs (..))
import           RIO
import           RIO.ByteString.Lazy          (fromStrict, toStrict)
import qualified RIO.ByteString.Lazy          as BL
import qualified RIO.HashMap                  as HM
import qualified RIO.Map                      as Map
import qualified RIO.Text                     as Text
import           System.IO.Unsafe             (unsafePerformIO)
import           System.Random
import           Test.Hspec
import           Text.Printf                  (printf)
import qualified Z.Data.CBytes                as CB

source1, source2, sink1, sink2 :: Text

source1 = unsafePerformIO $ newRandomName 10
{-# NOINLINE source1 #-}

source2 = unsafePerformIO $ newRandomName 10
{-# NOINLINE source2 #-}

sink1   = unsafePerformIO $ newRandomName 10
{-# NOINLINE sink1 #-}

sink2   = unsafePerformIO $ newRandomName 10
{-# NOINLINE sink2 #-}

adminClient :: AdminClient
adminClient = unsafePerformIO $
  mkAdminClient $ AdminClientConfig { adminConfigUri = "/data/store/logdevice.conf" }
{-# NOINLINE adminClient #-}

producerConfig :: ProducerConfig
producerConfig = ProducerConfig { producerConfigUri = "/data/store/logdevice.conf" }

consumerConfig :: ConsumerConfig
consumerConfig = ConsumerConfig
                 { consumerConfigUri = "/data/store/logdevice.conf",
                   consumerName = "demo",
                   consumerBufferSize = 4 * 1024,
                   consumerCheckpointUri = "/tmp/checkpoint",
                   consumerCheckpointRetries = 3
                 }

producer :: Producer
producer = unsafePerformIO $ mkProducer producerConfig
{-# NOINLINE producer #-}

getConsumer :: Text -> IO Consumer
getConsumer topicName = mkConsumer consumerConfig [topicName]

spec :: Spec
spec = describe "HStream.RunSQLSpec" $ do
  it "create streams" $
    (do
        setLogDeviceDbgLevel C_DBG_ERROR
        handleCreateStreamSQL $ "CREATE STREAM " <> source1 <> " WITH (FORMAT = \"JSON\");"
        handleCreateStreamSQL $ "CREATE STREAM " <> source2 <> " WITH (FORMAT = \"JSON\");"
        handleCreateStreamSQL $ "CREATE STREAM " <> sink1   <> " WITH (FORMAT = \"JSON\");"
        handleCreateStreamSQL $ "CREATE STREAM " <> sink2   <> " WITH (FORMAT = \"JSON\");"
    ) `shouldReturn` ()

  it "insert data to source topics" $
    (do
      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (22, 80);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (15, 10);"
    ) `shouldReturn` ()

  it "a simple SQL query" $
    (do
      result <- async . handleCreateBySelectSQL $
        "CREATE STREAM " <> sink1 <> " AS SELECT * FROM " <> source1 <> " EMIT CHANGES WITH (FORMAT = \"JSON\");"
      threadDelay 5000000
      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (31, 26);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (22, 80);"
      handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (15, 10);"
      handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (31, 26);"
      wait result
    ) `shouldReturn` ["{\"temperature\":31,\"humidity\":26}","{\"temperature\":15,\"humidity\":10}"]

  it "a more complex query" $
    (do
       result <- async . handleCreateBySelectSQL $ Text.pack $ printf
         "CREATE STREAM %s AS SELECT SUM(%s.humidity) AS result FROM %s INNER JOIN %s WITHIN (INTERVAL 5 SECOND) ON (%s.temperature = %s.temperature) WHERE %s.humidity > 20 GROUP BY %s.humidity, TUMBLING (INTERVAL 10 SECOND) EMIT CHANGES WITH (FORMAT = \"JSON\");" sink2 source2 source2 source1 source2 source1 source2 source2
       threadDelay 5000000
       handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (31, 26);"
       handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (22, 80);"
       handleInsertSQL $ "INSERT INTO " <> source1 <> " (temperature, humidity) VALUES (15, 10);"
       handleInsertSQL $ "INSERT INTO " <> source2 <> " (temperature, humidity) VALUES (31, 26);"
       wait result
    ) `shouldReturn` ["{\"result\":26}"]

sendProducerRecord :: Text -> ByteString -> IO ()
sendProducerRecord topicName value = do
  timestamp <- getCurrentTimestamp
  send producer RawProducerRecord { rprTopic = topicName
                                  , rprKey = defaultKey
                                  , rprValue = fromStrict value
                                  , rprTimestamp = timestamp
                                  }
  where
    defaultKey = Just $ encode $ HM.fromList [ ("key" :: Text, Number (scientific (toInteger 1) 0))]

handleCreateStreamSQL :: Text -> IO ()
handleCreateStreamSQL sql = do
  plan <- streamCodegen sql
  case plan of
    CreatePlan topicName ->
      createTopics adminClient
      (Map.singleton (CB.pack . Text.unpack $ topicName) TopicAttrs {replicationFactor = 3})
    _ -> error "Execution plan type mismatched"


handleInsertSQL :: Text -> IO ()
handleInsertSQL sql = do
  plan <- streamCodegen sql
  case plan of
    InsertPlan topicName payload -> do
      sendProducerRecord topicName (toStrict payload)
    _ -> error "Execution plan type mismatched"

handleCreateBySelectSQL :: Text -> IO [BL.ByteString]
handleCreateBySelectSQL sql = do
  plan <- streamCodegen sql
  case plan of
    CreateBySelectPlan _ sink task -> do
      logOptions <- logOptionsHandle stderr True
      _ <- async $ withLogFunc logOptions $ \lf -> do
        let taskConfig =
              TaskConfig
              { tcMessageStoreType = LogDevice producerConfig consumerConfig,
                tcLogFunc = lf
              }
        runTask taskConfig task
      mc <- getConsumer sink
      records <- replicateM 20 $ do
        threadDelay 500000
        pollRecords mc 10 1000
      return $ rcrValue <$> concat records
    _ -> error "Execution plan type mismatch"

newRandomName :: Int -> IO Text
newRandomName n = Text.pack . take n . randomRs ('a', 'z') <$> newStdGen
