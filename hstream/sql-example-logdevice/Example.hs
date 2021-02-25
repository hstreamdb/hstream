{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Data.Aeson
import qualified Data.HashMap.Strict          as HM
import           Data.Scientific
import           Data.Text                    (unpack)
import           Data.Text.IO                 (getLine)
import           HStream.Processing.Processor
import           HStream.Processing.Topic
import           HStream.Processing.Util
import           HStream.SQL.Codegen          (ExecutionPlan (..),
                                               streamCodegen)
import           HStream.Store                (AdminClientConfig (..),
                                               createTopics, mkAdminClient)
import           HStream.Store.Logger
import           HStream.Store.Stream         (TopicAttrs (..))
import qualified Prelude                      as P
import           RIO
import qualified RIO.ByteString.Lazy          as BL
import qualified RIO.Map                      as Map
import           System.Random                (Random (randomR), getStdRandom)
import           Z.Data.CBytes                (pack)

---------------------------------- Example -------------------------------------
-- CREATE STREAM demoSink AS SELECT * FROM source1 EMIT CHANGES WITH (FORMAT = "JSON");

-- CREATE STREAM demoSink AS SELECT SUM(source2.humidity) AS result FROM source2 INNER JOIN source1 WITHIN (INTERVAL 5 SECOND) ON (source2.temperature = source1.temperature) WHERE source2.humidity > 20 GROUP BY source2.humidity, TUMBLING (INTERVAL 10 SECOND) EMIT CHANGES WITH (FORMAT = "JSON");
--------------------------------------------------------------------------------

main :: IO ()
main = getLine >>= run

run :: Text -> IO ()
run input = do
  plan <- streamCodegen input
  (sTopicName,task) <- case plan of
    SelectPlan source sink task         -> return (sink,task)
    CreateBySelectPlan source sink task -> return (sink,task)
    _                                   -> error "Not supported"

  let tTopicName = "source1"
  let hTopicName = "source2"

  setLogDeviceDbgLevel C_DBG_ERROR
  let producerConfig =
        ProducerConfig
          { producerConfigUri = "/data/store/logdevice.conf"
          }
  let consumerConfig =
        ConsumerConfig
          { consumerConfigUri = "/data/store/logdevice.conf",
            consumerName = "demo",
            consumerBufferSize = 4 * 1024,
            consumerCheckpointUri = "/tmp/checkpoint",
            consumerCheckpointRetries = 3
          }
  let adminConfig =
        AdminClientConfig {adminConfigUri = "/data/store/logdevice.conf"}
  adminClient <- mkAdminClient adminConfig
  createTopics adminClient (Map.singleton "source1" TopicAttrs {replicationFactor = 3})
  createTopics adminClient (Map.singleton "source2" TopicAttrs {replicationFactor = 3})
  createTopics adminClient (Map.singleton (pack.unpack $ sTopicName) TopicAttrs {replicationFactor = 3})

  mp <- mkProducer producerConfig
  mc <- mkConsumer consumerConfig [sTopicName]

  async . forever $ do
    threadDelay 1000000
    MockMessage {..} <- mkMockData
    send
      mp
      RawProducerRecord
      { rprTopic = hTopicName,
        rprKey = mmKey,
        --rprValue = encode $
        --  HM.fromList [ ("humidity" :: Text, (HM.!) ((fromJust . decode) mmValue :: Object) "humidity") ],
        rprValue = mmValue,
        rprTimestamp = mmTimestamp
      }
    send
      mp
      RawProducerRecord
      { rprTopic = tTopicName,
        rprKey = mmKey,
        --rprValue = encode $
        --  HM.fromList [ ("temperature" :: Text, (HM.!) ((fromJust . decode) mmValue :: Object) "temperature") ],
        rprValue = mmValue,
        rprTimestamp = mmTimestamp
      }

  _ <- async $
    forever $ do
      records <- pollRecords mc 100 1000
      forM_ records $ \RawConsumerRecord {..} ->
        P.putStr "detect abnormal data: " >> BL.putStrLn rcrValue

  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    let taskConfig =
          TaskConfig
            { tcMessageStoreType = LogDevice producerConfig consumerConfig,
              tcLogFunc = lf
            }
    runTask taskConfig task

--------------------------------------------------------------------------------
mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 3)) :: IO Int
  t <- getStdRandom (randomR (0, 100)) :: IO Int
  h <- getStdRandom (randomR (0, 100)) :: IO Int
  let r = HM.fromList [ ("temperature" :: Text, Number (scientific (toInteger t) 0))
                      , ("humidity"    :: Text, Number (scientific (toInteger h) 0)) ]
  P.putStrLn $ "gen data: " ++ show r
  ts <- getCurrentTimestamp
  return
    MockMessage
      { mmTimestamp = ts,
        -- WARNING: A Nothing key in a task with JOIN can raise an exception
        mmKey = Just $ encode $ HM.fromList [ ("key" :: Text, Number (scientific (toInteger k) 0))],     -- TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode r
      }
