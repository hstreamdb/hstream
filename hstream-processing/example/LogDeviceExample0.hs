{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

import           Data.Aeson
import           Data.Maybe
import qualified Data.Text.Lazy               as TL
import qualified Data.Text.Lazy.Encoding      as TLE
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Topic
import           HStream.Processing.Util
import           HStream.Store                (AdminClientConfig (..),
                                               HsLogAttrs (..), LogAttrs (..),
                                               createTopic_, mkAdminClient,
                                               pattern C_DBG_ERROR,
                                               setLogDeviceDbgLevel)
import qualified Prelude                      as P
import           RIO
import qualified RIO.ByteString.Lazy          as BL
import qualified RIO.Map                      as Map
import           System.Random

data R
  = R
      { temperature :: Int,
        humidity    :: Int
      }
  deriving (Generic, Show, Typeable)

instance ToJSON R

instance FromJSON R

main :: IO ()
main = do
  P.putStrLn "enter logdevice-example0"
  let sourceConfig =
        SourceConfig
          { sourceName = "source",
            sourceTopicName = "demo-source",
            keyDeserializer = voidDeserializer,
            valueDeserializer = Deserializer (\s -> (fromJust $ decode s) :: R)
          }
  let sinkConfig =
        SinkConfig
          { sinkName = "sink",
            sinkTopicName = "demo-sink",
            keySerializer = voidSerializer,
            valueSerializer = Serializer (encode :: R -> BL.ByteString)
          }
  let task =
        build $
          buildTask "demo"
            <> addSource sourceConfig
            <> addProcessor
              "filter"
              (filterProcessor filterR)
              ["source"]
            <> addSink sinkConfig ["filter"]
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
  createTopic_ adminClient "demo-source" (LogAttrs $ HsLogAttrs 3 Map.empty)
  createTopic_ adminClient "demo-sink" (LogAttrs $ HsLogAttrs 3 Map.empty)
  mp <- mkProducer producerConfig
  mc <- mkConsumer consumerConfig ["demo-sink"]
  _ <- async
    $ forever
    $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = "demo-source",
            rprKey = mmKey,
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }
  _ <- async
    $ forever
    $ do
      records <- pollRecords mc 1 100
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

filterR :: Record Void R -> Bool
filterR Record {..} =
  temperature recordValue >= 50
    && humidity recordValue >= 0

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 10)) :: IO Int
  t <- getStdRandom (randomR (0, 100))
  h <- getStdRandom (randomR (0, 100))
  let r = R {temperature = t, humidity = h}
  P.putStrLn $ "gen data: " ++ show r
  ts <- getCurrentTimestamp
  return
    MockMessage
      { mmTimestamp = ts,
        mmKey = Just $ TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode $ R {temperature = t, humidity = h}
      }

filterProcessor :: (Typeable k, Typeable v) => (Record k v -> Bool) -> Processor k v
filterProcessor f = Processor $ \r ->
  when (f r) $ forward r
