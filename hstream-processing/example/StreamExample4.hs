{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

import           Data.Aeson
import           Data.Maybe
import qualified Data.Text.Lazy                        as TL
import qualified Data.Text.Lazy.Encoding               as TLE
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Store
import qualified HStream.Processing.Stream             as HS
import           HStream.Processing.Stream.JoinWindows
import           HStream.Processing.Topic
import           HStream.Processing.Util
import qualified Prelude                               as P
import           RIO
import qualified RIO.ByteString.Lazy                   as BL
import           System.Random

data R
  = R
      { temperature :: Int,
        humidity :: Int
      }
  deriving (Generic, Show, Typeable)

instance ToJSON R

instance FromJSON R

data R1
  = R1
      { r1Temperature :: Int
      }
  deriving (Generic, Show, Typeable)

instance ToJSON R1

instance FromJSON R1

data R2
  = R2
      { r2Humidity :: Int
      }
  deriving (Generic, Show, Typeable)

instance ToJSON R2

instance FromJSON R2

main :: IO ()
main = do
  let textSerde =
        Serde
          { serializer = Serializer TLE.encodeUtf8,
            deserializer = Deserializer TLE.decodeUtf8
          } ::
          Serde TL.Text
  let rSerde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R
  let r1Serde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R1
  let r2Serde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R2
  let tTopicName = "temperature-source"
  let hTopicName = "humidity-source"
  let sTopicName = "demo-sink"
  let streamSourceConfig1 =
        HS.StreamSourceConfig
          { sscTopicName = tTopicName,
            sscKeySerde = textSerde,
            sscValueSerde = r1Serde
          }
  let streamSourceConfig2 =
        HS.StreamSourceConfig
          { sscTopicName = hTopicName,
            sscKeySerde = textSerde,
            sscValueSerde = r2Serde
          }
  let streamSinkConfig =
        HS.StreamSinkConfig
          { sicTopicName = sTopicName,
            sicKeySerde = textSerde,
            sicValueSerde = rSerde
          }
  stream2 <-
    HS.mkStreamBuilder ""
      >>= HS.stream streamSourceConfig2
  let joinWindows =
        JoinWindows
          { jwBeforeMs = 1000,
            jwAfterMs = 1000,
            jwGraceMs = 3600 * 1000
          }
  store1 <- mkInMemoryStateTimestampedKVStore
  store2 <- mkInMemoryStateTimestampedKVStore
  let streamJoined =
        HS.StreamJoined
          { sjK1Serde = textSerde,
            sjV1Serde = r1Serde,
            sjK2Serde = textSerde,
            sjV2Serde = r2Serde,
            sjThisStore = store1,
            sjOtherStore = store2
          }
  streamBuilder <-
    HS.mkStreamBuilder "demo"
      >>= HS.stream streamSourceConfig1
      >>= HS.joinStream stream2 joiner keySelector1 keySelector2 joinWindows streamJoined
      >>= HS.filter filterR
      >>= HS.to streamSinkConfig
  mockStore <- mkMockTopicStore
  mp <- mkMockTopicProducer mockStore
  mc' <- mkMockTopicConsumer mockStore
  _ <- async
    $ forever
    $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = hTopicName,
            rprKey = mmKey,
            rprValue = encode $ R2 {r2Humidity = humidity ((fromJust . decode) mmValue :: R)},
            rprTimestamp = mmTimestamp
          }
      send
        mp
        RawProducerRecord
          { rprTopic = tTopicName,
            rprKey = mmKey,
            rprValue = encode $ R1 {r1Temperature = temperature ((fromJust . decode) mmValue :: R)},
            rprTimestamp = mmTimestamp
          }
  mc <- subscribe mc' [sTopicName]
  _ <- async
    $ forever
    $ do
      records <- pollRecords mc 1000000
      forM_ records $ \RawConsumerRecord {..} ->
        P.putStr "detect abnormal data: " >> BL.putStrLn rcrValue
  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    let taskConfig =
          TaskConfig
            { tcMessageStoreType = Mock mockStore,
              tcLogFunc = lf
            }
    runTask taskConfig (HS.build streamBuilder)

joiner :: R1 -> R2 -> R
joiner R1 {..} R2 {..} =
  R
    { temperature = r1Temperature,
      humidity = r2Humidity
    }

keySelector1 :: Record TL.Text R1 -> TL.Text
keySelector1 = fromJust . recordKey

keySelector2 :: Record TL.Text R2 -> TL.Text
keySelector2 = fromJust . recordKey

filterR :: Record TL.Text R -> Bool
filterR Record {..} =
  temperature recordValue >= 50
    && humidity recordValue >= 0

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 3)) :: IO Int
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
