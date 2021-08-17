{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

import           Data.Aeson
import           Data.Maybe
import qualified Data.Text.Lazy                        as TL
import qualified Data.Text.Lazy.Encoding               as TLE
import           HStream.Processing.Connector
import           HStream.Processing.Encoding
import           HStream.Processing.MockStreamStore
import           HStream.Processing.Processor
import           HStream.Processing.Store
import qualified HStream.Processing.Stream             as HS
import           HStream.Processing.Stream.JoinWindows
import           HStream.Processing.Type
import           HStream.Processing.Util
import qualified Prelude                               as P
import           RIO
import qualified RIO.ByteString.Lazy                   as BL
import           System.Random

data R = R
  { temperature :: Int,
    humidity :: Int
  }
  deriving (Generic, Show, Typeable)

instance ToJSON R

instance FromJSON R

data R1 = R1
  { r1Temperature :: Int
  }
  deriving (Generic, Show, Typeable)

instance ToJSON R1

instance FromJSON R1

data R2 = R2
  { r2Humidity :: Int
  }
  deriving (Generic, Show, Typeable)

instance ToJSON R2

instance FromJSON R2

main :: IO ()
main = do
  mockStore <- mkMockStreamStore
  sourceConnector1 <- mkMockStoreSourceConnector mockStore
  sourceConnector2 <- mkMockStoreSourceConnector mockStore
  sinkConnector <- mkMockStoreSinkConnector mockStore

  let textSerde =
        Serde
          { serializer = Serializer TLE.encodeUtf8,
            deserializer = Deserializer TLE.decodeUtf8
          } ::
          Serde TL.Text BL.ByteString
  let rSerde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R BL.ByteString
  let r1Serde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R1 BL.ByteString
  let r2Serde =
        Serde
          { serializer = Serializer encode,
            deserializer = Deserializer $ fromJust . decode
          } ::
          Serde R2 BL.ByteString
  let tStreamName = "temperature-source"
  let hStreamName = "humidity-source"
  let sStreamName = "demo-sink"
  let streamSourceConfig1 =
        HS.StreamSourceConfig
          { sscStreamName = tStreamName,
            sscKeySerde = textSerde,
            sscValueSerde = r1Serde
          }
  let streamSourceConfig2 =
        HS.StreamSourceConfig
          { sscStreamName = hStreamName,
            sscKeySerde = textSerde,
            sscValueSerde = r2Serde
          }
  let streamSinkConfig =
        HS.StreamSinkConfig
          { sicStreamName = sStreamName,
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

  _ <- async $
    forever $
      do
        threadDelay 1000000
        MockMessage {..} <- mkMockData
        writeRecord
          sinkConnector
          SinkRecord
            { snkStream = hStreamName,
              snkKey = mmKey,
              snkValue = encode $ R2 {r2Humidity = humidity ((fromJust . decode) mmValue :: R)},
              snkTimestamp = mmTimestamp
            }
        writeRecord
          sinkConnector
          SinkRecord
            { snkStream = tStreamName,
              snkKey = mmKey,
              snkValue = encode $ R1 {r1Temperature = temperature ((fromJust . decode) mmValue :: R)},
              snkTimestamp = mmTimestamp
            }

  _ <- async $
    forever $
      do
        subscribeToStream sourceConnector1 sStreamName Earlist
        records <- readRecords sourceConnector1
        forM_ records $ \SourceRecord {..} ->
          P.putStr "detect abnormal data: " >> BL.putStrLn srcValue

  runTask sourceConnector2 sinkConnector (HS.build streamBuilder)

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
