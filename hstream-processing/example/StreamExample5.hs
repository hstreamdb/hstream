{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

import           Data.Aeson
import           Data.Maybe
import qualified Data.Text.Lazy               as TL
import qualified Data.Text.Lazy.Encoding      as TLE
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import qualified HStream.Processing.Stream    as HS
import           HStream.Processing.Topic
import           HStream.Processing.Util
import qualified Prelude                      as P
import           RIO
import qualified RIO.ByteString.Lazy          as BL
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
      { location :: TL.Text
      }
  deriving (Generic, Show, Typeable)

instance ToJSON R1

instance FromJSON R1

data R2
  = R2
      { r2Location :: TL.Text,
        r2Temperature :: Int,
        r2Humidity :: Int
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
  let streamTopicName = "stream-source"
  let tableTopicName = "table-source"
  let sinkTopicName = "demo-sink"
  let streamSourceConfig1 =
        HS.StreamSourceConfig
          { sscTopicName = streamTopicName,
            sscKeySerde = textSerde,
            sscValueSerde = rSerde
          }
  let streamSourceConfig2 =
        HS.StreamSourceConfig
          { sscTopicName = tableTopicName,
            sscKeySerde = textSerde,
            sscValueSerde = r1Serde
          }
  let streamSinkConfig =
        HS.StreamSinkConfig
          { sicTopicName = sinkTopicName,
            sicKeySerde = textSerde,
            sicValueSerde = r2Serde
          }
  table <-
    HS.mkStreamBuilder ""
      >>= HS.table streamSourceConfig2
  streamBuilder <-
    HS.mkStreamBuilder "demo"
      >>= HS.stream streamSourceConfig1
      >>= HS.joinTable table joiner textSerde r1Serde
      >>= HS.to streamSinkConfig
  mockStore <- mkMockTopicStore
  mp <- mkMockTopicProducer mockStore
  mc' <- mkMockTopicConsumer mockStore
  forM_
    ([1 .. 3] :: [Int])
    ( \i ->
        send
          mp
          RawProducerRecord
            { rprTopic = tableTopicName,
              rprKey = Just $ TLE.encodeUtf8 $ TL.pack $ show i,
              rprValue = encode $ R1 {location = TL.append "location-" $ TL.pack (show i)},
              rprTimestamp = -1
            }
    )
  _ <- async
    $ forever
    $ do
      threadDelay 1000000
      MockMessage {..} <- mkMockData
      send
        mp
        RawProducerRecord
          { rprTopic = streamTopicName,
            rprKey = mmKey,
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }
  mc <- subscribe mc' [sinkTopicName]
  _ <- async
    $ forever
    $ do
      records <- pollRecords mc 1000000
      forM_ records $ \RawConsumerRecord {..} ->
        P.putStr "joined data: " >> BL.putStrLn rcrValue
  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    let taskConfig =
          TaskConfig
            { tcMessageStoreType = Mock mockStore,
              tcLogFunc = lf
            }
    runTask taskConfig (HS.build streamBuilder)

joiner :: R -> R1 -> R2
joiner R {..} R1 {..} =
  R2
    { r2Temperature = temperature,
      r2Humidity = humidity,
      r2Location = location
    }

mkMockData :: IO MockMessage
mkMockData = do
  k <- getStdRandom (randomR (1, 3)) :: IO Int
  t <- getStdRandom (randomR (0, 100))
  h <- getStdRandom (randomR (0, 100))
  let r = R {temperature = t, humidity = h}
  P.putStrLn $ "gen data: " ++ " key: " ++ show k ++ ", value: " ++ show r
  ts <- getCurrentTimestamp
  return
    MockMessage
      { mmTimestamp = ts,
        mmKey = Just $ TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode $ R {temperature = t, humidity = h}
      }
