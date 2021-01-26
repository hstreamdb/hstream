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
      { r1Temperature :: Int
      }
  deriving (Generic, Show, Typeable)

instance ToJSON R1

instance FromJSON R1

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
  let streamSourceConfig =
        HS.StreamSourceConfig
          { sscTopicName = "demo-source",
            sscKeySerde = textSerde,
            sscValueSerde = rSerde
          }
  let streamSinkConfig =
        HS.StreamSinkConfig
          { sicTopicName = "demo-sink",
            sicKeySerde = textSerde,
            sicValueSerde = r1Serde
          }
  streamBuilder <-
    HS.mkStreamBuilder "demo"
      >>= HS.stream streamSourceConfig
      >>= HS.filter filterR
      >>= HS.map mapR
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
          { rprTopic = "demo-source",
            rprKey = mmKey,
            rprValue = mmValue,
            rprTimestamp = mmTimestamp
          }
  mc <- subscribe mc' ["demo-sink"]
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

filterR :: Record TL.Text R -> Bool
filterR Record {..} =
  temperature recordValue >= 50
    && humidity recordValue >= 0

mapR :: Record TL.Text R -> Record TL.Text R1
mapR r@Record {..} =
  r
    { recordValue =
        R1
          { r1Temperature = temperature recordValue
          }
    }

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
