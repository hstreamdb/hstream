{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

import           Data.Aeson
import           Data.Maybe
import qualified Data.Text.Lazy                     as TL
import qualified Data.Text.Lazy.Encoding            as TLE
import           HStream.Processing.Connector
import           HStream.Processing.Encoding
import           HStream.Processing.MockStreamStore
import           HStream.Processing.Processor
import           HStream.Processing.Type
import           HStream.Processing.Util
import qualified Prelude                            as P
import           RIO
import qualified RIO.ByteString.Lazy                as BL
import           System.Random

data R = R
  { temperature :: Int,
    humidity :: Int
  }
  deriving (Generic, Show, Typeable)

instance ToJSON R

instance FromJSON R

main :: IO ()
main = do
  mockStore <- mkMockStreamStore
  sourceConnector1 <- mkMockStoreSourceConnector mockStore
  sourceConnector2 <- mkMockStoreSourceConnector mockStore
  sinkConnector <- mkMockStoreSinkConnector mockStore

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
          buildTask "demo"
            <> addSource sourceConfig
            <> addProcessor
              "filter"
              (filterProcessor filterR)
              ["source"]
            <> addSink sinkConfig ["filter"]
  _ <- async $
    forever $
      do
        threadDelay 1000000
        MockMessage {..} <- mkMockData
        writeRecord
          sinkConnector
          SinkRecord
            { snkStream = "demo-source",
              snkKey = mmKey,
              snkValue = mmValue,
              snkTimestamp = mmTimestamp
            }
  _ <- async $
    forever $
      do
        subscribeToStream sourceConnector1 "demo-sink" Earlist
        records <- readRecords sourceConnector1
        forM_ records $ \SourceRecord {..} ->
          P.putStr "detect abnormal data: " >> BL.putStrLn srcValue

  runTask sourceConnector2 sinkConnector task

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
