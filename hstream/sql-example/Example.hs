{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Data.Aeson
import qualified Data.HashMap.Strict          as HM
import           Data.Scientific
import           Data.Text.IO                 (getLine)
import           HStream.Processing.Processor (MessageStoreType (Mock),
                                               MockMessage (..),
                                               TaskConfig (..),
                                               mkMockTopicConsumer,
                                               mkMockTopicProducer,
                                               mkMockTopicStore, runTask)
import           HStream.Processing.Topic     (RawConsumerRecord (..),
                                               RawProducerRecord (..),
                                               TopicConsumer (..),
                                               TopicProducer (..))
import           HStream.Processing.Util      (getCurrentTimestamp)
import           HStream.SQL.Codegen          (ExecutionPlan (..),
                                               streamCodegen)
import qualified Prelude                      as P
import           RIO
import qualified RIO.ByteString.Lazy          as BL
import           System.Random                (Random (randomR), getStdRandom)

---------------------------------- Example -------------------------------------
-- CREATE STREAM demoSink AS SELECT * FROM temperatureSource WITH (FORMAT = JSON);

-- CREATE STREAM demoSink AS SELECT SUM(humiditySource.humidity) AS result FROM humiditySource INNER JOIN temperatureSource WITHIN (INTERVAL 5 SECOND) ON (humiditySource.temperature = temperatureSource.temperature) WHERE humiditySource.humidity > 20 GROUP BY humiditySource.humidity, TUMBLING (INTERVAL 10 SECOND) WITH (FORMAT = "JSON");
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

  let tTopicName = "temperatureSource"
  let hTopicName = "humiditySource"

  mockStore <- mkMockTopicStore
  mp  <- mkMockTopicProducer mockStore
  mc' <- mkMockTopicConsumer mockStore

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

  mc <- subscribe mc' [sTopicName]
  _ <- async $
    forever $ do
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
