{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Data.Aeson
import qualified Data.HashMap.Strict     as HM
import           Data.Scientific
import           Data.Text.IO            (getLine)
import qualified Data.Text.Lazy          as TL
import qualified Data.Text.Lazy.Encoding as TLE
import           HStream.Processor       (MessageStoreType (Mock),
                                          MockMessage (..), TaskConfig (..),
                                          mkMockTopicConsumer,
                                          mkMockTopicProducer, mkMockTopicStore,
                                          runTask)
import           HStream.Topic           (RawConsumerRecord (..),
                                          RawProducerRecord (..),
                                          TopicConsumer (..),
                                          TopicProducer (..))
import           HStream.Util            (getCurrentTimestamp)
import           Language.SQL            (streamCodegen)
import qualified Prelude                 as P
import           RIO
import qualified RIO.ByteString.Lazy     as BL
import           System.Random           (Random (randomR), getStdRandom)

main :: IO ()
main = getLine >>= run

run :: Text -> IO ()
run input = do
  task <- streamCodegen input

  let tTopicName = "temperatureSource"
  let hTopicName = "humiditySource"
  let sTopicName = "demoSink"

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
        mmKey = Just $ TLE.encodeUtf8 $ TL.pack $ show k,
        mmValue = encode r
      }
