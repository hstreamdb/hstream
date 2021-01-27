{-# LANGUAGE OverloadedStrings #-}

module HStream.TopicApiSpec (spec) where

import           Data.Int              (Int64)
import           Data.Time
import           Data.Time.Clock.POSIX
import           HStream.TopicApi
import           Test.Hspec

spec :: Spec
spec = describe "HStream.TopicApi" $ do
  it "create topic" $
    (do ctopic
        return True
    ) `shouldReturn` True

  it "pub message" $
    (do pubs
        return True
    ) `shouldReturn` True

  it "consum" $
    consu
    `shouldReturn` True
 where
  path = "/data/store/logdevice.conf"

  ctopic = do
    client <- mkAdminClient $ AdminClientConfig path
    createTopics client [ "a/a/a",  "a/a/b"] 3

  pubs = do
    pd <- mkProducer $ ProducerConfig path
    t <- getCurrentTimestamp'
    sendMessageBatch pd $
      [ProducerRecord
         ("a/a/a")
         (Just "aa")
         "aaaaaa"
         t
       ]

  consu = do
    cs <- mkConsumer (ConsumerConfig path "/tmp/start") [ "a/a/a"]
    pubs
    _ <- pollMessages cs 1 1000
    commitOffsets cs
    _ <- readCommit cs ("a/a/a")
    return True


  posixTimeToMilliSeconds' :: POSIXTime -> Timestamp
  posixTimeToMilliSeconds' =
    floor . (* 1000) . nominalDiffTimeToSeconds

  -- return millisecond timestamp
  getCurrentTimestamp' :: IO Timestamp
  getCurrentTimestamp' = posixTimeToMilliSeconds' <$> getPOSIXTime


type Timestamp = Int64
