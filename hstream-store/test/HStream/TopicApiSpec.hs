
module HStream.TopicApiSpec (spec) where

import           HStream.Store
import           HStream.TopicApi


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
    (do consu
    ) `shouldReturn` True

path = "/data/store/logdevice.conf"

ctopic = do
  client <- mkAdminClient $ AdminClientConfig path
  createTopics client [Topic "a/a/a", Topic "a/a/b"] 3

pubs = do
  pd <- mkProducer $ ProducerConfig path
  t <- getCurrentTime
  sendMessageBatch pd $
    [ProducerRecord
       (Topic "a/a/a")
       (Just "aa")
       "aaaaaa"
       t
     ]

consu = do
  cs <- mkConsumer (ConsumerConfig path "start") [Topic "a/a/a"]
  pub1
  v <- pollMessages cs 1 1000
  commitOffsets cs
  c <- readCommit cs (Topic "a/a/a")
  return True

