module HStream.StatsSpec (spec) where

import           Test.Hspec

import           HStream.Stats
import           HStream.Utils (runConc)

spec :: Spec
spec = do
  statsSpec
  threadedStatsSpec

statsSpec :: Spec
statsSpec = describe "HStream.Stats" $ do
  it "pre stream stats counter" $ do
    s <- newStats
    streamStatAdd_append_payload_bytes s "/topic_1" 100
    streamStatAdd_append_payload_bytes s "/topic_1" 100
    streamStatAdd_append_payload_bytes s "/topic_2" 100

    streamStatGet_append_payload_bytes s "/topic_1" `shouldReturn` 200
    streamStatGet_append_payload_bytes s "/topic_2" `shouldReturn` 100

  it "pre stream stats time series" $ do
    s <- newStats
    streamTimeSeriesAdd_append_in_bytes s "/topic_1" 1000
    streamTimeSeriesAdd_append_in_bytes s "/topic_1" 1000
    streamTimeSeriesFlush_append_in_bytes s "/topic_1"
    streamTimeSeriesGetRate_append_in_bytes s "/topic_1" 0 `shouldReturn` 2000

threadedStatsSpec :: Spec
threadedStatsSpec = describe "HStream.Stats (threaded)" $ do
  it "pre stream stats counter (threaded)" $ do
    holder <- newStatsHolder
    runConc 10 $ runConc 1000 $ do
      streamStatHolderAdd_append_payload_bytes holder "a_stream" 1
      streamStatHolderAdd_append_payload_bytes holder "b_stream" 1

    streamStatGetAll_append_payload_bytes holder "a_stream" `shouldReturn` 10000
    streamStatGetAll_append_payload_bytes holder "b_stream" `shouldReturn` 10000
