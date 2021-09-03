module HStream.StatsSpec (spec) where

import           Control.Concurrent
import qualified Data.Map.Strict    as Map
import           Data.Maybe         (fromJust)
import           Test.Hspec

import           HStream.Stats
import           HStream.Utils      (runConc, setupSigsegvHandler)

spec :: Spec
spec = do
  runIO setupSigsegvHandler

  statsSpec
  threadedStatsSpec

statsSpec :: Spec
statsSpec = describe "HStream.Stats" $ do
  it "pre stream stats counter" $ do
    h <- newStatsHolder
    stream_stat_add_append_payload_bytes h "/topic_1" 100
    stream_stat_add_append_payload_bytes h "/topic_1" 100
    stream_stat_add_append_payload_bytes h "/topic_2" 100

    s <- newAggregateStats h
    stream_stat_get_append_payload_bytes s "/topic_1" `shouldReturn` 200
    stream_stat_get_append_payload_bytes s "/topic_2" `shouldReturn` 100

  it "pre stream stats time series" $ do
    h <- newStatsHolder
    stream_time_series_add_append_in_bytes h "/topic_1" 1000
    -- NOTE: we choose to sleep 1sec so that we can assume the speed won't be
    -- faster than 2000B/s
    threadDelay 1000000
    stream_time_series_add_append_in_bytes h "/topic_1" 1000
    m <- stream_time_series_getall_by_name h "appends" [5* 1000, 10 * 1000]  -- 5, 10 sec
    Map.lookup "/topic_1" m `shouldSatisfy` ((\s -> head s > 0 && head s <= 2000) . fromJust)
    Map.lookup "/topic_1" m `shouldSatisfy` ((\s -> s!!1 > 0 && s!!1 <= 2000) . fromJust)

threadedStatsSpec :: Spec
threadedStatsSpec = describe "HStream.Stats (threaded)" $ do
  it "pre stream stats counter (threaded)" $ do
    h <- newStatsHolder
    runConc 10 $ runConc 1000 $ do
      stream_stat_add_append_payload_bytes h "a_stream" 1
      stream_stat_add_append_payload_bytes h "b_stream" 1

    s <- newAggregateStats h
    stream_stat_get_append_payload_bytes s "a_stream" `shouldReturn` 10000
    stream_stat_get_append_payload_bytes s "b_stream" `shouldReturn` 10000

    m <- stream_stat_getall_append_payload_bytes s
    Map.lookup "a_stream" m `shouldBe` Just 10000
    Map.lookup "b_stream" m `shouldBe` Just 10000

  it "pre stream stats time series (threaded)" $ do
    h <- newStatsHolder
    runConc 10 $ runConc 1000 $ do
      stream_time_series_add_append_in_bytes h "a_stream" 1000
      stream_time_series_add_append_in_bytes h "b_stream" 1000

    m <- stream_time_series_getall_by_name h "appends" [10 * 1000]  -- 10 sec
    Map.lookup "non-existed-stream-name" m `shouldBe` Nothing
    -- FIXME: Unfortunately, there is no easy way to test with real speed. So we just
    -- check the speed is positive.
    Map.lookup "a_stream" m `shouldSatisfy` ((\s -> head s > 0) . fromJust)
