module HStream.StatsSpec (spec) where

import           Control.Concurrent
import qualified Data.Map.Strict    as Map
import           Data.Maybe         (fromJust)
import           Test.Hspec

import           HStream.Stats
import           HStream.Utils      (runConc, setupSigsegvHandler)

{-# ANN module ("HLint: ignore Use head" :: String) #-}

spec :: Spec
spec = do
  runIO setupSigsegvHandler

  statsSpec
  threadedStatsSpec

statsSpec :: Spec
statsSpec = describe "HStream.Stats" $ do
  it "pre stream stats counter" $ do
    h <- newStatsHolder
    stream_stat_add_append_payload_bytes h "topic_1" 100
    stream_stat_add_append_payload_bytes h "topic_1" 100
    stream_stat_add_append_payload_bytes h "topic_2" 100

    stream_stat_add_append_total h "/topic_1" 1
    stream_stat_add_append_total h "/topic_1" 1
    stream_stat_add_append_total h "/topic_2" 1

    s <- newAggregateStats h
    stream_stat_get_append_payload_bytes s "topic_1" `shouldReturn` 200
    stream_stat_get_append_payload_bytes s "topic_2" `shouldReturn` 100

    m <- stream_stat_getall_append_payload_bytes s
    Map.lookup "topic_1" m `shouldBe` Just 200
    Map.lookup "topic_2" m `shouldBe` Just 100

    stream_stat_get_append_total s "/topic_1" `shouldReturn` 2
    stream_stat_get_append_total s "/topic_2" `shouldReturn` 1

  it "pre stream stats time series" $ do
    h <- newStatsHolder
    let intervals = [5 * 1000, 10 * 1000] -- 5, 10 sec
    stream_time_series_add_append_in_bytes h "/topic_1" 1000
    stream_time_series_add_append_in_bytes h "/topic_2" 10000
    -- NOTE: we choose to sleep 1sec so that we can assume the speed of topic_1
    -- won't be faster than 2000B/s
    threadDelay 1000000
    stream_time_series_add_append_in_bytes h "/topic_1" 1000
    stream_time_series_add_append_in_bytes h "/topic_2" 10000

    stream_time_series_get h "appends" "non-existed-stream-name" intervals
      `shouldReturn` Nothing

    Just [rate1_p5s, rate1_p10s] <- stream_time_series_get h "appends" "/topic_1" intervals
    rate1_p5s `shouldSatisfy` (\s -> s > 0 && s <= 2000)
    rate1_p10s `shouldSatisfy` (\s -> s > 0 && s <= 2000)
    Just [rate2_p5s, rate2_p10s] <- stream_time_series_get h "appends" "/topic_2" intervals
    rate2_p5s `shouldSatisfy` (\s -> s > 2000 && s <= 20000)
    rate2_p10s `shouldSatisfy` (\s -> s > 2000 && s <= 20000)

    m <- stream_time_series_getall_by_name h "appends" intervals
    Map.lookup "/topic_1" m `shouldSatisfy` ((\s -> s!!0 > 0 && s!!0 <= 2000) . fromJust)
    Map.lookup "/topic_2" m `shouldSatisfy` ((\s -> s!!1 > 2000 && s!!1 <= 20000) . fromJust)

  it "pre subscription stats counter" $ do
    h <- newStatsHolder
    subscription_stat_add_consumers h "subid_1" 1
    subscription_stat_add_consumers h "subid_1" 2
    subscription_stat_add_consumers h "subid_2" 1

    s <- newAggregateStats h
    subscription_stat_get_consumers s "subid_1" `shouldReturn` 3
    subscription_stat_get_consumers s "subid_2" `shouldReturn` 1

    m <- subscription_stat_getall_consumers s
    Map.lookup "subid_1" m `shouldBe` Just 3
    Map.lookup "subid_2" m `shouldBe` Just 1

  it "pre subscription stats time series" $ do
    h <- newStatsHolder
    let intervals = [5 * 1000, 10 * 1000] -- 5, 10 sec

    subscription_time_series_add_send_out_bytes h "topic_1" 1000
    subscription_time_series_add_send_out_bytes h "topic_2" 10000
    threadDelay 1000000
    subscription_time_series_add_send_out_bytes h "topic_1" 1000
    subscription_time_series_add_send_out_bytes h "topic_2" 10000

    subscription_time_series_get h "send_out_bytes" "non-existed-stream-name" intervals
      `shouldReturn` Nothing

    Just [rate1_p5s, rate1_p10s] <- subscription_time_series_get h "send_out_bytes" "topic_1" intervals
    rate1_p5s `shouldSatisfy` (\s -> s > 0 && s <= 2000)
    rate1_p10s `shouldSatisfy` (\s -> s > 0 && s <= 2000)
    Just [rate2_p5s, rate2_p10s] <- subscription_time_series_get h "send_out_bytes" "topic_2" intervals
    rate2_p5s `shouldSatisfy` (\s -> s > 2000 && s <= 20000)
    rate2_p10s `shouldSatisfy` (\s -> s > 2000 && s <= 20000)

    m <- subscription_time_series_getall_by_name h "send_out_bytes" intervals
    Map.lookup "topic_1" m `shouldSatisfy` ((\s -> s!!0 > 0 && s!!0 <= 2000) . fromJust)
    Map.lookup "topic_2" m `shouldSatisfy` ((\s -> s!!1 > 2000 && s!!1 <= 20000) . fromJust)

threadedStatsSpec :: Spec
threadedStatsSpec = describe "HStream.Stats (threaded)" $ do
  h <- runIO newStatsHolder

  it "pre stream stats counter (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      stream_stat_add_append_payload_bytes h "a_stream" 1
      stream_stat_add_append_payload_bytes h "b_stream" 1

      stream_stat_add_append_total h "a_stream" 1
      stream_stat_add_append_total h "b_stream" 1

    s <- newAggregateStats h
    stream_stat_get_append_payload_bytes s "a_stream" `shouldReturn` 10000
    stream_stat_get_append_payload_bytes s "b_stream" `shouldReturn` 10000

    stream_stat_get_append_total s "a_stream" `shouldReturn` 10000
    stream_stat_get_append_total s "b_stream" `shouldReturn` 10000

    m <- stream_stat_getall_append_payload_bytes s
    Map.lookup "a_stream" m `shouldBe` Just 10000
    Map.lookup "b_stream" m `shouldBe` Just 10000

    m' <- stream_stat_getall_append_total s
    Map.lookup "a_stream" m' `shouldBe` Just 10000
    Map.lookup "b_stream" m' `shouldBe` Just 10000

  it "pre stream stats time series (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      stream_time_series_add_append_in_bytes h "a_stream" 1000
      stream_time_series_add_append_in_bytes h "b_stream" 1000

    let max_intervals = [60 * 1000]   -- 1min
    m <- stream_time_series_getall_by_name h "appends" max_intervals
    Map.lookup "non-existed-stream-name" m `shouldBe` Nothing
    stream_time_series_get h "appends" "non-existed-stream-name" max_intervals
      `shouldReturn` Nothing

    -- FIXME: Unfortunately, there is no easy way to test with real speed. So we just
    -- check the speed is positive.
    Map.lookup "a_stream" m `shouldSatisfy` ((\s -> head s > 0) . fromJust)
    Just rate <- stream_time_series_get h "appends" "a_stream" max_intervals
    rate `shouldSatisfy` (\s -> head s > 0)

  it "pre subscription stats counter (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      subscription_stat_add_consumers h "a_stream" 1
      subscription_stat_add_consumers h "b_stream" 1

    s <- newAggregateStats h
    subscription_stat_get_consumers s "a_stream" `shouldReturn` 10000
    subscription_stat_get_consumers s "b_stream" `shouldReturn` 10000

    m <- subscription_stat_getall_consumers s
    Map.lookup "a_stream" m `shouldBe` Just 10000
    Map.lookup "b_stream" m `shouldBe` Just 10000

  it "pre subscription stats time series (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      subscription_time_series_add_send_out_bytes h "a_stream" 1000
      subscription_time_series_add_send_out_bytes h "b_stream" 1000

    let max_intervals = [60 * 1000]   -- 1min
    m <- subscription_time_series_getall_by_name h "sends" max_intervals
    Map.lookup "non-existed-stream-name" m `shouldBe` Nothing
    subscription_time_series_get h "sends" "non-existed-stream-name" max_intervals
      `shouldReturn` Nothing

    -- FIXME: Unfortunately, there is no easy way to test with real speed. So we just
    -- check the speed is positive.
    Map.lookup "a_stream" m `shouldSatisfy` ((\s -> head s > 0) . fromJust)
    Just rate <- subscription_time_series_get h "sends" "a_stream" max_intervals
    rate `shouldSatisfy` (\s -> head s > 0)
