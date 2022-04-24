module HStream.StatsSpecUtils where

import           Control.Concurrent (threadDelay)
import           Data.Int           (Int64)
import qualified Data.Map.Strict    as Map
import           Data.Maybe         (fromJust)
import           Test.Hspec
import           Z.Data.CBytes      (CBytes)

import           HStream.Stats      (StatsHolder, subscription_time_series_get,
                                     subscription_time_series_getall_by_name)

{-# ANN module ("HLint: ignore Use head" :: String) #-}

mkSubTimeSeriesSpec :: StatsHolder -> CBytes
  -> (StatsHolder -> CBytes -> Int64 -> IO ())
  -> Expectation
mkSubTimeSeriesSpec h stats_name stats_add = do
    let intervals = [5 * 1000, 10 * 1000] -- 5, 10 sec

    stats_add h "topic_1" 1000
    stats_add h "topic_2" 10000
    threadDelay 1000000
    stats_add h "topic_1" 1000
    stats_add h "topic_2" 10000

    subscription_time_series_get h stats_name "non-existed-stream-name" intervals
      `shouldReturn` Nothing

    Just [rate1_p5s, rate1_p10s] <- subscription_time_series_get h stats_name "topic_1" intervals
    rate1_p5s `shouldSatisfy` (\s -> s > 0 && s <= 2000)
    rate1_p10s `shouldSatisfy` (\s -> s > 0 && s <= 2000)
    Just [rate2_p5s, rate2_p10s] <- subscription_time_series_get h stats_name "topic_2" intervals
    rate2_p5s `shouldSatisfy` (\s -> s > 2000 && s <= 20000)
    rate2_p10s `shouldSatisfy` (\s -> s > 2000 && s <= 20000)

    m <- subscription_time_series_getall_by_name h stats_name intervals
    Map.lookup "topic_1" m `shouldSatisfy` ((\s -> s!!0 > 0 && s!!0 <= 2000) . fromJust)
    Map.lookup "topic_2" m `shouldSatisfy` ((\s -> s!!1 > 2000 && s!!1 <= 20000) . fromJust)
