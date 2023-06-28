module HStream.StatsSpec (spec) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.Bits              (shiftL)
import           Data.Either
import           Data.Int
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromJust)
import           GHC.IO                 (catchException)
import           System.Random
import           Test.Hspec
import           Z.Data.CBytes          (CBytes)

import           HStream.Base           (runConc, setupFatalSignalHandler)
import           HStream.Stats
import           HStream.StatsSpecUtils (mkTimeSeriesTest)

{-# ANN module ("HLint: ignore Use head" :: String) #-}

spec :: Spec
spec = do
  runIO setupFatalSignalHandler

  statsSpec
  threadedStatsSpec

  miscSpec

statsSpec :: Spec
statsSpec = describe "HStream.Stats" $ do
  it "per stream stats counter" $ do
    h <- newStatsHolder True
    stream_stat_add_append_in_bytes h "topic_1" 100
    stream_stat_add_append_in_bytes h "topic_1" 100
    stream_stat_add_append_in_bytes h "topic_2" 100

    stream_stat_add_append_total h "/topic_1" 1
    stream_stat_add_append_total h "/topic_1" 1
    stream_stat_add_append_total h "/topic_2" 1

    s <- newAggregateStats h
    stream_stat_get_append_in_bytes s "topic_1" `shouldReturn` 200
    stream_stat_get_append_in_bytes s "topic_2" `shouldReturn` 100

    m <- stream_stat_getall_append_in_bytes s
    m' <- stream_stat_getall h "append_in_bytes"

    Map.lookup "topic_1" m `shouldBe` Map.lookup "topic_1" m'
    Map.lookup "topic_1" m `shouldBe` Just 200
    Map.lookup "topic_2" m `shouldBe` Map.lookup "topic_2" m'
    Map.lookup "topic_2" m `shouldBe` Just 100

    stream_stat_get_append_total s "/topic_1" `shouldReturn` 2
    stream_stat_get_append_total s "/topic_2" `shouldReturn` 1

  setSpec "per subscription set stats"
          subscription_stat_set_checklist_size
          subscription_stat_get_checklist_size
          subscription_stat_getall_checklist_size

  eraseSpec "per stream stats"
            stream_stat_add_append_in_bytes
            stream_stat_get_append_in_bytes
            stream_stat_getall_append_in_bytes
            stream_stat_erase

  eraseSpec "per subscription stats"
            subscription_stat_add_send_out_bytes
            subscription_stat_get_send_out_bytes
            subscription_stat_getall_send_out_bytes
            subscription_stat_erase

  eraseSpec "per connector stats"
            connector_stat_add_delivered_in_records
            connector_stat_get_delivered_in_records
            connector_stat_getall_delivered_in_records
            connector_stat_erase

  eraseSpec "per query stats"
            query_stat_add_total_input_records
            query_stat_get_total_input_records
            query_stat_getall_total_input_records
            query_stat_erase

  eraseSpec "per view stats"
            view_stat_add_total_execute_queries
            view_stat_get_total_execute_queries
            view_stat_getall_total_execute_queries
            view_stat_erase

  it "per stream stats time series" $ do
    h <- newStatsHolder True
    let intervals = [5 * 1000, 10 * 1000] -- 5, 10 sec
    let mkTest name stats_add = mkTimeSeriesTest h intervals name stats_add stream_time_series_get stream_time_series_getall

    mkTest "appends" stream_time_series_add_append_in_bytes

  it "per subscription stats counter" $ do
    h <- newStatsHolder True
    subscription_stat_add_resend_records h "subid_1" 1
    subscription_stat_add_resend_records h "subid_1" 2
    subscription_stat_add_resend_records h "subid_2" 1

    s <- newAggregateStats h
    subscription_stat_get_resend_records s "subid_1" `shouldReturn` 3
    subscription_stat_get_resend_records s "subid_2" `shouldReturn` 1

    m <- subscription_stat_getall_resend_records s
    Map.lookup "subid_1" m `shouldBe` Just 3
    Map.lookup "subid_2" m `shouldBe` Just 1

  it "per subscription stats time series" $ do
    h <- newStatsHolder True
    let intervals = [5 * 1000, 10 * 1000] -- 5, 10 sec
    let mkTest name stats_add = mkTimeSeriesTest h intervals name stats_add subscription_time_series_get subscription_time_series_getall

    mkTest "send_out_bytes" subscription_time_series_add_send_out_bytes
    mkTest "acks" subscription_time_series_add_acks
    mkTest "request_messages" subscription_time_series_add_request_messages
    mkTest "response_messages" subscription_time_series_add_response_messages

  it "per handle stats time series" $ do
    h <- newStatsHolder True
    let intervals = [5 * 1000, 10 * 1000] -- 5, 10 sec
    let mkTest name stats_add = mkTimeSeriesTest h intervals name stats_add handle_time_series_get handle_time_series_getall

    mkTest "queries" handle_time_series_add_queries_in

  it "ServerHistogram" $ do
    h <- newStatsHolder True

    serverHistogramEstimatePercentile h SHL_AppendRequestLatency 0
      `shouldReturn` 0

    serverHistogramAdd h SHL_AppendRequestLatency 85
    p50 <- serverHistogramEstimatePercentile h SHL_AppendRequestLatency 0.5
    [p50_] <- serverHistogramEstimatePercentiles h SHL_AppendRequestLatency [0.5]
    p50 `shouldBe` p50_
    p50 `shouldSatisfy` (\p -> p >= 1`shiftL`6{- 64 -} && p <= 1`shiftL`7 {- 128 -})

    serverHistogramAdd h SHL_AppendRequestLatency (5`shiftL`30 {- ~5000s -})
    p99 <- serverHistogramEstimatePercentile h SHL_AppendRequestLatency 0.99
    [p99_] <- serverHistogramEstimatePercentiles h SHL_AppendRequestLatency [0.99]
    p99 `shouldBe` p99_
    p99 `shouldSatisfy` (\p -> p >= 1`shiftL`32 && p <= 1`shiftL`33)

    serverHistogramAdd h SHL_AppendRequestLatency (1`shiftL`50 {- ~5 years -})
    p99' <- serverHistogramEstimatePercentile h SHL_AppendRequestLatency 0.99
    p99' `shouldSatisfy` (\p -> p >= 1`shiftL`50 && p <= 1`shiftL`51)

    pMin <- serverHistogramEstimatePercentile h SHL_AppendRequestLatency 0
    pMin `shouldSatisfy` (\p -> p >= 1`shiftL`6{- 64 -} && p <= 1`shiftL`7 {- 128 -})
    pMax <- serverHistogramEstimatePercentile h SHL_AppendRequestLatency 1
    pMax `shouldSatisfy` (\p -> p >= 1`shiftL`50 && p <= 1`shiftL`51)

threadedStatsSpec :: Spec
threadedStatsSpec = describe "HStream.Stats (threaded)" $ do
  h <- runIO $ newStatsHolder True

  it "per stream stats counter (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      stream_stat_add_append_in_bytes h "a_stream" 1
      stream_stat_add_append_in_bytes h "b_stream" 1

      stream_stat_add_append_total h "a_stream" 1
      stream_stat_add_append_total h "b_stream" 1

    s <- newAggregateStats h
    stream_stat_get_append_in_bytes s "a_stream" `shouldReturn` 10000
    stream_stat_get_append_in_bytes s "b_stream" `shouldReturn` 10000

    stream_stat_get_append_total s "a_stream" `shouldReturn` 10000
    stream_stat_get_append_total s "b_stream" `shouldReturn` 10000

    m <- stream_stat_getall_append_in_bytes s
    Map.lookup "a_stream" m `shouldBe` Just 10000
    Map.lookup "b_stream" m `shouldBe` Just 10000

    m' <- stream_stat_getall_append_total s
    Map.lookup "a_stream" m' `shouldBe` Just 10000
    Map.lookup "b_stream" m' `shouldBe` Just 10000

  it "per stream stats time series (threaded)" $ do
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

  it "per subscription stats counter (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      subscription_stat_add_resend_records h "a_stream" 1
      subscription_stat_add_resend_records h "b_stream" 1

    s <- newAggregateStats h
    subscription_stat_get_resend_records s "a_stream" `shouldReturn` 10000
    subscription_stat_get_resend_records s "b_stream" `shouldReturn` 10000

    m <- subscription_stat_getall_resend_records s
    Map.lookup "a_stream" m `shouldBe` Just 10000
    Map.lookup "b_stream" m `shouldBe` Just 10000

  it "per subscription stats time series (threaded)" $ do
    runConc 10 $ runConc 1000 $ do
      subscription_time_series_add_send_out_bytes h "a_stream" 1000
      subscription_time_series_add_send_out_bytes h "b_stream" 1000

    let max_intervals = [60 * 1000]   -- 1min
    Right m <- subscription_time_series_getall h "sends" max_intervals
    Map.lookup "non-existed-stream-name" m `shouldBe` Nothing
    subscription_time_series_get h "sends" "non-existed-stream-name" max_intervals
      `shouldReturn` Nothing

    -- FIXME: Unfortunately, there is no easy way to test with real speed. So we just
    -- check the speed is positive.
    Map.lookup "a_stream" m `shouldSatisfy` ((\s -> head s > 0) . fromJust)
    Just rate <- subscription_time_series_get h "sends" "a_stream" max_intervals
    rate `shouldSatisfy` (\s -> head s > 0)

miscSpec :: Spec
miscSpec = describe "HStream.Stats (misc)" $ do

  it "get empty stream_time_series should return empty map" $ do
    h <- newServerStatsHolder
    stream_time_series_getall h "appends" [1]
      `shouldReturn` Right Map.empty

  it ("intervals should not larger than MaxInterval defined in .inc file"
   <> "(NOTE: not all functions are checking this)") $ do
    h <- newServerStatsHolder

    let tooBig = [24 * 60 * 60 * 1000] -- 24h
    stream_time_series_add_append_in_bytes h "stream" 1000
    m <- stream_time_series_getall h "appends" tooBig
    m `shouldSatisfy` isLeft

-------------------------------------------------------------------------------

eraseSpec
  :: String
  -> (StatsHolder -> CBytes -> Int64 -> IO ())
  -> (Stats -> CBytes -> IO Int64)
  -> (Stats -> IO (Map.Map CBytes Int64))
  -> (StatsHolder -> CBytes -> IO ())
  -> Spec
eraseSpec label stat_add stat_get stat_getall stat_erase = do
  it (label <> ": erase 1") $ do
    h <- newStatsHolder True
    stat_add h "topic_1" 100
    stat_add h "topic_1" 100
    stat_add h "topic_2" 100
    s <- newAggregateStats h
    stat_get s "topic_1" `shouldReturn` 200
    stat_get s "topic_2" `shouldReturn` 100
    stat_erase h "topic_1"            -- delete "topic_1"
    s <- newAggregateStats h
    stat_get s "topic_1" `shouldReturn` (-1)
    stat_get s "topic_2" `shouldReturn` 100
    stat_add h "topic_1" 100
    s <- newAggregateStats h
    stat_get s "topic_1" `shouldReturn` 100

  it (label <> ": erase 2") $ do
    h <- newStatsHolder True
    stat_add h "topic_1" 100
    stat_add h "topic_1" 100
    stat_add h "topic_2" 100
    s <- newAggregateStats h
    m <- stat_getall s
    Map.lookup "topic_1" m `shouldBe` Just 200
    Map.lookup "topic_2" m `shouldBe` Just 100
    stat_erase h "topic_2"     -- delete "topic_2"
    s <- newAggregateStats h
    m' <- stat_getall s
    Map.lookup "topic_1" m' `shouldBe` (Just 200)
    Map.lookup "topic_2" m' `shouldBe` Nothing

-------------------------------------------------------------------------------

setSpec
  :: String
  -> (StatsHolder -> CBytes -> Int64 -> IO ())
  -> (Stats -> CBytes -> IO Int64)
  -> (Stats -> IO (Map.Map CBytes Int64))
  -> Spec
setSpec label stat_set stat_get stat_getall = do
  it (label <> ": basic set") $ do
    h <- newStatsHolder True
    stat_set h "topic_1" 1000
    stat_set h "topic_1" 2000
    stat_set h "topic_2" 1000
    s <- newAggregateStats h
    stat_get s "topic_1" `shouldReturn` 2000
    stat_get s "topic_2" `shouldReturn` 1000
    m <- stat_getall s
    Map.lookup "topic_1" m `shouldBe` Just 2000
    Map.lookup "topic_2" m `shouldBe` Just 1000

  it "Run set stats in an unbounded thread should be OK" $ do
    h <- newStatsHolder True
    let action =
          do x <- randomRIO (0, 2000)
             stat_set h "topic_1" x
             s <- newAggregateStats h
             stat_get s "topic_1" `shouldReturn` x
    -- NOTE: here we use a new thread created by forkIO to make "more unbounded"
    -- (do not use 'Control.Concurrent.runInUnboundThread' directly)
    replicateM_ 1000 $ do
      mv <- newEmptyMVar
      mask $ \restore -> do
        tid <- forkIO $ try (restore action) >>= putMVar mv
        let wait = takeMVar mv `catchException` \(e :: SomeException) ->
                     throwTo tid e >> wait
        wait >>= (either (throwIO @SomeException) return)
