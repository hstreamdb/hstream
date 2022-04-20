{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP          #-}
{-# LANGUAGE MagicHash    #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp -Werror=unused-top-binds #-}

-- To dump CPP output, do
--
-- > cabal exec -- ghc -E common/stats/HStream/Stats.hs
--
-- And then you can @cat common/stats/HStream/Stats.hspp@

module HStream.Stats
  ( -- * StatsHolder
    Stats
  , StatsHolder
  , newStatsHolder
  , newAggregateStats
  , printStatsHolder

    -- * PerStreamStats
  , stream_stat_add_append_payload_bytes
  , stream_stat_add_append_total
  , stream_stat_add_append_failed
  , stream_stat_add_record_payload_bytes
  , stream_stat_get_append_payload_bytes
  , stream_stat_get_append_total
  , stream_stat_get_append_failed
  , stream_stat_get_record_payload_bytes
  , stream_stat_getall_append_payload_bytes
  , stream_stat_getall_append_total
  , stream_stat_getall_append_failed
  , stream_stat_getall_record_payload_bytes
    -- ** Time series
  , stream_time_series_add_append_in_bytes
  , stream_time_series_add_append_in_records
  , stream_time_series_add_append_in_requests
  , stream_time_series_add_record_bytes
  , stream_time_series_get
  , stream_time_series_getall_by_name

    -- * PerSubscriptionStats
  , subscription_stat_add_consumers
  , subscription_stat_get_consumers
  , subscription_stat_getall_consumers
    -- ** Time series
  , subscription_time_series_add_send_out_bytes
  , subscription_time_series_add_send_out_records
  , subscription_time_series_get
  , subscription_time_series_getall_by_name
  ) where

import           Control.Monad            (forM_)
import           Control.Monad.ST         (RealWorld)
import           Data.Int
import qualified Data.Map.Strict          as Map
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimArray
import           Foreign.ForeignPtr
import           Z.Data.CBytes            (CBytes, withCBytesUnsafe)

import           HStream.Foreign
import qualified HStream.Logger           as Log
import qualified HStream.Stats.Internal   as I

-------------------------------------------------------------------------------

newtype Stats = Stats (ForeignPtr I.CStats)
newtype StatsHolder = StatsHolder (ForeignPtr I.CStatsHolder)

newStatsHolder :: IO StatsHolder
newStatsHolder = StatsHolder <$>
  (newForeignPtr I.c_delete_stats_holder_fun =<< I.c_new_stats_holder)

newAggregateStats :: StatsHolder -> IO Stats
newAggregateStats (StatsHolder holder) = withForeignPtr holder $ \holder' ->
  Stats <$> (newForeignPtr I.c_delete_stats_fun =<< I.c_new_aggregate_stats holder')

-- TODO: add Show instance for StatsHolder
printStatsHolder :: StatsHolder -> IO ()
printStatsHolder (StatsHolder holder) = withForeignPtr holder I.c_stats_holder_print

#define PER_X_STAT_ADD(PREFIX, STATS_NAME)                                     \
PREFIX##add_##STATS_NAME :: StatsHolder -> CBytes -> Int64 -> IO ();           \
PREFIX##add_##STATS_NAME (StatsHolder holder) key val =                        \
  withForeignPtr holder $ \holder' ->                                          \
  withCBytesUnsafe key $ \key' ->                                              \
    I.PREFIX##add_##STATS_NAME holder' (BA# key') val;

-- TODO: Error while return value is a negative number.
#define PER_X_STAT_GET(PREFIX, STATS_NAME)                                     \
PREFIX##get_##STATS_NAME :: Stats -> CBytes -> IO Int64;                       \
PREFIX##get_##STATS_NAME (Stats stats) key =                                   \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe key $ \key' ->                                              \
    I.PREFIX##get_##STATS_NAME stats' (BA# key');

#define PER_X_STAT_GETALL(PREFIX, STATS_NAME)                                  \
PREFIX##getall_##STATS_NAME :: Stats -> IO (Map.Map CBytes Int64);             \
PREFIX##getall_##STATS_NAME (Stats stats) =                                    \
  withForeignPtr stats $ \stats' -> snd <$>                                    \
    peekCppMap                                                                 \
      (I.PREFIX##getall_##STATS_NAME stats')                                   \
      peekStdStringToCBytesN c_delete_vector_of_string                         \
      peekN c_delete_vector_of_int64;

#define STAT_DEFINE(name, _)                                                   \
PER_X_STAT_ADD(stream_stat_, name)                                             \
PER_X_STAT_GET(stream_stat_, name)                                             \
PER_X_STAT_GETALL(stream_stat_, name)
#include "../include/per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
PER_X_STAT_ADD(stream_time_series_, name)
#include "../include/per_stream_time_series.inc"

stream_time_series_get
  :: StatsHolder -> CBytes -> CBytes -> [Int] -> IO (Maybe [Double])
stream_time_series_get (StatsHolder holder) method_name stream_name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe method_name $ \method_name' ->
  withCBytesUnsafe stream_name $ \stream_name' -> do
    let interval_len = length intervals
    (mpa@(MutablePrimArray mba#) :: MutablePrimArray RealWorld Double) <- newPrimArray interval_len
    forM_ [0..interval_len] $ \i -> writePrimArray mpa i 0
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    !ret <- I.c_stream_time_series_get
              holder' (BA# method_name') (BA# stream_name')
              interval_len (BA# intervals') (MBA# mba#)
    !pa <- unsafeFreezePrimArray mpa
    return $ if ret == 0 then Just (primArrayToList pa) else Nothing

stream_time_series_getall_by_name
  :: StatsHolder -> CBytes -> [Int] -> IO (Map.Map CBytes [Double])
stream_time_series_getall_by_name (StatsHolder holder) name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe name $ \name' -> do
    let interval_len = length intervals
    -- NOTE only for unsafe ffi
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    (ret, statMap) <-
      peekCppMap
        (I.c_stream_time_series_getall_by_name holder' (BA# name') interval_len (BA# intervals'))
        peekStdStringToCBytesN c_delete_vector_of_string
        peekFollySmallVectorDoubleN c_delete_std_vec_of_folly_small_vec_of_double
    if ret == 0 then pure statMap
                else do Log.fatal "stream_time_series_getall failed!"
                        pure Map.empty

#define STAT_DEFINE(name, _)                                                   \
PER_X_STAT_ADD(subscription_stat_, name)                                       \
PER_X_STAT_GET(subscription_stat_, name)                                       \
PER_X_STAT_GETALL(subscription_stat_, name)
#include "../include/per_subscription_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
PER_X_STAT_ADD(subscription_time_series_, name)
#include "../include/per_subscription_time_series.inc"

subscription_time_series_get
  :: StatsHolder -> CBytes -> CBytes -> [Int] -> IO (Maybe [Double])
subscription_time_series_get (StatsHolder holder) method_name stream_name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe method_name $ \method_name' ->
  withCBytesUnsafe stream_name $ \stream_name' -> do
    let interval_len = length intervals
    (mpa@(MutablePrimArray mba#) :: MutablePrimArray RealWorld Double) <- newPrimArray interval_len
    forM_ [0..interval_len] $ \i -> writePrimArray mpa i 0
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    !ret <- I.subscription_time_series_get
              holder' (BA# method_name') (BA# stream_name')
              interval_len (BA# intervals') (MBA# mba#)
    !pa <- unsafeFreezePrimArray mpa
    return $ if ret == 0 then Just (primArrayToList pa) else Nothing

subscription_time_series_getall_by_name
  :: StatsHolder -> CBytes -> [Int] -> IO (Map.Map CBytes [Double])
subscription_time_series_getall_by_name (StatsHolder holder) name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe name $ \name' -> do
    let interval_len = length intervals
    -- NOTE only for unsafe ffi
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    (ret, statMap) <-
      peekCppMap
        (I.subscription_time_series_getall_by_name holder' (BA# name') interval_len (BA# intervals'))
        peekStdStringToCBytesN c_delete_vector_of_string
        peekFollySmallVectorDoubleN c_delete_std_vec_of_folly_small_vec_of_double
    if ret == 0 then pure statMap
                else do Log.fatal "subscription_time_series_getall failed!"
                        pure Map.empty

#undef PER_X_STAT_ADD
#undef PER_X_STAT_GET
#undef PER_X_STAT_GETALL
