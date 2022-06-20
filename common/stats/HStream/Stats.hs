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
  , newServerStatsHolder
  , newAggregateStats
  , printStatsHolder
  , resetStatsHolder

    -- * PerStreamStats
    -- ** Counters
  , stream_stat_add_append_payload_bytes
  , stream_stat_add_append_total
  , stream_stat_add_append_failed
  , stream_stat_add_record_payload_bytes
  , stream_stat_get_append_payload_bytes
  , stream_stat_get_append_total
  , stream_stat_get_append_failed
  , stream_stat_get_record_payload_bytes
  , stream_stat_getall
  , stream_stat_getall_append_payload_bytes
  , stream_stat_getall_append_total
  , stream_stat_getall_append_failed
  , stream_stat_getall_record_payload_bytes
    -- ** Time series
  , stream_time_series_add_append_in_bytes
  , stream_time_series_add_append_in_records
  , stream_time_series_add_append_in_requests
  , stream_time_series_add_append_failed_requests
  , stream_time_series_add_record_bytes
  , stream_time_series_get
  , stream_time_series_getall_by_name
  , stream_time_series_getall_by_name'

    -- * PerSubscriptionStats
    -- ** Counters
  , subscription_stat_add_resend_records
  , subscription_stat_get_resend_records
  , subscription_stat_getall
  , subscription_stat_getall_resend_records
    -- ** Time series
  , subscription_time_series_add_send_out_bytes
  , subscription_time_series_add_send_out_records
  , subscription_time_series_add_acks
  , subscription_time_series_add_request_messages
  , subscription_time_series_add_response_messages
  , subscription_time_series_get
  , subscription_time_series_getall_by_name
  , subscription_time_series_getall_by_name'

    -- * PerHandleStats
    -- ** Time series
  , handle_time_series_add_queries_in
  , handle_time_series_get
  , handle_time_series_getall

    -- * ServerHistogram
  , ServerHistogramLabel (..)
  , serverHistogramAdd
  , serverHistogramEstimatePercentiles
  , serverHistogramEstimatePercentile
  ) where

import           Control.Monad            (forM_, when)
import           Control.Monad.ST         (RealWorld)
import           Data.Int
import qualified Data.Map.Strict          as Map
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimArray
import           Foreign.ForeignPtr
import           Foreign.Ptr
import qualified Text.Read                as Read
import           Z.Data.CBytes            (CBytes, withCBytesUnsafe)
import qualified Z.Data.CBytes            as CBytes

import           HStream.Foreign
import qualified HStream.Logger           as Log
import qualified HStream.Stats.Internal   as I

-------------------------------------------------------------------------------

newtype Stats = Stats (ForeignPtr I.CStats)
newtype StatsHolder = StatsHolder (ForeignPtr I.CStatsHolder)

newStatsHolder :: Bool -> IO StatsHolder
newStatsHolder isServer = StatsHolder <$>
  (newForeignPtr I.c_delete_stats_holder_fun =<< I.c_new_stats_holder isServer)

newServerStatsHolder :: IO StatsHolder
newServerStatsHolder = newStatsHolder True

newAggregateStats :: StatsHolder -> IO Stats
newAggregateStats (StatsHolder holder) = withForeignPtr holder $ \holder' ->
  Stats <$> (newForeignPtr I.c_delete_stats_fun =<< I.c_new_aggregate_stats holder')

-- TODO: add Show instance for StatsHolder
printStatsHolder :: StatsHolder -> IO ()
printStatsHolder (StatsHolder holder) = withForeignPtr holder I.c_stats_holder_print

resetStatsHolder :: StatsHolder -> IO ()
resetStatsHolder (StatsHolder holder) = withForeignPtr holder I.stats_holder_reset

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

#define PER_X_STAT_GETALL_SEP(PREFIX, STATS_NAME)                              \
PREFIX##getall_##STATS_NAME :: Stats -> IO (Map.Map CBytes Int64);             \
PREFIX##getall_##STATS_NAME (Stats stats) =                                    \
  withForeignPtr stats $ \stats' -> snd <$>                                    \
    peekCppMap                                                                 \
      (I.PREFIX##getall_##STATS_NAME stats')                                   \
      peekStdStringToCBytesN c_delete_vector_of_string                         \
      peekN c_delete_vector_of_int64;

#define PER_X_STAT_GETALL(PREFIX)                                              \
PREFIX##getall :: StatsHolder -> CBytes -> IO (Map.Map CBytes Int64);          \
PREFIX##getall (StatsHolder stats_holder) stat_name =                          \
  withForeignPtr stats_holder $ \stats_holder' ->                              \
  withCBytesUnsafe stat_name $ \stat_name' -> do                               \
    (ret, statMap) <-                                                          \
      peekCppMap                                                               \
        (I.PREFIX##getall stats_holder' (BA# stat_name'))                      \
        peekStdStringToCBytesN c_delete_vector_of_string                       \
        peekN c_delete_vector_of_int64 ;                                       \
    if ret == 0 then pure statMap                                              \
                else do Log.fatal "PREFIX##getall failed!";                    \
                        pure Map.empty

PER_X_STAT_GETALL(stream_stat_)
PER_X_STAT_GETALL(subscription_stat_)

#define STAT_DEFINE(name, _)                                                   \
PER_X_STAT_ADD(stream_stat_, name)                                             \
PER_X_STAT_GET(stream_stat_, name)                                             \
PER_X_STAT_GETALL_SEP(stream_stat_, name)
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

-- TODO: make intervals checking by default
--
-- | the same as 'stream_time_series_getall_by_name', but check intervals first.
stream_time_series_getall_by_name'
  :: StatsHolder
  -> CBytes
  -> [Int]
  -> IO (Either String (Map.Map CBytes [Double]))
stream_time_series_getall_by_name' (StatsHolder holder) name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe name $ \name' -> do
    let interval_len = length intervals
    -- NOTE only for unsafe ffi
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    (errmsg, pass) <- withStdStringUnsafe $ I.per_stream_verify_intervals
                                            holder' (BA# name')
                                            interval_len (BA# intervals')
    if pass then Right <$> get holder' name' interval_len intervals'
            else pure $ Left (CBytes.unpack errmsg)
  where
    get holder' name' interval_len intervals' = do
      (ret, statMap) <-
        peekCppMap
          (cfun holder' (BA# name') interval_len (BA# intervals'))
          peekStdStringToCBytesN c_delete_vector_of_string
          peekFollySmallVectorDoubleN c_delete_std_vec_of_folly_small_vec_of_double
      if ret == 0 then pure statMap
                  else do Log.fatal "stream_time_series_getall failed!"
                          pure Map.empty
    cfun = I.c_stream_time_series_getall_by_name

#define STAT_DEFINE(name, _)                                                   \
PER_X_STAT_ADD(subscription_stat_, name)                                       \
PER_X_STAT_GET(subscription_stat_, name)                                       \
PER_X_STAT_GETALL_SEP(subscription_stat_, name)
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

subscription_time_series_getall_by_name'
  :: StatsHolder
  -> CBytes
  -> [Int]
  -> IO (Either String (Map.Map CBytes [Double]))
subscription_time_series_getall_by_name' (StatsHolder holder) name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe name $ \name' -> do
    let interval_len = length intervals
    -- NOTE only for unsafe ffi
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    (errmsg, pass) <- withStdStringUnsafe $ I.per_subscription_verify_intervals
                                            holder' (BA# name')
                                            interval_len (BA# intervals')
    if pass then Right <$> get holder' name' interval_len intervals'
            else pure $ Left (CBytes.unpack errmsg)
  where
    get holder' name' interval_len intervals' = do
      (ret, statMap) <-
        peekCppMap
          (cfun holder' (BA# name') interval_len (BA# intervals'))
          peekStdStringToCBytesN c_delete_vector_of_string
          peekFollySmallVectorDoubleN c_delete_std_vec_of_folly_small_vec_of_double
      if ret == 0 then pure statMap
                  else do Log.fatal "subscription_time_series_getall' failed!"
                          pure Map.empty
    cfun = I.subscription_time_series_getall_by_name

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
PER_X_STAT_ADD(handle_time_series_, name)
#include "../include/per_handle_time_series.inc"

handle_time_series_get
  :: StatsHolder -> CBytes -> CBytes -> [Int] -> IO (Maybe [Double])
handle_time_series_get (StatsHolder holder) method_name key_name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe method_name $ \method_name' ->
  withCBytesUnsafe key_name $ \key_name' -> do
    let interval_len = length intervals
    (mpa@(MutablePrimArray mba#) :: MutablePrimArray RealWorld Double) <- newPrimArray interval_len
    forM_ [0..interval_len] $ \i -> writePrimArray mpa i 0
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    !ret <- I.handle_time_series_get
              holder' (BA# method_name') (BA# key_name')
              interval_len (BA# intervals') (MBA# mba#)
    !pa <- unsafeFreezePrimArray mpa
    return $ if ret == 0 then Just (primArrayToList pa) else Nothing

handle_time_series_getall
  :: StatsHolder
  -> CBytes
  -> [Int]
  -> IO (Either String (Map.Map CBytes [Double]))
handle_time_series_getall (StatsHolder holder) name intervals =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe name $ \name' -> do
    let interval_len = length intervals
    -- NOTE only for unsafe ffi
    let !(ByteArray intervals') = byteArrayFromListN interval_len intervals
    (errmsg, pass) <- withStdStringUnsafe $ I.per_handle_verify_intervals
                                            holder' (BA# name')
                                            interval_len (BA# intervals')
    if pass then Right <$> get holder' name' interval_len intervals'
            else pure $ Left (CBytes.unpack errmsg)
  where
    get holder' name' interval_len intervals' = do
      (ret, statMap) <-
        peekCppMap
          (cfun holder' (BA# name') interval_len (BA# intervals'))
          peekStdStringToCBytesN c_delete_vector_of_string
          peekFollySmallVectorDoubleN c_delete_std_vec_of_folly_small_vec_of_double
      if ret == 0 then pure statMap
                  else do Log.fatal "handle_time_series_getall failed!"
                          pure Map.empty
    cfun = I.handle_time_series_getall_by_name

#undef PER_X_STAT_ADD
#undef PER_X_STAT_GET
#undef PER_X_STAT_GETALL_SEP

-------------------------------------------------------------------------------

-- TODO: auto generate from "cbits/stats/ServerHistogram.h"
data ServerHistogramLabel
  = SHL_AppendRequestLatency
  | SHL_AppendLatency

packServerHistogramLabel :: ServerHistogramLabel -> CBytes
packServerHistogramLabel SHL_AppendRequestLatency = "append_request_latency"
packServerHistogramLabel SHL_AppendLatency        = "append_latency"

instance Read ServerHistogramLabel where
  readPrec = do
    l <- Read.lexP
    return $
      case l of
        Read.Ident "append_request_latency" -> SHL_AppendRequestLatency
        Read.Ident "append_latency" -> SHL_AppendLatency
        x -> errorWithoutStackTrace $ "cannot parse ServerHistogramLabel: " <> show x

serverHistogramAdd :: StatsHolder -> ServerHistogramLabel -> Int64 -> IO ()
serverHistogramAdd (StatsHolder holder) label val =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe (packServerHistogramLabel label) $ \label' -> do
    ret <- I.server_histogram_add holder' (BA# label') val
    when (ret /= 0) $ error "serverHistogramAdd failed!"

-- NOTE: Input percentiles must be sorted and in valid range [0.0, 1.0].
serverHistogramEstimatePercentiles
  :: StatsHolder -> ServerHistogramLabel -> [Double] -> IO [Int64]
serverHistogramEstimatePercentiles (StatsHolder holder) label ps =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe (packServerHistogramLabel label) $ \label' -> do
    let len = length ps
    (mpa@(MutablePrimArray mba#) :: MutablePrimArray RealWorld Int64) <- newPrimArray len
    forM_ [0..len] $ \i -> writePrimArray mpa i 0
    let !(ByteArray ps') = byteArrayFromListN len ps
    !ret <- I.server_histogram_estimatePercentiles
                holder' (BA# label') (BA# ps') len
                (MBA# mba#) nullPtr nullPtr
    if ret == 0 then do !pa <- unsafeFreezePrimArray mpa
                        pure $ primArrayToList pa
                else error "get serverHistogramEstimatePercentiles failed!"

serverHistogramEstimatePercentile
  :: StatsHolder -> ServerHistogramLabel -> Double -> IO Int64
serverHistogramEstimatePercentile (StatsHolder holder) label p =
  withForeignPtr holder $ \holder' ->
  withCBytesUnsafe (packServerHistogramLabel label) $ \label' -> do
    I.server_histogram_estimatePercentile holder' (BA# label') p
