{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Stats where

import           Data.Int
import           Data.Primitive     (ByteArray#)
import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Z.Data.CBytes      (CBytes, withCBytesUnsafe)

-------------------------------------------------------------------------------
-- Exposed functions

type Stats = ForeignPtr CStats
type StatsHolder = ForeignPtr CStatsHolder

newStats :: IO Stats
newStats = newForeignPtr c_delete_stats_fun =<< c_new_stats

newStatsHolder :: IO StatsHolder
newStatsHolder = newForeignPtr c_delete_stats_holder_fun =<< c_new_stats_holder

-- TODO: add Show instance for StatsHolder
printStatsHolder :: StatsHolder -> IO ()
printStatsHolder holder = withForeignPtr holder c_stats_holder_print

#define STAT_DEFINE(name, _)                                                   \
streamStatAdd_##name :: Stats -> CBytes -> Int64 -> IO ();                     \
streamStatAdd_##name stats stream_name val =                                   \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_stat_add_##name nullPtr stats' stream_name' val
#include "../include/per_stream_stats.inc"

#define STAT_DEFINE(name, _)                                                   \
streamStatHolderAdd_##name :: StatsHolder -> CBytes -> Int64 -> IO ();         \
streamStatHolderAdd_##name stats stream_name val =                             \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_stat_add_##name stats' nullPtr stream_name' val
#include "../include/per_stream_stats.inc"

#define STAT_DEFINE(name, _)                                                   \
streamStatGet_##name :: Stats -> CBytes -> IO Int64;                           \
streamStatGet_##name stats stream_name =                                       \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_stat_get_##name nullPtr stats' stream_name'
#include "../include/per_stream_stats.inc"

#define STAT_DEFINE(name, _)                                                   \
streamStatGetAll_##name :: StatsHolder -> CBytes -> IO Int64;                  \
streamStatGetAll_##name stats stream_name =                                    \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_stat_getall_##name stats' stream_name'
#include "../include/per_stream_stats.inc"

#define STAT_DEFINE(name, _)                                                   \
streamStatHolderGet_##name :: StatsHolder -> CBytes -> IO Int64;               \
streamStatHolderGet_##name stats stream_name =                                 \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_stat_get_##name stats' nullPtr stream_name'
#include "../include/per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
streamTimeSeriesAdd_##name :: Stats -> CBytes -> Int64 -> IO ();               \
streamTimeSeriesAdd_##name stats stream_name val =                             \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_time_series_add_##name stats' stream_name' val
#include "../include/per_stream_time_series.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
streamTimeSeriesGetRate_##name :: Stats -> CBytes -> CSize -> IO Double;       \
streamTimeSeriesGetRate_##name stats stream_name level =                       \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_time_series_get_rate_##name stats' stream_name' level
#include "../include/per_stream_time_series.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
streamTimeSeriesFlush_##name :: Stats -> CBytes -> IO ();                      \
streamTimeSeriesFlush_##name stats stream_name =                               \
  withForeignPtr stats $ \stats' ->                                            \
  withCBytesUnsafe stream_name $ \stream_name' ->                              \
    c_stream_time_series_flush_##name stats' stream_name'
#include "../include/per_stream_time_series.inc"

-------------------------------------------------------------------------------
-- Considering as internal functions

data CStats
data CStatsHolder

foreign import ccall unsafe "hs_common.h new_stats"
  c_new_stats :: IO (Ptr CStats)

foreign import ccall unsafe "hs_common.h &delete_stats"
  c_delete_stats_fun :: FunPtr (Ptr CStats -> IO ())

foreign import ccall unsafe "hs_common.h new_stats_holder"
  c_new_stats_holder :: IO (Ptr CStatsHolder)

foreign import ccall unsafe "hs_common.h &delete_stats_holder"
  c_delete_stats_holder_fun :: FunPtr (Ptr CStatsHolder -> IO ())

foreign import ccall unsafe "hs_common.h stats_holder_print"
  c_stats_holder_print :: Ptr CStatsHolder -> IO ()

#define STAT_DEFINE(name, _)                                                   \
foreign import ccall unsafe "hs_common.h stream_stat_add_##name"               \
  c_stream_stat_add_##name                                                     \
    :: Ptr CStatsHolder -> Ptr CStats -> ByteArray# -> Int64 -> IO ();         \
foreign import ccall unsafe "hs_common.h stream_stat_get_##name"               \
  c_stream_stat_get_##name                                                     \
    :: Ptr CStatsHolder -> Ptr CStats -> ByteArray# -> IO Int64;               \
foreign import ccall unsafe "hs_common.h stream_stat_getall_##name"            \
  c_stream_stat_getall_##name                                                  \
    :: Ptr CStatsHolder -> ByteArray# -> IO Int64;
#include "../include/per_stream_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
foreign import ccall unsafe "hs_common.h stream_time_series_add_##name"        \
  c_stream_time_series_add_##name                                              \
    :: Ptr CStats -> ByteArray# -> Int64 -> IO ();                             \
foreign import ccall unsafe "hs_common.h stream_time_series_get_rate_##name"   \
  c_stream_time_series_get_rate_##name                                         \
    :: Ptr CStats -> ByteArray# -> CSize -> IO Double;                         \
foreign import ccall unsafe "hs_common.h stream_time_series_flush_##name"      \
  c_stream_time_series_flush_##name :: Ptr CStats -> ByteArray# -> IO ();
#include "../include/per_stream_time_series.inc"
