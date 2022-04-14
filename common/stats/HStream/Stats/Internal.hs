{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Stats.Internal where

import           Data.Int
import           Data.Primitive
import           Data.Word
import           Foreign.C.Types
import           Foreign.Ptr

import           HStream.Foreign


-------------------------------------------------------------------------------
-- Considering as internal functions

data CStats
data CStatsHolder

foreign import ccall unsafe "hs_stats.h &delete_stats"
  c_delete_stats_fun :: FunPtr (Ptr CStats -> IO ())

foreign import ccall unsafe "hs_stats.h new_stats_holder"
  c_new_stats_holder :: IO (Ptr CStatsHolder)

foreign import ccall unsafe "hs_stats.h &delete_stats_holder"
  c_delete_stats_holder_fun :: FunPtr (Ptr CStatsHolder -> IO ())

foreign import ccall unsafe "hs_stats.h stats_holder_print"
  c_stats_holder_print :: Ptr CStatsHolder -> IO ()

foreign import ccall unsafe "hs_stats.h new_aggregate_stats"
  c_new_aggregate_stats :: Ptr CStatsHolder -> IO (Ptr CStats)

#define PER_X_STAT_DEFINE(prefix, name) \
foreign import ccall unsafe "hs_stats.h prefix##add_##name"                    \
  prefix##add_##name                                                           \
    :: Ptr CStatsHolder -> BA# Word8 -> Int64 -> IO ();                        \
                                                                               \
foreign import ccall unsafe "hs_stats.h prefix##get_##name"                    \
  prefix##get_##name                                                           \
    :: Ptr CStats -> BA# Word8 -> IO Int64;                                    \
                                                                               \
foreign import ccall unsafe "hs_stats.h prefix##getall_##name"                 \
  prefix##getall_##name                                                        \
    :: Ptr CStats                                                              \
    -> MBA# Int                                                                \
    -> MBA# (Ptr StdString)                                                    \
    -> MBA# (Ptr Int64)                                                        \
    -> MBA# (Ptr (StdVector StdString))                                        \
    -> MBA# (Ptr (StdVector Int64))                                            \
    -> IO ();

#define STAT_DEFINE(name, _) PER_X_STAT_DEFINE(stream_stat_, name)
#include "../include/per_stream_stats.inc"

#define STAT_DEFINE(name, _) PER_X_STAT_DEFINE(subscription_stat_, name)
#include "../include/per_subscription_stats.inc"

#define TIME_SERIES_DEFINE(name, _, __, ___)                                   \
foreign import ccall unsafe "hs_stats.h stream_time_series_add_##name"         \
  stream_time_series_add_##name                                                \
    :: Ptr CStatsHolder -> BA# Word8 -> Int64 -> IO ();
#include "../include/per_stream_time_series.inc"

foreign import ccall unsafe "hs_stats.h stream_time_series_get"
  c_stream_time_series_get
    :: Ptr CStatsHolder -> BA# Word8 -> BA# Word8
    -> Int -> BA# Int -> MBA# Double
    -> IO CInt

foreign import ccall unsafe "hs_stats.h stream_time_series_getall_by_name"
  c_stream_time_series_getall_by_name
    :: Ptr CStatsHolder -> BA# Word8
    -> Int -> BA# Int
    -> MBA# Int
    -> MBA# (Ptr StdString)
    -> MBA# (Ptr (FollySmallVector Double))
    -> MBA# (Ptr (StdVector StdString))
    -> MBA# (Ptr (StdVector (FollySmallVector Double)))
    -> IO CInt

#undef PER_X_STAT_DEFINE
