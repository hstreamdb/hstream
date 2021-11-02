{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Server.Handler.Stats
  ( perStreamTimeSeriesStatsAll
  , perStreamTimeSeriesStats
  ) where

import           Network.GRPC.HighLevel.Generated

import qualified Data.Map.Strict                  as Map
import qualified Data.Vector                      as V
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Stats                    (StatsHolder)
import qualified HStream.Stats                    as Stats
import qualified HStream.Utils                    as U

perStreamTimeSeriesStatsAll
  :: StatsHolder
  -> ServerRequest 'Normal PerStreamTimeSeriesStatsAllRequest PerStreamTimeSeriesStatsAllResponse
  -> IO (ServerResponse 'Normal PerStreamTimeSeriesStatsAllResponse)
perStreamTimeSeriesStatsAll holder req = defaultExceptionHandle $ do
  let ServerNormalRequest
        _metadata
        PerStreamTimeSeriesStatsAllRequest
          { perStreamTimeSeriesStatsAllRequestMethod = method
          , perStreamTimeSeriesStatsAllRequestIntervals = m_intervals
          } = req
  U.returnResp =<< maybe (pure emptyResp) (getall method) m_intervals
  where
    emptyResp = PerStreamTimeSeriesStatsAllResponse Map.empty
    getall method intervals = do
      let name       = U.textToCBytes method
          intervals' = map fromIntegral . V.toList . statsIntervalValsIntervals $ intervals
      m <- Stats.stream_time_series_getall_by_name holder name intervals'
      let m' = Map.map (Just . StatsDoubleVals . V.fromList) . Map.mapKeys U.cBytesToText $ m
      return $ PerStreamTimeSeriesStatsAllResponse m'

perStreamTimeSeriesStats
  :: StatsHolder
  -> ServerRequest 'Normal PerStreamTimeSeriesStatsRequest PerStreamTimeSeriesStatsResponse
  -> IO (ServerResponse 'Normal PerStreamTimeSeriesStatsResponse)
perStreamTimeSeriesStats holder (ServerNormalRequest _ PerStreamTimeSeriesStatsRequest {..}) = defaultExceptionHandle $ do
    maybe (pure Nothing) (Stats.stream_time_series_get holder methodName sName) intervals
    >>= U.returnResp . PerStreamTimeSeriesStatsResponse . fmap (StatsDoubleVals . V.fromList)
  where
    methodName = U.textToCBytes perStreamTimeSeriesStatsRequestMethod
    sName      = U.textToCBytes perStreamTimeSeriesStatsRequestStreamName
    intervals  = map fromIntegral . V.toList . statsIntervalValsIntervals <$>
      perStreamTimeSeriesStatsRequestIntervals
