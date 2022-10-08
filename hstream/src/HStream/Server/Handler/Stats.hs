{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Server.Handler.Stats
  ( -- * For grpc-haskell
    perStreamTimeSeriesStatsAll
  , perStreamTimeSeriesStats
    -- * For hs-grpc-server
  , handlePerStreamTimeSeriesStatsAll
  , handlePerStreamTimeSeriesStats
  ) where

import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import           Control.Exception                (throwIO)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Vector                      as V
import qualified HStream.Exception                as HE
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Stats                    (StatsHolder)
import qualified HStream.Stats                    as Stats
import qualified HStream.Utils                    as U

-------------------------------------------------------------------------------

perStreamTimeSeriesStatsAll
  :: StatsHolder
  -> ServerRequest 'Normal PerStreamTimeSeriesStatsAllRequest PerStreamTimeSeriesStatsAllResponse
  -> IO (ServerResponse 'Normal PerStreamTimeSeriesStatsAllResponse)
perStreamTimeSeriesStatsAll holder (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  r <- getPerStreamTimeSeriesStatsAll holder req
  U.returnResp $ PerStreamTimeSeriesStatsAllResponse r

handlePerStreamTimeSeriesStatsAll
  :: StatsHolder
  -> G.UnaryHandler PerStreamTimeSeriesStatsAllRequest PerStreamTimeSeriesStatsAllResponse
handlePerStreamTimeSeriesStatsAll holder _ req = catchDefaultEx $
  PerStreamTimeSeriesStatsAllResponse <$> getPerStreamTimeSeriesStatsAll holder req

perStreamTimeSeriesStats
  :: StatsHolder
  -> ServerRequest 'Normal PerStreamTimeSeriesStatsRequest PerStreamTimeSeriesStatsResponse
  -> IO (ServerResponse 'Normal PerStreamTimeSeriesStatsResponse)
perStreamTimeSeriesStats holder (ServerNormalRequest _ req) = defaultExceptionHandle $ do
  r <- getPerStreamTimeSeriesStats holder req
  U.returnResp $ PerStreamTimeSeriesStatsResponse r

handlePerStreamTimeSeriesStats
  :: StatsHolder
  -> G.UnaryHandler PerStreamTimeSeriesStatsRequest PerStreamTimeSeriesStatsResponse
handlePerStreamTimeSeriesStats holder _ req = catchDefaultEx $ do
  r <- getPerStreamTimeSeriesStats holder req
  pure $ PerStreamTimeSeriesStatsResponse r

-------------------------------------------------------------------------------

getPerStreamTimeSeriesStats
   :: StatsHolder -> PerStreamTimeSeriesStatsRequest -> IO (Maybe StatsDoubleVals)
getPerStreamTimeSeriesStats holder PerStreamTimeSeriesStatsRequest{..} = do
  r <- maybe (pure Nothing) (Stats.stream_time_series_get holder methodName sName) m_intervals
  pure $ StatsDoubleVals . V.fromList <$> r
  where
    methodName  = U.textToCBytes perStreamTimeSeriesStatsRequestMethod
    sName       = U.textToCBytes perStreamTimeSeriesStatsRequestStreamName
    m_intervals = map fromIntegral . V.toList . statsIntervalValsIntervals <$>
      perStreamTimeSeriesStatsRequestIntervals

getPerStreamTimeSeriesStatsAll
  :: StatsHolder
  -> PerStreamTimeSeriesStatsAllRequest
  -> IO (Map.Map Text (Maybe StatsDoubleVals))
getPerStreamTimeSeriesStatsAll holder req = do
  let PerStreamTimeSeriesStatsAllRequest
        { perStreamTimeSeriesStatsAllRequestMethod = method
        , perStreamTimeSeriesStatsAllRequestIntervals = m_intervals
        } = req
  maybe (pure Map.empty) (getall method) m_intervals
  where
    getall method intervals = do
      let name       = U.textToCBytes method
          intervals' = map fromIntegral . V.toList . statsIntervalValsIntervals $ intervals
      m <- Stats.stream_time_series_getall holder name intervals'
      case m of
        Left errmsg -> throwIO $ HE.InvalidStatsInterval errmsg
        Right m' -> pure $ Map.map (Just . StatsDoubleVals . V.fromList) . Map.mapKeys U.cBytesToText $ m'
