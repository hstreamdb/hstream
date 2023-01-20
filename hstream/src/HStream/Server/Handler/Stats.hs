{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE GADTs           #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Server.Handler.Stats
  ( -- * For grpc-haskell
    perStreamTimeSeriesStatsAll
  , perStreamTimeSeriesStats
  , getStreamStatsHandler
  , getSubscriptionStatsHandler
    -- * For hs-grpc-server
  , handlePerStreamTimeSeriesStatsAll
  , handlePerStreamTimeSeriesStats
  , handleGetStreamStats
  , handleGetSubscriptionStats
  ) where

import           Control.Exception                (throwIO)
import           Data.Int                         (Int64)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated
import qualified Proto3.Suite                     as PS

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
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

getStreamStatsHandler
  :: StatsHolder
  -> ServerRequest 'Normal API.GetStreamStatsRequest API.GetStreamStatsResponse
  -> IO (ServerResponse 'Normal API.GetStreamStatsResponse)
getStreamStatsHandler holder req = defaultExceptionHandle $ do
  let (ServerNormalRequest _ (API.GetStreamStatsRequest stats)) = req
  res <- getStreamStatsInternal holder stats
  U.returnResp $ API.GetStreamStatsResponse {getStreamStatsResponseStatValues = res}

handleGetStreamStats
  :: StatsHolder
  -> G.UnaryHandler API.GetStreamStatsRequest API.GetStreamStatsResponse
handleGetStreamStats holder _ (API.GetStreamStatsRequest stats) = do
  res <- getStreamStatsInternal holder stats
  pure $ API.GetStreamStatsResponse {getStreamStatsResponseStatValues = res}

getSubscriptionStatsHandler
  :: StatsHolder
  -> ServerRequest 'Normal API.GetSubscriptionStatsRequest API.GetSubscriptionStatsResponse
  -> IO (ServerResponse 'Normal API.GetSubscriptionStatsResponse)
getSubscriptionStatsHandler holder req = defaultExceptionHandle $ do
  let (ServerNormalRequest _ (API.GetSubscriptionStatsRequest stats)) = req
  res <- getSubscriptionStatsInternal holder stats
  U.returnResp $ API.GetSubscriptionStatsResponse {getSubscriptionStatsResponseStatValues = res}

handleGetSubscriptionStats
  :: StatsHolder
  -> G.UnaryHandler API.GetSubscriptionStatsRequest API.GetSubscriptionStatsResponse
handleGetSubscriptionStats holder _ (API.GetSubscriptionStatsRequest stats) = do
  res <- getSubscriptionStatsInternal holder stats
  pure $ API.GetSubscriptionStatsResponse {getSubscriptionStatsResponseStatValues = res}

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

getStreamStatsInternal
  :: Stats.StatsHolder -> PS.Enumerated API.StreamStats -> IO (Map Text Int64)
getStreamStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request stream stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  res <- case stats of
    Right API.StreamStatsAppendInBytes ->
      Stats.stream_stat_getall_append_payload_bytes s
    Right API.StreamStatsAppendInRecords ->
      Stats.stream_stat_getall_append_record_count s
    Right API.StreamStatsTotalAppend ->
      Stats.stream_stat_getall_append_total s
    Right API.StreamStatsFailedAppend ->
      Stats.stream_stat_getall_append_failed s
    Left _ -> throwIO . HE.InvalidStatsType $ show stats
  return $ Map.mapKeys U.cBytesToText res

getSubscriptionStatsInternal
  :: Stats.StatsHolder
  -> PS.Enumerated API.SubscriptionStats
  -> IO (Map Text Int64)
getSubscriptionStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request subscription stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  res <- case stats of
    Right API.SubscriptionStatsDeliveryInBytes ->
      Stats.subscription_stat_getall_delivery_in_bytes s
    Right API.SubscriptionStatsDeliveryInRecords ->
      Stats.subscription_stat_getall_delivery_in_records s
    Right API.SubscriptionStatsAckReceived ->
      Stats.subscription_stat_getall_received_acks s
    Right API.SubscriptionStatsResendRecords ->
      Stats.subscription_stat_getall_resend_records s
    Right API.SubscriptionStatsMessageRequestCount ->
      Stats.subscription_stat_getall_request_messages_counter s
    Right API.SubscriptionStatsMessageResponseCount ->
      Stats.subscription_stat_getall_response_messages_counter s
    Left _ -> throwIO . HE.InvalidStatsType $ show stats
  return $ Map.mapKeys U.cBytesToText res
