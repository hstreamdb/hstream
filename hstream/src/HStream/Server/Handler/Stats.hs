{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Handler.Stats
  ( -- * For grpc-haskell
    perStreamTimeSeriesStatsAll
  , perStreamTimeSeriesStats
  , getStatsHandler
    -- * For hs-grpc-server
  , handlePerStreamTimeSeriesStatsAll
  , handlePerStreamTimeSeriesStats
  , handleGetStats
  ) where

import           Control.Exception                (throwIO)
import           Data.Functor                     ((<&>))
import           Data.Int                         (Int64)
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Vector                      as V
import qualified Data.Vector.Algorithms           as V
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated
import qualified Proto3.Suite                     as PS

import           Control.Monad                    (forM, when)
import           Data.Either                      (partitionEithers)
import           Data.Maybe                       (mapMaybe)
import qualified Data.Text                        as T
import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import           HStream.Stats                    (StatsHolder)
import qualified HStream.Stats                    as Stats
import qualified HStream.Utils                    as U
import           Z.Data.CBytes                    (CBytes)

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

getStatsHandler
  :: StatsHolder
  -> ServerRequest 'Normal API.GetStatsRequest API.GetStatsResponse
  -> IO (ServerResponse 'Normal API.GetStatsResponse)
getStatsHandler holder (ServerNormalRequest _ (API.GetStatsRequest mstats)) = defaultExceptionHandle $ do
  (failed, suc) <- getStats mstats holder
  U.returnResp $ API.GetStatsResponse {getStatsResponseStatsValues = V.fromList suc, getStatsResponseErrors = V.fromList failed}

handleGetStats
  :: StatsHolder
  -> G.UnaryHandler API.GetStatsRequest API.GetStatsResponse
handleGetStats holder _ (API.GetStatsRequest mstats) = do
  (failed, suc) <- getStats mstats holder
  pure $ API.GetStatsResponse {getStatsResponseStatsValues = V.fromList suc, getStatsResponseErrors = V.fromList failed}

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

getStats :: V.Vector StatType -> StatsHolder -> IO ([StatError], [StatValue])
getStats mstats holder = do
  let stats = mapMaybe statTypeStat . V.toList $ V.nub mstats
  when (null stats) $ throwIO . HE.InvalidStatsType $ show mstats
  partitionEithers <$> forM stats (getStatsInternal holder)

getStatsInternal :: StatsHolder -> StatTypeStat -> IO (Either StatError StatValue)
getStatsInternal holder s@(StatTypeStatStreamStat stats) = do
  getStreamStatsInternal holder stats <&> convert s
getStatsInternal holder s@(StatTypeStatSubStat stats) = do
  getSubscriptionStatsInternal holder stats <&> convert s
getStatsInternal holder s@(StatTypeStatConnStat stats) = do
  getConnectorStatsInternal holder stats <&> convert s
getStatsInternal holder s@(StatTypeStatQueryStat stats) = do
  getQueryStatsInternal holder stats <&> convert s
getStatsInternal holder s@(StatTypeStatViewStat stats) = do
  getViewStatsInternal holder stats <&> convert s

getStreamStatsInternal
  :: Stats.StatsHolder
  -> PS.Enumerated API.StreamStats
  -> IO (Either T.Text (Map CBytes Int64))
getStreamStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request stream stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  case stats of
    Right API.StreamStatsAppendInBytes ->
      Stats.stream_stat_getall_append_in_bytes s <&> Right
    Right API.StreamStatsAppendInRecords ->
      Stats.stream_stat_getall_append_in_records s <&> Right
    Right API.StreamStatsAppendTotal ->
      Stats.stream_stat_getall_append_total s <&> Right
    Right API.StreamStatsAppendFailed ->
      Stats.stream_stat_getall_append_failed s <&> Right
    Left _ -> return . Left . T.pack $ "invalid stat type " <> show stats

getSubscriptionStatsInternal
  :: Stats.StatsHolder
  -> PS.Enumerated API.SubscriptionStats
  -> IO (Either T.Text (Map CBytes Int64))
getSubscriptionStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request subscription stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  case stats of
    Right API.SubscriptionStatsSendOutBytes ->
      Stats.subscription_stat_getall_send_out_bytes s <&> Right
    Right API.SubscriptionStatsSendOutRecords ->
      Stats.subscription_stat_getall_send_out_records s <&> Right
    Right API.SubscriptionStatsSendOutRecordsFailed ->
      Stats.subscription_stat_getall_send_out_records_failed s <&> Right
    Right API.SubscriptionStatsReceivedAcks ->
      Stats.subscription_stat_getall_received_acks s <&> Right
    Right API.SubscriptionStatsResendRecords ->
      Stats.subscription_stat_getall_resend_records s <&> Right
    Right API.SubscriptionStatsResendRecordsFailed ->
      Stats.subscription_stat_getall_resend_records_failed s <&> Right
    Right API.SubscriptionStatsRequestMessages ->
      Stats.subscription_stat_getall_request_messages s <&> Right
    Right API.SubscriptionStatsResponseMessages ->
      Stats.subscription_stat_getall_response_messages s <&> Right
    Left _ -> return . Left . T.pack $ "invalid stat type " <> show stats

getConnectorStatsInternal
  :: Stats.StatsHolder
  -> PS.Enumerated API.ConnectorStats
  -> IO (Either T.Text (Map CBytes Int64))
getConnectorStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request stream stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  case stats of
    Right API.ConnectorStatsDeliveredInRecords ->
      Stats.connector_stat_getall_delivered_in_records s <&> Right
    Right API.ConnectorStatsDeliveredInBytes ->
      Stats.connector_stat_getall_delivered_in_bytes s <&> Right
    Left _ -> return . Left . T.pack $ "invalid stat type " <> show stats

getQueryStatsInternal
  :: Stats.StatsHolder
  -> PS.Enumerated API.QueryStats
  -> IO (Either T.Text (Map CBytes Int64))
getQueryStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request query stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  case stats of
    Right API.QueryStatsTotalInputRecords ->
      Stats.query_stat_getall_total_input_records s <&> Right
    Right API.QueryStatsTotalOutputRecords ->
      Stats.query_stat_getall_total_output_records s <&> Right
    Right API.QueryStatsTotalExecuteErrors ->
      Stats.query_stat_getall_total_execute_errors s <&> Right
    Left _ -> return . Left . T.pack $ "invalid stat type " <> show stats

getViewStatsInternal
  :: Stats.StatsHolder
  -> PS.Enumerated API.ViewStats
  -> IO (Either T.Text (Map CBytes Int64))
getViewStatsInternal statsHolder (PS.Enumerated stats) = do
  Log.debug $ "request view stats: " <> Log.buildString' stats
  s <- Stats.newAggregateStats statsHolder
  case stats of
    Right API.ViewStatsTotalExecuteQueries ->
      Stats.view_stat_getall_total_execute_queries s <&> Right
    Left _ -> return . Left . T.pack $ "invalid stat type " <> show stats

convert :: StatTypeStat -> Either T.Text (Map CBytes Int64) -> Either StatError StatValue
convert stat (Left msg) = Left $ mkStatError stat msg
convert stat (Right value) = Right . mkStatValue stat $ Map.mapKeys U.cBytesToText value

mkStatType :: StatTypeStat -> StatType
mkStatType stat = StatType {statTypeStat = Just stat}

mkStatValue :: StatTypeStat -> Map T.Text Int64 -> StatValue
mkStatValue stat values =
  StatValue {statValueStatType = Just . mkStatType $ stat, statValueStatValues = values}

mkStatError :: StatTypeStat -> T.Text -> StatError
mkStatError stat msg =
  StatError {statErrorStatType = Just . mkStatType $ stat, statErrorMessage = msg}
