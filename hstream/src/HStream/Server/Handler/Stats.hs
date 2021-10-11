{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Stats
  ( perStreamTimeSeriesStatsAll
  , perStreamTimeSeriesStats
  --------------------
  , processTable
  , queryAllAppendInBytes
  , queryAllRecordBytes
  ) where

import           Network.GRPC.HighLevel.Generated

import           Data.Bifunctor
import           Data.Function
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Maybe
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Int
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi        as API
import           HStream.Stats                    (StatsHolder)
import qualified HStream.Stats                    as Stats
import qualified HStream.Utils                    as U
import qualified Text.Layout.Table                as LT

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
      let name = U.lazyTextToCBytes method
          intervals' = map fromIntegral . V.toList . statsIntervalValsIntervals $ intervals
      m <- Stats.stream_time_series_getall_by_name holder name intervals'
      let m' = Map.map (Just . StatsDoubleVals . V.fromList) . Map.mapKeys U.cBytesToLazyText $ m
      return $ PerStreamTimeSeriesStatsAllResponse m'

perStreamTimeSeriesStats
  :: StatsHolder
  -> ServerRequest 'Normal PerStreamTimeSeriesStatsRequest PerStreamTimeSeriesStatsResponse
  -> IO (ServerResponse 'Normal PerStreamTimeSeriesStatsResponse)
perStreamTimeSeriesStats holder (ServerNormalRequest _ PerStreamTimeSeriesStatsRequest {..}) = defaultExceptionHandle $ do
    maybe (pure Nothing) (Stats.stream_time_series_get holder methodName sName) intervals
    >>= U.returnResp . PerStreamTimeSeriesStatsResponse . fmap (StatsDoubleVals . V.fromList)
  where
    methodName = U.lazyTextToCBytes perStreamTimeSeriesStatsRequestMethod
    sName = U.lazyTextToCBytes perStreamTimeSeriesStatsRequestStreamName
    intervals = map fromIntegral . V.toList . statsIntervalValsIntervals <$>
      perStreamTimeSeriesStatsRequestIntervals

-------------------------------------------------------------------------------
queryAllAppendInBytes, queryAllRecordBytes :: U.HStreamClientApi -> IO (HM.HashMap T.Text (HM.HashMap T.Text Double))
queryAllAppendInBytes api = queryAdminTable api "appends_in"
  ["throughput_1min", "throughput_5min", "throughput_10min"]   . V.fromList $ map (* 1000)
  [60               , 300              , 600]
queryAllRecordBytes   api = queryAdminTable api "reads"
  ["throughput_15min", "throughput_30min", "throughput_60min"] . V.fromList $ map (* 1000)
  [900               , 1800              , 3600]

queryAdminTable :: U.HStreamClientApi
                -> T.Text -> [T.Text] -> V.Vector Int32
                -> IO (HM.HashMap T.Text (HM.HashMap T.Text Double))
queryAdminTable API.HStreamApi{..} tableName methodNames intervalVec = do
  let statsRequestTimeOut  = 10
      colNames :: [T.Text] = methodNames
      statsRequest         = API.PerStreamTimeSeriesStatsAllRequest (TL.fromStrict tableName) (Just $ API.StatsIntervalVals intervalVec)
      resRequest           = ClientNormalRequest statsRequest statsRequestTimeOut (MetadataMap Map.empty)
  API.PerStreamTimeSeriesStatsAllResponse respM <- hstreamApiPerStreamTimeSeriesStatsAll resRequest >>= U.getServerResp
  let resp  = filter (isJust . snd) (Map.toList respM) <&> second (V.toList . API.statsDoubleValsVals . fromJust)
      lbled = (map . second) (zip colNames) resp
  pure . HM.fromList
    $ (map .  first) TL.toStrict
    $ (map . second) HM.fromList lbled

processTable :: Show a => HM.HashMap T.Text (HM.HashMap T.Text a)
             -> [T.Text]
             -> [T.Text]
             -> String
processTable adminTable selectNames_ streamNames_
  | HM.size adminTable == 0 = "Empty status table." | otherwise =
    if any (`notElem` inTableSelectNames) selectNames || any (`notElem` inTableStreamNames) streamNames
      then "Col name or stream name not in scope."
      else
        let titles     = ["stream_id"] <> map T.unpack selectNames
            tableSiz   = L.length selectNames + 1
            colSpecs   = L.replicate tableSiz $ LT.column LT.expand LT.left LT.noAlign (LT.singleCutMark "...")
            tableSty   = LT.asciiS
            headerSpec = LT.titlesH titles
            rowGrps    = [LT.colsAllG LT.center resTable]
        in LT.tableString colSpecs tableSty headerSpec rowGrps
  where
    inTableSelectNames = (snd . head) (HM.toList adminTable) & map fst . HM.toList
    inTableStreamNames = fst <$> HM.toList adminTable
    selectNames = case selectNames_ of
      [] -> inTableSelectNames
      _  -> selectNames_
    streamNames = case streamNames_ of
      [] -> inTableStreamNames
      _  -> streamNames_
    processedTable = streamNames <&> \curStreamName ->
      let curLn  = HM.lookup curStreamName adminTable & fromJust
          curCol = selectNames <&> \curSelectName -> fromJust $
            HM.lookup curSelectName curLn
      in T.unpack curStreamName : map show curCol
    resTable = L.transpose processedTable
