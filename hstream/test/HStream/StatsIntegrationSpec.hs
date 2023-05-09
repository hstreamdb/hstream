module HStream.StatsIntegrationSpec (spec) where

import           Test.Hspec

import           Control.Monad
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Base
import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import           HStream.Utils

spec :: Spec
spec = describe "HStream.StatsIntegrationTest" $ do
  runIO setupFatalSignalHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  perStreamTimeSeriesSpec

perStreamTimeSeriesSpec :: Spec
perStreamTimeSeriesSpec = aroundAll provideHstreamApi $ describe "PerStreamTimeSeries" $ do

  aroundWith withRandomStream $ do
    it "appends" $ \(api, (name, shardId)) -> do
      let methodName = "appends"
      PerStreamTimeSeriesStatsAllResponse resp <- perStreamTimeSeriesReq api methodName
      Map.lookup name resp `shouldBe` Nothing

      PerStreamTimeSeriesStatsResponse resp' <- perStreamTimeSeriesGetReq api methodName name
      resp' `shouldBe` Nothing

      let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty T.empty
      payloads <- V.map (mkHStreamRecord header) <$> V.replicateM 10 (newRandomByteString 1024)
      replicateM_ 100 $ appendRequest api name shardId payloads

      PerStreamTimeSeriesStatsAllResponse resp_ <- perStreamTimeSeriesReq api methodName
      let Just (Just (StatsDoubleVals rates1)) = Map.lookup name resp_
      PerStreamTimeSeriesStatsResponse (Just (StatsDoubleVals rates2)) <- perStreamTimeSeriesGetReq api methodName name

      V.head rates1 `shouldSatisfy` (> 0)
      V.head rates2 `shouldSatisfy` (> 0)

perStreamTimeSeriesReq
  :: HStreamApi ClientRequest ClientResult
  -> T.Text
  -> IO PerStreamTimeSeriesStatsAllResponse
perStreamTimeSeriesReq HStreamApi{..} name = do
  let requestTimeout = 10
      statReq = PerStreamTimeSeriesStatsAllRequest name (Just $ StatsIntervalVals $ V.singleton 10000)
      req = ClientNormalRequest statReq requestTimeout $ MetadataMap Map.empty
  getServerResp =<< hstreamApiPerStreamTimeSeriesStatsAll req

perStreamTimeSeriesGetReq
  :: HStreamApi ClientRequest ClientResult
  -> T.Text
  -> T.Text
  -> IO PerStreamTimeSeriesStatsResponse
perStreamTimeSeriesGetReq HStreamApi{..} name sName = do
  let requestTimeout = 10
      statReq = PerStreamTimeSeriesStatsRequest name sName (Just $ StatsIntervalVals $ V.singleton 10000)
      req = ClientNormalRequest statReq requestTimeout $ MetadataMap Map.empty
  getServerResp =<< hstreamApiPerStreamTimeSeriesStats req
