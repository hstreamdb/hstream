module HStream.StatsIntegrationSpec (spec) where

import           Test.Hspec

import           Control.Monad
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import           HStream.Utils

spec :: Spec
spec = describe "HStream.StatsIntegrationTest" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  perStreamTimeSeriesSpec

perStreamTimeSeriesSpec :: Spec
perStreamTimeSeriesSpec = aroundAll provideHstreamApi $ describe "PerStreamTimeSeries" $ do

  aroundWith withRandomStream $ do
    it "appends" $ \(api, name) -> do
      let methodName = "appends"
      PerStreamTimeSeriesStatsAllResponse resp <- perStreamTimeSeriesReq api methodName
      Map.lookup name resp `shouldBe` Nothing

      timeStamp <- getProtoTimestamp
      let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp TL.empty
      payloads <- V.map (buildRecord header) <$> V.replicateM 10 (newRandomByteString 1024)
      replicateM_ 100 $ appendRequest api name payloads

      PerStreamTimeSeriesStatsAllResponse resp' <- perStreamTimeSeriesReq api methodName
      Map.lookup name resp' `shouldSatisfy` \case
        Just (Just (StatsDoubleVals rates)) -> V.head rates > 0
        _                                   -> False

perStreamTimeSeriesReq
  :: HStreamApi ClientRequest ClientResult
  -> TL.Text
  -> IO PerStreamTimeSeriesStatsAllResponse
perStreamTimeSeriesReq HStreamApi{..} name = do
  let requestTimeout = 10
      statReq = PerStreamTimeSeriesStatsAllRequest name (Just $ StatsIntervalVals $ V.singleton 10000)
      req = ClientNormalRequest statReq requestTimeout $ MetadataMap Map.empty
  getServerResp =<< hstreamApiPerStreamTimeSeriesStatsAll req
