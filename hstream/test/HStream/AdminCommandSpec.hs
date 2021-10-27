{-# LANGUAGE OverloadedStrings #-}

module HStream.AdminCommandSpec where

import           Test.Hspec

import           Control.Monad
import qualified Data.Aeson                       as A
import qualified Data.HashMap.Internal            as HM
import qualified Data.Map.Strict                  as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Text.Lazy.Encoding          as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger
import           HStream.Utils

spec :: Spec
spec = describe "HStream.AdminCommnadSpec" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  adminCommandStatsSpec

adminCommandStatsSpec :: Spec
adminCommandStatsSpec = aroundAll provideHstreamApi $ describe "adminCommandStatsSpec" $ do
  aroundWith withRandomStream $ do
    it "stats append" $ \(api, streamName) -> do

      timeStamp <- getProtoTimestamp
      let header = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty timeStamp TL.empty
      payloads <- V.map (buildRecord header) <$> V.replicateM 10 (newRandomByteString 1024)
      replicateM_ 100 $ appendRequest api streamName payloads

      resp <- adminCommandStatsReq api streamName
      let Just (Just resultMap) =
            A.decode @(Maybe A.Object) . TL.encodeUtf8 . adminCommandResponseResult $ resp
      resultMap `shouldSatisfy`
        (\m -> case HM.lookup "throughput_1min" m of
          Just (A.Number x) -> x > 0 &&
            case HM.lookup "throughput_2min" m of
              Just (A.Number y) -> y > 0
              _                 -> False
          _ -> False)

adminCommandStatsReq
  :: HStreamApi ClientRequest ClientResult
  -> TL.Text
  -> IO AdminCommandResponse
adminCommandStatsReq HStreamApi{..} name = do
  let requestTimeout = 10
      statReq = AdminCommandRequest ("stats appends --stream-name " <> name <> " --intervals 1min --intervals 2min")
      req = ClientNormalRequest statReq requestTimeout $ MetadataMap Map.empty
  getServerResp =<< hstreamApiSendAdminCommand req
