{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.HandlerSpec (spec) where

import           Control.Concurrent
import           Control.Monad                    (forM_)
import qualified Data.Map.Strict                  as Map
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite.Class               (HasDefault (def))
import           Test.Hspec

import           HStream.Server.HStreamApi
import           HStream.SpecUtils
import           HStream.Store.Logger             (pattern C_DBG_ERROR,
                                                   setLogDeviceDbgLevel)
import qualified HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

spec :: Spec
spec =  describe "HStream.HandlerSpec" $ do
  runIO setupSigsegvHandler
  runIO $ setLogDeviceDbgLevel C_DBG_ERROR

  streamSpec

----------------------------------------------------------------------------------------------------------
-- StreamSpec

streamSpec :: Spec
streamSpec = aroundAll provideHstreamApi $ describe "StreamSpec" $ parallel $ do

  aroundWith withRandomStreamName $ do
    it "test createStream request" $ \(api, name) -> do
      let stream = mkStream name 3 10
      createStreamRequest api stream `shouldReturn` stream
      -- create an existed stream should fail
      createStreamRequest api stream `shouldThrow` anyException

  aroundWith (withRandomStreamNames 5) $ do
    it "test listStream request" $ \(api, names) -> do
      let createStreamReqs = zipWith mkStreamWithDefaultShards names [1, 2, 3, 3, 2]
      forM_ createStreamReqs $ \stream -> do
        createStreamRequest api stream `shouldReturn` stream

      resp <- listStreamRequest api
      let sortedResp = Set.fromList $ V.toList resp
          sortedReqs = Set.fromList createStreamReqs
      sortedReqs `shouldSatisfy` (`Set.isSubsetOf` sortedResp)

  aroundWith withRandomStreamName $ do
    xit "test deleteStream request" $ \(api, name) -> do
      let stream = mkStreamWithDefaultShards name 1
      createStreamRequest api stream `shouldReturn` stream
      resp <- listStreamRequest api
      resp `shouldSatisfy` V.elem stream
      deleteStreamRequest api name `shouldReturn` PB.Empty
      resp' <- listStreamRequest api
      resp' `shouldNotSatisfy`  V.elem stream
      -- delete a nonexistent stream without ignoreNonExist set should throw an exception
      deleteStreamRequest api name `shouldThrow` anyException
      -- delete a nonexistent stream with ignoreNonExist set should be okay
      cleanStreamReq api name `shouldReturn` PB.Empty

  aroundWith withRandomStreamName $ do
    it "test append request" $ \(api, name) -> do
      payload1 <- newRandomByteString 5
      payload2 <- newRandomByteString 5
      timeStamp <- getProtoTimestamp
      let stream = mkStreamWithDefaultShards name 1
          header  = buildRecordHeader HStreamRecordHeader_FlagRAW Map.empty T.empty
          record1 = mkHStreamRecord header payload1
          record2 = mkHStreamRecord header payload2
      createStreamRequest api stream `shouldReturn` stream
      -- FIXME: Even we have called the "syncLogsConfigVersion" method, there is
      -- __no__ guarantee that subsequent "append" will have an up-to-date view
      -- of the LogsConfig. For details, see Logdevice::Client::syncLogsConfigVersion
      threadDelay 2000000
      ListShardsResponse shards <- listShardsReq api name
      let Shard{..}:_ = V.toList shards
      resp <- appendRequest api name shardShardId (V.fromList [record1, record2])
      appendResponseStreamName resp `shouldBe` name
      recordIdBatchIndex <$> appendResponseRecordIds resp `shouldBe` V.fromList [0, 1]

-------------------------------------------------------------------------------------------------

createStreamRequest :: HStreamClientApi -> Stream -> IO Stream
createStreamRequest HStreamApi{..} stream =
  let req = ClientNormalRequest stream requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiCreateStream req

listStreamRequest :: HStreamClientApi -> IO (V.Vector Stream)
listStreamRequest HStreamApi{..} =
  let req = ClientNormalRequest ListStreamsRequest requestTimeout $ MetadataMap Map.empty
  in listStreamsResponseStreams <$> (getServerResp =<< hstreamApiListStreams req)

deleteStreamRequest :: HStreamClientApi -> T.Text -> IO PB.Empty
deleteStreamRequest HStreamApi{..} streamName =
  let delReq = def { deleteStreamRequestStreamName = streamName }
      req = ClientNormalRequest delReq requestTimeout $ MetadataMap Map.empty
  in getServerResp =<< hstreamApiDeleteStream req

requestTimeout :: Int
requestTimeout = 10
