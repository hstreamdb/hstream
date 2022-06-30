{-# LANGUAGE BangPatterns #-}
module HStream.Gossip.FailureSpec where

import           Control.Concurrent       (threadDelay)
import           Control.Concurrent.Async (cancel)
import           Control.Concurrent.STM   (readTVarIO)
import qualified Data.Map.Strict          as Map
import           Test.Hspec               (SpecWith, describe, it, runIO,
                                           shouldBe)

import           HStream.Gossip           (GossipContext (serverList))
import           HStream.Gossip.TestUtils (startCluster)
import qualified HStream.Logger           as Log

spec :: SpecWith ()
spec =
  describe "FailureSpec" $ do
  runIO $ Log.setLogLevel (Log.Level Log.DEBUG) True

  it "Stop one server in a cluster" $ do
    let x = 3
    servers <- startCluster (fromIntegral x)
    threadDelay $ 5 * 1000 *1000
    lists <- mapM (readTVarIO . serverList . fst) (Map.elems servers)
    Map.size <$> lists `shouldBe` replicate x (x - 1)

    let y = fromIntegral x `div` 2
    _ <- cancel . snd $ servers Map.! y

    threadDelay $ 10 * 1000 *1000
    !lists' <- mapM (readTVarIO . serverList . fst) (Map.elems . Map.delete y $ servers)
    Map.size <$> lists' `shouldBe` replicate (x - 1) (x - 2)
