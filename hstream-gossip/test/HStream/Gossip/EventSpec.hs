{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip.EventSpec where

import           Control.Concurrent       (putMVar, threadDelay)
import           Control.Concurrent.STM   (atomically, readTVarIO, writeTQueue)
import qualified Data.IntMap              as IM
import qualified Data.Map.Strict          as Map
import           Test.Hspec               (SpecWith, describe, it, runIO,
                                           shouldBe)

import           HStream.Gossip.TestUtils (startCluster)
import           HStream.Gossip.Types     (EventMessage (..),
                                           GossipContext (..), InitType (User))
import qualified HStream.Gossip.Types     as T
import           HStream.Gossip.Utils     (incrementTVar)
import qualified HStream.Logger           as Log

spec :: SpecWith ()
spec =
  describe "EventSpec" $ do
  runIO $ Log.setLogLevel (Log.Level Log.DEBUG) True

  it "Send one single event to the cluster" $ do
    let x = 3
    servers <- startCluster (fromIntegral x)
    -- putMVar (clusterInited . fst . head . Map.elems $ servers) User
    threadDelay $ 5 * 1000 *1000
    lists <- mapM (readTVarIO . serverList . fst) (Map.elems servers)
    Map.size <$> lists `shouldBe` replicate x (x - 1)

    let y = fromIntegral x `div` 2
    let GossipContext{..} = fst $ servers Map.! y
    _msg <- atomically $ do
      lpTime <- incrementTVar eventLpTime
      let eventMessage = EventMessage "Greeting" lpTime "Hello from Test"
      writeTQueue eventPool eventMessage
      return eventMessage
    threadDelay $ 5 * 1000 * 1000
    eventss <- mapM ((concat . IM.elems <$>) . readTVarIO . T.seenEvents . fst) (Map.elems servers)
    eventss `shouldBe` replicate x [("Greeting", "Hello from Test")]
