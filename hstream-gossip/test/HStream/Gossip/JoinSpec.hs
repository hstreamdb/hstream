{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Gossip.JoinSpec where

import           Control.Concurrent.STM         (readTVarIO)
import qualified Data.Map.Strict                as Map
import           Data.Streaming.Network         (getUnassignedPort)
import           Test.Hspec                     (SpecWith, describe, it, runIO,
                                                 shouldBe)

import           HStream.Gossip                 (GossipContext (..),
                                                 ServerStatus (..),
                                                 defaultGossipOpts,
                                                 initGossipContext, startGossip)
import qualified HStream.Gossip.HStreamGossip   as API
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as I

spec :: SpecWith ()
spec = describe "JoinSpec" $ do
  runIO $ Log.setLogLevel (Log.Level Log.FATAL) True

  runSingleJoinSpec

runSingleJoinSpec :: SpecWith ()
runSingleJoinSpec = describe "runSingleJoinSpec" $ do
  it "Single Join Test" $ do
    let host = "127.0.0.1"
    port <- fromIntegral <$> getUnassignedPort
    port1 <- getUnassignedPort
    port2 <- fromIntegral <$> getUnassignedPort
    let node1 = I.ServerNode 1 host port (fromIntegral port1)
    let node2 = I.ServerNode 2 host port port2
    gc <- initGossipContext defaultGossipOpts mempty node1
    _ <- startGossip host [(host, port1)] gc
    gc2 <- initGossipContext defaultGossipOpts mempty node2
    _ <- startGossip host [(host, port1)] gc2
    !l1 <- readTVarIO . serverList $ gc
    !l2 <- readTVarIO . serverList $ gc2
    serverInfo <$> Map.elems l1 `shouldBe` [serverSelf gc2]
    serverInfo <$> Map.elems l2 `shouldBe` [serverSelf gc]
