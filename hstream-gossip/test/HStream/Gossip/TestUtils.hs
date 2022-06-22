{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.Gossip.TestUtils where

import           Control.Concurrent.Async     (Async, mapConcurrently)
import           Control.Monad                (replicateM, zipWithM)
import           Data.Map                     (Map)
import qualified Data.Map                     as Map
import           Data.Streaming.Network       (getUnassignedPort)
import           Data.Word                    (Word32)

import qualified HStream.Gossip.HStreamGossip as API
import           HStream.Gossip.Start         (initGossipContext, startGossip)
import           HStream.Gossip.Types         (GossipContext (..), ServerId)
import           HStream.Gossip.Utils         (defaultGossipOpts)

type MemInfo = (GossipContext, Async ())

type Servers = Map ServerId MemInfo

startCluster :: Word32 -> IO Servers
startCluster n = do
  let host = "127.0.0.1"
  !port <- fromIntegral <$> getUnassignedPort
  !ports <- replicateM (fromIntegral n) $ fromIntegral <$> getUnassignedPort
  !gcs   <- zipWithM (\x y -> initGossipContext defaultGossipOpts mempty
                           $ API.ServerNodeInternal x host port y)
                     [1 .. n]
                     ports
  asyncs <- mapConcurrently (startGossip host (zip (repeat host) (fromIntegral <$> ports))) gcs
  return $ foldr (\x@(GossipContext{..}, _)
                    -> Map.insert (API.serverNodeInternalId serverSelf) x)
                 mempty
         $ zip gcs asyncs
