{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip.Gossip where

import           Control.Concurrent            (threadDelay)
import           Control.Concurrent.STM        (atomically, check, readTVar,
                                                stateTVar, writeTChan)
import           Data.ByteString               (ByteString)
import qualified Data.Map.Strict               as Map
import           Data.Serialize                (encode)
import qualified HStream.Logger                as Log
import           Network.GRPC.HighLevel.Client (ClientResult (..))
import qualified Network.GRPC.HighLevel.Client as GRPC
import           System.Random.Shuffle         (shuffle')

import           HStream.Gossip.HStreamGossip  (Gossip (..), HStreamGossip (..),
                                                hstreamGossipClient)
import           HStream.Gossip.Types          (GossipContext (..),
                                                GossipOpts (..),
                                                RequestAction (..))
import           HStream.Gossip.Utils          (getMessagesToSend,
                                                mkClientNormalRequest)

gossip :: ByteString -> GRPC.Client -> IO ()
gossip msg client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipGossip (mkClientNormalRequest $ Gossip msg) >>= \case
    ClientNormalResponse {} -> return ()
    ClientErrorResponse  {} -> Log.debug "Failed to send gossip"

scheduleGossip :: GossipContext -> IO ()
scheduleGossip gc@GossipContext{..} = do
  atomically doGossip
  threadDelay $ gossipInterval gossipOpts
  scheduleGossip gc
  where
    doGossip = do
      memberMap <- readTVar serverList
      check (not $ Map.null memberMap)
      msgs <- stateTVar broadcastPool $ getMessagesToSend (fromIntegral (Map.size memberMap))
      check (not $ null msgs)
      let members = Map.keys memberMap
      let selected = take 3 $ shuffle' members (length members) randomGen
      writeTChan actionChan (DoGossip selected (encode msgs))
