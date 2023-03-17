{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip.Gossip where

import           Control.Concurrent             (readMVar, threadDelay)
import           Control.Concurrent.STM         (atomically, check, stateTVar,
                                                 writeTChan)
import           Control.Monad                  (forever)
import qualified Data.Vector                    as V
import           Network.GRPC.HighLevel.Client  (ClientResult (..))
import qualified Network.GRPC.HighLevel.Client  as GRPC
import           System.Random.Shuffle          (shuffle')

import           HStream.Gossip.HStreamGossip   (Gossip (..),
                                                 HStreamGossip (..),
                                                 hstreamGossipClient)
import qualified HStream.Gossip.HStreamGossip   as G
import           HStream.Gossip.Types           (GossipContext (..),
                                                 GossipOpts (..),
                                                 RequestAction (..))
import           HStream.Gossip.Utils           (getMessagesToSend,
                                                 getOtherMembersSTM,
                                                 mkClientNormalRequest)
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as I

doGossip :: GRPC.Client -> [G.Message] -> IO ()
doGossip client msgs = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipSendGossip (mkClientNormalRequest . Gossip $ V.fromList msgs) >>= \case
    ClientNormalResponse {} -> return ()
    ClientErrorResponse  {} -> Log.debug "failed to send gossip to node"

scheduleGossip :: GossipContext -> IO ()
scheduleGossip gc@GossipContext{..} = do
  _ <- readMVar clusterInited
  forever $ do
    atomically doGossip
    threadDelay $ gossipInterval gossipOpts
  where
    doGossip = do
      memberMap <- getOtherMembersSTM gc
      check (not $ null memberMap)
      msgs <- stateTVar broadcastPool $ getMessagesToSend (fromIntegral (length memberMap))
      check (not $ null msgs)
      let members = I.serverNodeId <$> memberMap
      let selected = take (gossipFanout gossipOpts) $ shuffle' members (length members) randomGen
      writeTChan actionChan (DoGossip selected msgs)
