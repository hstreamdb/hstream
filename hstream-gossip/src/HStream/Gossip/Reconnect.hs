{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip.Reconnect where

import           Control.Concurrent               (readMVar, threadDelay)
import           Control.Concurrent.STM           (atomically, check, readTVar,
                                                   readTVarIO, writeTChan,
                                                   writeTQueue)
import           Control.Monad                    (forever, when)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import           Data.Word                        (Word32)
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           System.Random                    (randomRIO)
import           System.Random.Shuffle            (shuffle')

import           HStream.Gossip.HStreamGossip     (Empty (..),
                                                   HStreamGossip (..),
                                                   hstreamGossipClient)
import           HStream.Gossip.Types             (GossipContext (..),
                                                   GossipOpts (..), Messages,
                                                   RequestAction (..),
                                                   ServerStatus (..))
import qualified HStream.Gossip.Types             as T
import           HStream.Gossip.Utils             (foreverCatchAll,
                                                   getMemberList, getMsgInc,
                                                   mkClientNormalRequest',
                                                   mkGRPCClientConf')
import           HStream.Logger                   as Log
import qualified HStream.Server.HStreamInternal   as I

scheduleReconnect :: GossipContext -> IO ()
scheduleReconnect gc@GossipContext{..} = do
  _ <- readMVar clusterInited
  -- FIXME: does catch all exception(SomeException) proper here?
  foreverCatchAll True "Gossip ScheduleReconnect" $ do
    memberMap <- atomically $ do
      memberMap <- readTVar deadServers
      check (not $ Map.null memberMap)
      return memberMap
    let members = Map.keys memberMap
    let lDead = length members
    let pingOrder = shuffle' members lDead randomGen
    lTotal <- length <$> getMemberList gc
    probToReconnect <- randomRIO (0, 1) :: IO Float
    let prob = fromIntegral lDead / fromIntegral (min 1 (lTotal - lDead))
    when (probToReconnect < prob) $ do
      runReconnect pingOrder
      threadDelay (30 * 1000 * 1000)
  where
    runReconnect [] = pure ()
    runReconnect (x:_) = do
      Map.lookup x . snd <$> readTVarIO serverList >>= \case
        Nothing -> return ()
        Just ss@ServerStatus{serverInfo = I.ServerNode{..}, ..} -> do
          state <- readTVarIO serverState
          when (state == T.ServerDead) $ do
            Log.info $ "Trying to reconnect to server node " <> Log.buildString' serverNodeId
            GRPC.withGRPCClient (mkGRPCClientConf' serverNodeGossipAddress (fromIntegral serverNodeGossipPort)) $ \client ->
              doReconnect client gc ss x

doReconnect
  :: GRPC.Client -> GossipContext -> ServerStatus -> Word32
  -> IO ()
doReconnect client GossipContext{gossipOpts = GossipOpts{..}, ..}
  ServerStatus{serverInfo = sNode, ..} _sid = do
  maybeAck <- reconnect (max 1 (roundtripTimeout `div` (1000*1000))) [] client
  case maybeAck of
    Just _ack -> do
      atomically $ do inc  <- readTVar stateIncarnation
                      writeTQueue statePool $ T.GAlive (inc + 1) sNode serverSelf
      Log.info $ "HStream-Gossip: reconnecting to " <> Log.buildString' (I.serverNodeId sNode)
    Nothing -> pure ()

reconnect :: Int -> Messages -> GRPC.Client -> IO (Maybe ())
reconnect ttSec msg client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipSendReconnect (mkClientNormalRequest' Empty ttSec) >>= \case
    GRPC.ClientNormalResponse {} -> return (Just ())
    GRPC.ClientErrorResponse _   ->  Log.debug "HStream-Gossip: failed to reconnect" >> return Nothing
