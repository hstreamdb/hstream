{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip
  ( GossipContext(..)
  , GossipOpts(..)
  , defaultGossipOpts

  , initGossipContext
  , bootstrap
  , startGossip
  , initCluster

  , broadcastEvent
  , createEventHandlers
  , getSeenEvents
  , getMemberList
  , getMemberListSTM
  , getClusterStatus
  , getEpoch
  , getEpochSTM
  , getFailedNodes
  , getFailedNodesSTM

  ) where

import           Control.Concurrent             (tryPutMVar, tryReadMVar)
import           Control.Concurrent.STM         (STM, atomically, readTVar,
                                                 readTVarIO, writeTQueue)
import           Data.Functor                   ((<&>))
import qualified Data.HashMap.Strict            as HM
import qualified Data.Map.Strict                as Map
import           Data.Word                      (Word32)

import           HStream.Common.Types           (fromInternalServerNode)
import           HStream.Gossip.Start           (bootstrap, initGossipContext,
                                                 startGossip)
import           HStream.Gossip.Types           (EventHandler, EventMessage,
                                                 EventName, GossipContext (..),
                                                 GossipOpts (..),
                                                 InitType (User), SeenEvents,
                                                 ServerStatus (..),
                                                 defaultGossipOpts)
import qualified HStream.Logger                 as Log
import           HStream.Server.HStreamApi      (NodeState (..),
                                                 ServerNode (..),
                                                 ServerNodeStatus (..))
import qualified HStream.Server.HStreamInternal as I
import           HStream.Utils                  (pattern EnumPB)

initCluster :: GossipContext -> IO ()
initCluster GossipContext{..} = tryPutMVar clusterInited User >>= \case
  True  -> return ()
  False -> Log.warning "The server has already received an init signal"

broadcastEvent :: GossipContext -> EventMessage -> IO ()
broadcastEvent GossipContext {..} = atomically . writeTQueue eventPool

createEventHandlers :: [(EventName, EventHandler)] -> Map.Map EventName EventHandler
createEventHandlers = Map.fromList

getSeenEvents :: GossipContext -> IO SeenEvents
getSeenEvents GossipContext {..} = readTVarIO seenEvents

getMemberList :: GossipContext -> IO [I.ServerNode]
getMemberList GossipContext {..} =
  readTVarIO serverList <&> ((:) serverSelf . map serverInfo . Map.elems . snd)

getMemberListSTM :: GossipContext -> STM [I.ServerNode]
getMemberListSTM GossipContext {..} =
  readTVar serverList <&> ((:) serverSelf . map serverInfo . Map.elems . snd)

getEpoch :: GossipContext -> IO Word32
getEpoch GossipContext {..} =
  readTVarIO serverList <&> fst

getEpochSTM :: GossipContext -> STM Word32
getEpochSTM GossipContext {..} =
  readTVar serverList <&> fst

getFailedNodes :: GossipContext -> IO [I.ServerNode]
getFailedNodes GossipContext {..} = readTVarIO deadServers <&> Map.elems

getFailedNodesSTM :: GossipContext -> STM [I.ServerNode]
getFailedNodesSTM GossipContext {..} = readTVar deadServers <&> Map.elems

getClusterStatus :: GossipContext -> IO (HM.HashMap Word32 ServerNodeStatus)
getClusterStatus gc@GossipContext {..} = do
  alives <- readTVarIO serverList <&> (map serverInfo . Map.elems . snd)
  deads <- getFailedNodes gc
  isReady <- tryReadMVar clusterReady
  let self = helper (case isReady of Just _  -> NodeStateRunning; Nothing -> NodeStateStarting)
           . fromInternalServerNode
           $ serverSelf
  return $ HM.fromList $
       self
     : map (helper NodeStateRunning . fromInternalServerNode) alives
    ++ map (helper NodeStateDead . fromInternalServerNode) deads
  where
    helper state node@ServerNode{..} =
      (serverNodeId, ServerNodeStatus { serverNodeStatusNode  = Just node
                                      , serverNodeStatusState = EnumPB state})
