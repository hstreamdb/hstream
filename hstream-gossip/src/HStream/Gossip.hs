{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Gossip
  ( GossipContext(..)
  , GossipOpts(..)
  , defaultGossipOpts

  , initGossipContext
  , bootstrap
  , startGossip

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
                                                 GossipOpts (..), SeenEvents,
                                                 ServerStatus (..),
                                                 defaultGossipOpts)
import           HStream.Server.HStreamApi      (NodeState (..),
                                                 ServerNode (..),
                                                 ServerNodeStatus (..))
import qualified HStream.Server.HStreamInternal as I
import           HStream.Utils                  (pattern EnumPB)

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
getClusterStatus gc = do
  (alives, deads) <-  (,) <$> getMemberList gc <*> getFailedNodes gc
  return $ HM.fromList $
       map (helper NodeStateRunning . fromInternalServerNode) alives
    ++ map (helper NodeStateDead    . fromInternalServerNode) deads
  where
    helper state node@ServerNode{..} =
      (serverNodeId, ServerNodeStatus { serverNodeStatusNode  = Just node
                                      , serverNodeStatusState = EnumPB state})
