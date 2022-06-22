{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Gossip
  ( initGossipContext
  , bootstrap
  , startGossip

  , broadcastEvent
  , createEventHandlers
  , getSeenEvents
  , getMemberList
  , getMemberListSTM
  , getClusterStatus
  ) where

import           Control.Concurrent.STM    (STM, atomically, readTVar,
                                            readTVarIO, writeTQueue)
import           Data.Functor              ((<&>))
import qualified Data.HashMap.Strict       as HM
import qualified Data.Map.Strict           as Map
import           Data.Word                 (Word32)

import           HStream.Gossip.Start      (bootstrap, initGossipContext,
                                            startGossip)
import           HStream.Gossip.Types      (EventHandler, EventMessage,
                                            EventName, GossipContext (..),
                                            SeenEvents,
                                            ServerStatus (serverInfo))
import           HStream.Gossip.Utils      (fromServerNodeInternal)
import           HStream.Server.HStreamApi (NodeState (..), ServerNode (..),
                                            ServerNodeStatus (..))
import           HStream.Utils             (pattern EnumPB)

broadcastEvent :: GossipContext -> EventMessage -> IO ()
broadcastEvent GossipContext {..} = atomically . writeTQueue eventPool

createEventHandlers :: [(EventName, EventHandler)] -> Map.Map EventName EventHandler
createEventHandlers = Map.fromList

getSeenEvents :: GossipContext -> IO SeenEvents
getSeenEvents GossipContext {..} = readTVarIO seenEvents

getMemberList :: GossipContext -> IO [ServerNode]
getMemberList GossipContext {..} =
  readTVarIO serverList <&> ((:) (fromServerNodeInternal serverSelf) . map (fromServerNodeInternal . serverInfo) . Map.elems)

getMemberListSTM :: GossipContext -> STM [ServerNode]
getMemberListSTM GossipContext {..} =
  readTVar serverList <&> ((:) (fromServerNodeInternal serverSelf) . map (fromServerNodeInternal . serverInfo) . Map.elems)

getClusterStatus :: GossipContext -> IO (HM.HashMap Word32 ServerNodeStatus)
getClusterStatus gc = do
  getMemberList gc <&> HM.fromList . map helper
  where
    helper node@ServerNode{..} =
      (serverNodeId, ServerNodeStatus { serverNodeStatusNode  = Just node
                                      , serverNodeStatusState = EnumPB NodeStateRunning})
