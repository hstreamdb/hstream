{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}

module HStream.Gossip
  ( GossipContext(..)
  , GossipOpts(..)
  , defaultGossipOpts

  , initGossipContext
  , bootstrap
  , startGossip
  , waitGossipBoot
  , initCluster

  , describeCluster

  , broadcastEvent
  , createEventHandlers
  , getSeenEvents
  , getMemberList
  , getMemberListSTM
  , getMemberListWithEpochSTM
  , getClusterStatus
  , getEpoch
  , getEpochSTM
  , getFailedNodes
  , getFailedNodesSTM
  ) where

import           Control.Concurrent        (tryReadMVar)
import qualified Data.List                 as L
import           Data.Text                 (Text)
import qualified Data.Vector               as V

import           HStream.Common.Types      (fromInternalServerNodeWithKey)
import           HStream.Gossip.Start      (bootstrap, initGossipContext,
                                            startGossip, waitGossipBoot)
import           HStream.Gossip.Types      (GossipContext (..), GossipOpts (..),
                                            defaultGossipOpts)
import           HStream.Gossip.Utils      (broadcastEvent, createEventHandlers,
                                            getClusterStatus, getEpoch,
                                            getEpochSTM, getFailedNodes,
                                            getFailedNodesSTM, getMemberList,
                                            getMemberListSTM,
                                            getMemberListWithEpochSTM,
                                            getSeenEvents, initCluster)
import           HStream.Server.HStreamApi
import           HStream.Utils             (pattern EnumPB)

describeCluster :: GossipContext -> Maybe Text -> IO (V.Vector ServerNode, V.Vector ServerNodeStatus)
describeCluster gc@GossipContext{..} advertisedListenersKey = do
  isReady <- tryReadMVar clusterReady
  self    <- getListeners serverSelf
  alives  <- getMemberList gc >>= fmap  V.concat . mapM getListeners . L.delete serverSelf
  deads   <- getFailedNodes gc >>= fmap V.concat . mapM getListeners
  let self'   = helper (case isReady of Just _  -> NodeStateRunning; Nothing -> NodeStateStarting) <$> self
      alives' = helper NodeStateRunning <$> alives
      deads'  = helper NodeStateDead    <$> deads
  -- TODO : If Cluster is not ready this should return empty
  let nodes = self <> alives
      nodesStatus = self' <> alives' <> deads'
  pure (nodes, nodesStatus)
  where
    getListeners = fromInternalServerNodeWithKey advertisedListenersKey

    helper state node = ServerNodeStatus
      { serverNodeStatusNode  = Just node
      , serverNodeStatusState = EnumPB state
      }
