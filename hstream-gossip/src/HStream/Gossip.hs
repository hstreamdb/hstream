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
  , waitGossipBoot
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

import           HStream.Gossip.Start (bootstrap, initGossipContext,
                                       startGossip, waitGossipBoot)
import           HStream.Gossip.Types (GossipContext (..), GossipOpts (..),
                                       defaultGossipOpts)
import           HStream.Gossip.Utils (broadcastEvent, createEventHandlers,
                                       getClusterStatus, getEpoch, getEpochSTM,
                                       getFailedNodes, getFailedNodesSTM,
                                       getMemberList, getMemberListSTM,
                                       getSeenEvents, initCluster)
