{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}

module HStream.Kafka.Server.Handler.Group
  ( -- 19: CreateTopics
    handleFindCoordinator
  , handleJoinGroup
  , handleSyncGroup
  , handleHeartbeat
  , handleLeaveGroup
  , handleListGroups
  , handleDescribeGroups
  ) where

import           HStream.Common.Server.Lookup         (KafkaResource (..),
                                                       lookupKafkaPersist)
import qualified HStream.Kafka.Group.GroupCoordinator as GC
import           HStream.Kafka.Server.Types           (ServerContext (..))
import qualified HStream.Logger                       as Log
import qualified HStream.Server.HStreamApi            as A
import qualified Kafka.Protocol.Message               as K
import qualified Kafka.Protocol.Service               as K

-- FIXME: move to a separated Coordinator module
handleFindCoordinator :: ServerContext -> K.RequestContext -> K.FindCoordinatorRequest -> IO K.FindCoordinatorResponse
handleFindCoordinator ServerContext{..} _ req = do
  A.ServerNode{..} <- lookupKafkaPersist metaHandle gossipContext loadBalanceHashRing scAdvertisedListenersKey (KafkaResGroup req.key)
  Log.info $ "findCoordinator for group:" <> Log.buildString' req.key <> ", result:" <> Log.buildString' serverNodeId
  return $ K.FindCoordinatorResponse 0 (fromIntegral serverNodeId) serverNodeHost (fromIntegral serverNodePort)

handleJoinGroup :: ServerContext -> K.RequestContext -> K.JoinGroupRequest -> IO K.JoinGroupResponse
handleJoinGroup ServerContext{..} = GC.joinGroup scGroupCoordinator

handleSyncGroup :: ServerContext -> K.RequestContext -> K.SyncGroupRequest -> IO K.SyncGroupResponse
handleSyncGroup ServerContext{..} _ = GC.syncGroup scGroupCoordinator

handleHeartbeat :: ServerContext -> K.RequestContext -> K.HeartbeatRequest -> IO K.HeartbeatResponse
handleHeartbeat ServerContext{..} _ = GC.heartbeat scGroupCoordinator

handleLeaveGroup :: ServerContext -> K.RequestContext -> K.LeaveGroupRequest -> IO K.LeaveGroupResponse
handleLeaveGroup ServerContext{..} _ = GC.leaveGroup scGroupCoordinator

handleListGroups :: ServerContext -> K.RequestContext -> K.ListGroupsRequest -> IO K.ListGroupsResponse
handleListGroups ServerContext{..} _ = GC.listGroups scGroupCoordinator

handleDescribeGroups :: ServerContext -> K.RequestContext -> K.DescribeGroupsRequest -> IO K.DescribeGroupsResponse
handleDescribeGroups ServerContext{..} _ = GC.describeGroups scGroupCoordinator
