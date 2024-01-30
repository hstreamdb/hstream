{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}

module HStream.Kafka.Server.Handler.Group
  ( handleJoinGroup
  , handleSyncGroup
  , handleHeartbeat
  , handleLeaveGroup
  , handleListGroups
  , handleDescribeGroups
  ) where

import qualified HStream.Kafka.Group.GroupCoordinator as GC
import           HStream.Kafka.Server.Types           (ServerContext (..))
import qualified Kafka.Protocol.Message               as K
import qualified Kafka.Protocol.Service               as K

handleJoinGroup :: ServerContext -> K.RequestContext -> K.JoinGroupRequest -> IO K.JoinGroupResponse
handleJoinGroup ServerContext{..} = GC.joinGroup scGroupCoordinator

handleSyncGroup :: ServerContext -> K.RequestContext -> K.SyncGroupRequest -> IO K.SyncGroupResponse
handleSyncGroup ServerContext{..} _ = GC.syncGroup scGroupCoordinator

handleHeartbeat :: ServerContext -> K.RequestContext -> K.HeartbeatRequest -> IO K.HeartbeatResponse
handleHeartbeat ServerContext{..} _ r = GC.heartbeat scGroupCoordinator r

handleLeaveGroup :: ServerContext -> K.RequestContext -> K.LeaveGroupRequest -> IO K.LeaveGroupResponse
handleLeaveGroup ServerContext{..} _ = GC.leaveGroup scGroupCoordinator

handleListGroups :: ServerContext -> K.RequestContext -> K.ListGroupsRequest -> IO K.ListGroupsResponse
handleListGroups ServerContext{..} _ = GC.listGroups scGroupCoordinator

handleDescribeGroups :: ServerContext -> K.RequestContext -> K.DescribeGroupsRequest -> IO K.DescribeGroupsResponse
handleDescribeGroups ServerContext{..} _ = GC.describeGroups scGroupCoordinator
