{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}

module HStream.Kafka.Server.Handler.Group
  ( -- 19: CreateTopics
    handleFindCoordinatorV0
  , handleJoinGroupV0
  , handleSyncGroupV0
  , handleHeartbeatV0
  , handleLeaveGroupV0
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
handleFindCoordinatorV0 :: ServerContext -> K.RequestContext -> K.FindCoordinatorRequestV0 -> IO K.FindCoordinatorResponseV0
handleFindCoordinatorV0 ServerContext{..} _ req = do
  A.ServerNode{..} <- lookupKafkaPersist metaHandle gossipContext loadBalanceHashRing scAdvertisedListenersKey (KafkaResGroup req.key)
  Log.info $ "findCoordinator for group:" <> Log.buildString' req.key <> ", result:" <> Log.buildString' serverNodeId
  return $ K.FindCoordinatorResponseV0 0 (fromIntegral serverNodeId) serverNodeHost (fromIntegral serverNodePort)

handleJoinGroupV0 :: ServerContext -> K.RequestContext -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
handleJoinGroupV0 ServerContext{..} _ = GC.joinGroup scGroupCoordinator scLDClient (fromIntegral serverID)

handleSyncGroupV0 :: ServerContext -> K.RequestContext -> K.SyncGroupRequestV0 -> IO K.SyncGroupResponseV0
handleSyncGroupV0 ServerContext{..} _ = GC.syncGroup scGroupCoordinator

handleHeartbeatV0 :: ServerContext -> K.RequestContext -> K.HeartbeatRequestV0 -> IO K.HeartbeatResponseV0
handleHeartbeatV0 ServerContext{..} _ = GC.heartbeat scGroupCoordinator

handleLeaveGroupV0 :: ServerContext -> K.RequestContext -> K.LeaveGroupRequestV0 -> IO K.LeaveGroupResponseV0
handleLeaveGroupV0 ServerContext{..} _ = GC.leaveGroup scGroupCoordinator
