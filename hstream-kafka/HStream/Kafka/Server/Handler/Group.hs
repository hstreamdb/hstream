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

import qualified Control.Exception                     as E
import           Control.Monad
import qualified Data.Vector                           as V

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Authorizer.Class
import qualified HStream.Kafka.Common.KafkaException   as K
import           HStream.Kafka.Common.Resource
import qualified HStream.Kafka.Common.Utils            as Utils
import qualified HStream.Kafka.Group.Group             as G
import qualified HStream.Kafka.Group.GroupCoordinator  as GC
import           HStream.Kafka.Server.Types            (ServerContext (..))
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Error                  as K
import qualified Kafka.Protocol.Message                as K
import qualified Kafka.Protocol.Service                as K

handleJoinGroup :: ServerContext
                -> K.RequestContext
                -> K.JoinGroupRequest
                -> IO K.JoinGroupResponse
handleJoinGroup ServerContext{..} reqCtx req = E.handle (\(K.ErrorCodeException code) -> return (makeErrorResponse code)) $ do
  -- [ACL] check [READ GROUP]
  simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP req.groupId AclOp_READ >>= \case
    False -> return $ makeErrorResponse K.GROUP_AUTHORIZATION_FAILED
    True  -> do
      -- get or create group
      group <- GC.getOrMaybeCreateGroup scGroupCoordinator req.groupId req.memberId
      -- join group
      G.joinGroup group reqCtx req
  where
    -- FIXME: Hard-coded constants
    makeErrorResponse code = K.JoinGroupResponse {
      errorCode      = code
    , generationId   = -1
    , protocolName   = ""
    , leader         = ""
    -- FIXME: memberId for error response should be `""`?
    --        see org.apache.kafka.common.requests.getErrorResponse
    , memberId       = req.memberId
    , members        = K.NonNullKaArray V.empty
    , throttleTimeMs = 0
    }

handleSyncGroup :: ServerContext
                -> K.RequestContext
                -> K.SyncGroupRequest
                -> IO K.SyncGroupResponse
handleSyncGroup ServerContext{..} reqCtx req = E.handle (\(K.ErrorCodeException code) -> return (makeErrorResponse code)) $ do
  -- [ACL] check [READ GROUP]
  simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP req.groupId AclOp_READ >>= \case
    False -> return $ makeErrorResponse K.GROUP_AUTHORIZATION_FAILED
    True  -> do
      group <- GC.getGroup scGroupCoordinator req.groupId
      G.syncGroup group req
  where
    -- FIXME: Hard-coded constants
    makeErrorResponse code = K.SyncGroupResponse {
      errorCode      = code
    , assignment     = ""
    , throttleTimeMs = 0
    }

handleHeartbeat :: ServerContext
                -> K.RequestContext
                -> K.HeartbeatRequest
                -> IO K.HeartbeatResponse
handleHeartbeat ServerContext{..} reqCtx req = E.handle (\(K.ErrorCodeException code) -> return (makeErrorResponse code)) $ do
  -- [ACL] check [READ GROUP]
  simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP req.groupId AclOp_READ >>= \case
    False -> return $ makeErrorResponse K.GROUP_AUTHORIZATION_FAILED
    True  -> do
      group <- GC.getGroup scGroupCoordinator req.groupId
      G.heartbeat group req
  where
    -- FIXME: Hard-coded constants
    makeErrorResponse code = K.HeartbeatResponse {
      errorCode      = code
    , throttleTimeMs = 0
    }

handleLeaveGroup :: ServerContext
                 -> K.RequestContext
                 -> K.LeaveGroupRequest
                 -> IO K.LeaveGroupResponse
handleLeaveGroup ServerContext{..} reqCtx req = E.handle (\(K.ErrorCodeException code) -> return (makeErrorResponse code)) $ do
  -- [ACL] check [READ GROUP]
  simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP req.groupId AclOp_READ >>= \case
    False -> return $ makeErrorResponse K.GROUP_AUTHORIZATION_FAILED
    True  -> do
      group <- GC.getGroup scGroupCoordinator req.groupId
      G.leaveGroup group req
  where
    -- FIXME: Hard-coded constants
    makeErrorResponse code = K.LeaveGroupResponse {
      errorCode      = code
    , throttleTimeMs = 0
    }

-- FIXME: This handler does not handle any Kafka ErrorCodeException.
--        Is this proper?
handleListGroups :: ServerContext -> K.RequestContext -> K.ListGroupsRequest -> IO K.ListGroupsResponse
handleListGroups ServerContext{..} reqCtx _ = do
  -- TODO: check [DESCRIBE CLUSTER] first. If authzed, return all groups.
  -- Note: Difference from handlers above:
  --       Check [DESCRIBE GROUP] for each group, then return authzed ones
  --       only. Unauthzed groups will not cause a UNAUTHORIZED error.
  gs <- GC.getAllGroups scGroupCoordinator
  listedGroups  <- mapM G.overview gs
  authzedGroups <-
    filterM (\listedGroup ->
               simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP listedGroup.groupId AclOp_DESCRIBE
            ) listedGroups
  -- FIXME: hard-coded constants
  return $ K.ListGroupsResponse
         { errorCode      = K.NONE
         , groups         = Utils.listToKaArray authzedGroups
         , throttleTimeMs = 0
         }

-- FIXME: This handler does not handle any Kafka ErrorCodeException.
--        Is this proper?
handleDescribeGroups :: ServerContext
                     -> K.RequestContext
                     -> K.DescribeGroupsRequest
                     -> IO K.DescribeGroupsResponse
handleDescribeGroups ServerContext{..} reqCtx req = do
  groups_m <- GC.getGroups scGroupCoordinator (Utils.kaArrayToList req.groups)
  describedGroups <- forM groups_m $ \(gid, group_m) ->
    -- [ACL] for each group id, check [DESCRIBE GROUP]
    simpleAuthorize (toAuthorizableReqCtx reqCtx) authorizer Res_GROUP gid AclOp_DESCRIBE >>= \case
      False -> return $ makeErrorGroup gid K.GROUP_AUTHORIZATION_FAILED
      True  -> case group_m of
        Nothing    -> return $ makeErrorGroup gid K.GROUP_ID_NOT_FOUND
        Just group -> G.describe group
  -- FIXME: hard-coded constants
  return $ K.DescribeGroupsResponse
         { groups         = Utils.listToKaArray describedGroups
         , throttleTimeMs = 0
         }
  where
    -- FIXME: hard-coded constants
    makeErrorGroup gid code = K.DescribedGroup {
      protocolData = ""
    , groupState   = ""
    , errorCode    = code
    , members      = Utils.listToKaArray []
    , groupId      = gid
    , protocolType = ""
    }
