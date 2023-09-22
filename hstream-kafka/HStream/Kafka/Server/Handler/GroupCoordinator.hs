{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.KafkaHandler.GroupCoordinator
  ( -- 19: CreateTopics
    handleFindCoordinatorV0
  ) where

import qualified HStream.Server.Types    as HsTypes

import qualified Kafka.Protocol.Encoding as K
import qualified Kafka.Protocol.Error    as K
import qualified Kafka.Protocol.Message  as K
import qualified Kafka.Protocol.Service  as K
import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import qualified Control.Concurrent as C

handleFindCoordinatorV0
  :: HsTypes.ServerContext -> K.RequestContext -> K.FindCoordinatorRequestV0 -> IO K.FindCoordinatorResponseV0
handleFindCoordinatorV0 ctx _ K.FindCoordinatorRequestV0{..} = undefined

handleJoinGroupV0
  :: HsTypes.ServerContext -> K.RequestContext -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
handleJoinGroupV0 ctx _ K.JoinGroupRequestV0{..} = undefined

data GroupState
  = PreparingRebalance
  | CompletingRebalance
  | Stable
  | Dead
  | Empty

data GroupConfig
  = GroupConfig
  {
  }

data Member
 = Member {}

data Group
  = Group
  { lock               :: C.MVar ()
  , id :: T.Text
  , state              :: GroupState
  , config             :: GroupConfig
  , leader             :: Maybe T.Text
  , members            :: HM.HashMap T.Text Member
  , pendingMembers     :: HS.HashSet T.Text
  , pendingSyncMembers :: HS.HashSet T.Text
  , newMemberAdded     :: Bool
  }

newGroup :: T.Text -> GroupState -> IO Group
newGroup group state = do
  lock <- C.newMVar ()
  return $ Group
    { lock = lock
    , id = group
    , state = state
    , config = GroupConfig
    , leader = Nothing
    , members = HM.empty
    , pendingMembers = HS.empty
    , pendingSyncMembers = HS.empty
    , newMemberAdded = False
    }

data GroupManager
  = GroupManager {
    groups :: C.MVar (HM.HashMap T.Text Group)
  }

data GroupCoordinator = GroupCoordinator
  { groupManager :: GroupManager
  }

joinGroup :: GroupCoordinator -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
joinGroup GroupCoordinator{..} req@K.JoinGroupRequestV0{..} = do
  group <- getOrMaybeCreateGroup groupManager groupId memberId
  C.withMVar (lock group) $ \_ -> do
    accepted <- acceptJoiningMember group memberId
    case (accepted, T.null memberId) of
      (False, _) -> do
        error "TODO: GROUP MAX SIZE"
      (_, True) -> doNewMemberJoinGoup group req
      (_, False) -> doCurrentMemeberJoinGroup group req

getOrMaybeCreateGroup :: GroupManager -> T.Text -> T.Text -> IO Group
getOrMaybeCreateGroup GroupManager{..} groupId memberId = do
  C.modifyMVar groups $ \gs -> do
    case HM.lookup groupId gs of
      Nothing -> if T.null memberId
        then newGroup groupId Empty >>= \ng -> return $ (HM.insert groupId ng gs, ng)
        else error "TODO: error"
      Just g -> return (gs, g)

acceptJoiningMember :: Group -> T.Text -> IO Bool
acceptJoiningMember Group{..} memberId = undefined

doNewMemberJoinGoup group req = undefined

doCurrentMemeberJoinGroup group req = undefined

