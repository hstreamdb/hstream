{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE DuplicateRecordFields #-}

module HStream.Server.KafkaHandler.GroupCoordinator
  ( -- 19: CreateTopics
    handleFindCoordinatorV0
  , handleJoinGroupV0
  ) where

-- import qualified HStream.Server.Types    as HsTypes

import qualified Kafka.Protocol.Encoding as K
import qualified Kafka.Protocol.Error    as K
import qualified Kafka.Protocol.Message  as K
import qualified Kafka.Protocol.Service  as K
import qualified Data.Text as T
-- import qualified Data.HashMap.Strict as HM
-- import qualified Data.HashSet as HS
import qualified Control.Concurrent as C
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.UUID.V4 as UUID
import qualified Data.UUID as UUID
import Data.Int (Int32)
import qualified Data.IORef as IO
import qualified Data.HashTable.IO as H
import Data.Maybe (isNothing, fromMaybe)
import Control.Monad (when)
import qualified Control.Monad as M
import qualified Data.Vector as V
import qualified Data.ByteString as BS
import qualified HStream.Logger as Log

type HashTable k v = H.BasicHashTable k v
type Assignments = Maybe (K.KaArray K.SyncGroupRequestAssignmentV0)

hashtableGet hashTable key errorCode = H.lookup hashTable key >>= \case
  Nothing -> error "ERROR"
  Just v -> return v

hashtableDeleteAll hashTable = do
  lst <- H.toList hashTable
  M.forM_ lst $ \(key, _) -> H.delete hashTable key

-- handleFindCoordinatorV0
--   :: HsTypes.ServerContext -> K.RequestContext -> K.FindCoordinatorRequestV0 -> IO K.FindCoordinatorResponseV0
handleFindCoordinatorV0 ctx _ K.FindCoordinatorRequestV0{..} = undefined

-- handleJoinGroupV0
--   :: HsTypes.ServerContext -> K.RequestContext -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
handleJoinGroupV0 = joinGroup

data GroupState
  -- Group is preparing to rebalance
  -- 
  -- action: respond to heartbeats with REBALANCE_IN_PROGRESS
  --         respond to sync group with REBALANCE_IN_PROGRESS
  --         remove member on leave group request
  --         park join group requests from new or existing members until all expected members have joined
  --         allow offset commits from previous generation
  --         allow offset fetch requests
  -- transition: some members have joined by the timeout => CompletingRebalance
  --             all members have left the group => Empty
  --             group is removed by partition emigration => Dead
  = PreparingRebalance

  -- Group is awaiting state assignment from the leader
  --
  -- action: respond to heartbeats with REBALANCE_IN_PROGRESS
  --         respond to offset commits with REBALANCE_IN_PROGRESS
  --         park sync group requests from followers until transition to Stable
  --         allow offset fetch requests
  -- transition: sync group with state assignment received from leader => Stable
  --             join group from new member or existing member with updated metadata => PreparingRebalance
  --             leave group from existing member => PreparingRebalance
  --             member failure detected => PreparingRebalance
  --             group is removed by partition emigration => Dead
  | CompletingRebalance

  -- Group is stable
  --
  -- action: respond to member heartbeats normally
  --         respond to sync group from any member with current assignment
  --         respond to join group from followers with matching metadata with current group metadata
  --         allow offset commits from member of current generation
  --         allow offset fetch requests
  -- transition: member failure detected via heartbeat => PreparingRebalance
  --             leave group from existing member => PreparingRebalance
  --             leader join-group received => PreparingRebalance
  --             follower join-group with new metadata => PreparingRebalance
  --             group is removed by partition emigration => Dead
  | Stable

  -- Group has no more members and its metadata is being removed
  -- 
  -- action: respond to join group with UNKNOWN_MEMBER_ID
  --         respond to sync group with UNKNOWN_MEMBER_ID
  --         respond to heartbeat with UNKNOWN_MEMBER_ID
  --         respond to leave group with UNKNOWN_MEMBER_ID
  --         respond to offset commit with UNKNOWN_MEMBER_ID
  --         allow offset fetch requests
  -- transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
  | Dead

  -- Group has no more members, but lingers until all offsets have expired. This state
  -- also represents groups which use Kafka only for offset commits and have no members.
  -- 
  -- action: respond normally to join group from new members
  --         respond to sync group with UNKNOWN_MEMBER_ID
  --         respond to heartbeat with UNKNOWN_MEMBER_ID
  --         respond to leave group with UNKNOWN_MEMBER_ID
  --         respond to offset commit with UNKNOWN_MEMBER_ID
  --         allow offset fetch requests
  -- transition: last offsets removed in periodic expiration task => Dead
  --             join group from a new member => PreparingRebalance
  --             group is removed by partition emigration => Dead
  --             group is removed by expiration => Dead
  | Empty
  deriving (Show, Eq)

data GroupConfig
  = GroupConfig
  {
  }

data Member
  = Member
  { memberId :: T.Text
  , sessionTimeoutMs :: Int32
  , assignment :: IO.IORef BS.ByteString
  }

newMember :: T.Text -> Int32 -> IO Member
newMember memberId sessionTimeoutMs = do
  assignment <- IO.newIORef BS.empty
  return $ Member {..}


data Group
  = Group
  { lock               :: C.MVar ()
  , groupId :: T.Text
  , groupGenerationId  :: IO.IORef Int32
  , state              :: IO.IORef GroupState
  , config             :: GroupConfig
  , leader             :: IO.IORef (Maybe T.Text)
  , members            :: HashTable T.Text Member
  -- , pendingMembers     :: HashTable T.Text ()
  , delayedJoinResponses :: HashTable T.Text (C.MVar K.JoinGroupResponseV0)
  -- , pendingSyncMembers :: HashTable T.Text ()
  -- , newMemberAdded     :: IO.IORef Bool
  , delayedRebalance :: IO.IORef (Maybe C.ThreadId)

  , delayedSyncResponses :: HashTable T.Text (C.MVar K.SyncGroupResponseV0)
  }

newGroup :: T.Text -> IO Group
newGroup group = do
  lock <- C.newMVar ()
  state <- IO.newIORef Empty
  groupGenerationId <- IO.newIORef 0
  leader <- IO.newIORef Nothing
  members <- H.new
  -- pendingMembers <- H.new
  delayedJoinResponses <- H.new
  -- pendingSyncMembers <- H.new
  -- newMemberAdded <- IO.newIORef False
  delayedRebalance <- IO.newIORef Nothing

  delayedSyncResponses <- H.new

  return $ Group
    { lock = lock
    , groupId = group
    , groupGenerationId = groupGenerationId
    , state = state
    , config = GroupConfig
    , leader = leader
    , members = members
    -- , pendingMembers = pendingMembers
    , delayedJoinResponses = delayedJoinResponses
    -- , pendingSyncMembers = pendingSyncMembers
    -- , newMemberAdded = newMemberAdded
    , delayedRebalance = delayedRebalance

    , delayedSyncResponses = delayedSyncResponses
    }

data GroupCoordinator = GroupCoordinator
  { groups :: C.MVar (HashTable T.Text Group)
  }

joinGroup :: GroupCoordinator -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
joinGroup coordinator@GroupCoordinator{..} req@K.JoinGroupRequestV0{..} = do
  group@Group{delayedJoinResponses=delayedJoinResponses} <- getOrMaybeCreateGroup coordinator groupId memberId
  delayedResponse <- C.newEmptyMVar
  C.withMVar (lock group) $ \_ -> do
    -- accepted <- acceptJoiningMember group memberId
    -- M.unless accepted $ error "TODO: GROUP MAX SIZE"
    -- TODO: check state
    IO.readIORef group.state >>= \s -> do
      if s `elem` [CompletingRebalance, Stable] then resetGroup group
      else if s `elem` [CompletingRebalance, Stable] then pure ()
      else error "TODO: UNKNOWN_NUMBER_ID"
    -- TODO: check memberId
    newMemberId <- if T.null memberId
      then doNewMemberJoinGoup group req
      else doCurrentMemeberJoinGroup group req
    H.insert delayedJoinResponses newMemberId delayedResponse
  C.takeMVar delayedResponse

getOrMaybeCreateGroup :: GroupCoordinator -> T.Text -> T.Text -> IO Group
getOrMaybeCreateGroup GroupCoordinator{..} groupId memberId = do
  C.withMVar groups $ \gs -> do
    H.lookup gs groupId >>= \case
      Nothing -> if T.null memberId
        then do
          ng <- newGroup groupId
          H.insert gs groupId ng
          return ng
        else error "TODO: INVALID MEMBER ID"
      Just g -> return g

getGroup :: GroupCoordinator -> T.Text -> IO Group
getGroup GroupCoordinator{..} groupId = do
  C.withMVar groups $ \gs -> do
    H.lookup gs groupId >>= \case
      Nothing -> error "TODO: INVALID GROUP"
      Just g -> return g

-- TODO: check
prepareJoinGroup :: Group -> T.Text -> IO Bool
prepareJoinGroup Group{..} memberId = pure True

resetGroup :: Group -> IO ()
resetGroup group@Group{..} = do
  cancelDelayedSyncResponses group 
  IO.writeIORef leader Nothing
  hashtableDeleteAll members

cancelDelayedSyncResponses :: Group -> IO ()
cancelDelayedSyncResponses Group{..} = do
  lst <- H.toList delayedSyncResponses
  M.forM_ lst $ \(memberId, delayed) -> do
    Log.info $ "cancel delayed sync response for " <> Log.buildString (T.unpack memberId)
    C.putMVar delayed $ K.SyncGroupResponseV0 K.REASSIGNMENT_IN_PROGRESS BS.empty
    H.delete delayedSyncResponses memberId

doNewMemberJoinGoup :: Group -> K.JoinGroupRequestV0 -> IO T.Text
doNewMemberJoinGoup group req = do
  -- TODO: check group state
  -- TODO: check protocol

  newMemberId <- generateMemberId
  doDynamicNewMemberJoinGroup group req newMemberId
  return newMemberId

-- TODO: kafka memberId format: clientId(from request context)/group_instance_id + "-" + UUID
generateMemberId :: IO T.Text
generateMemberId = UUID.toText <$> UUID.nextRandom

doCurrentMemeberJoinGroup :: Group -> K.JoinGroupRequestV0 -> IO T.Text
doCurrentMemeberJoinGroup group req = do
  doDynamicNewMemberJoinGroup group req req.memberId
  return req.memberId

doDynamicNewMemberJoinGroup :: Group -> K.JoinGroupRequestV0 -> T.Text -> IO ()
doDynamicNewMemberJoinGroup group req newMemberId = do
  addMemberAndRebalance group req newMemberId

addMemberAndRebalance :: Group -> K.JoinGroupRequestV0 -> T.Text -> IO ()
addMemberAndRebalance group K.JoinGroupRequestV0{..} newMemberId = do
  member <- newMember newMemberId sessionTimeoutMs
  addMember group member
  -- TODO: check state
  prepareRebalance group
  undefined

prepareRebalance :: Group -> IO ()
prepareRebalance group@Group{..} = do
  -- TODO: check state CompletingRebalance
  -- TODO: remoe sync expiration
  -- isEmptyState <- (Empty ==) <$> IO.readIORef state

  -- setup delayed rebalance if delayedRebalance is Nothing
  -- TODO: configurable rebalanceDelayMs
  IO.readIORef delayedRebalance >>= \case
    Nothing -> do
      delayed <- makeDelayedRebalance group 5000
      IO.writeIORef delayedRebalance (Just delayed)
      IO.writeIORef state PreparingRebalance
    _ -> pure ()

-- TODO: dynamically delay
makeDelayedRebalance :: Group -> Int32 -> IO C.ThreadId
makeDelayedRebalance group rebalanceDelayMs = C.forkIO $ do
  C.threadDelay (1000 * fromIntegral rebalanceDelayMs)
  rebalance group

rebalance :: Group -> IO ()
rebalance Group{..} = C.withMVar lock $ \() -> do
  Log.info "rebalancing is starting"
  (Just leaderMemberId) <- IO.readIORef leader

  -- next generation id
  nextGenerationId <- IO.atomicModifyIORef' groupGenerationId (\ggid -> (ggid + 1, ggid + 1))
  Log.info $ "next generation id:" <> Log.buildString (show nextGenerationId)
    <> ", leader:" <> Log.buildString (T.unpack leaderMemberId)

  delayedJoinResponseList <- H.toList delayedJoinResponses
  let membersInResponse = map (\(m, _) -> K.JoinGroupResponseMemberV0 m BS.empty) delayedJoinResponseList
  
  -- response all delayedJoinResponses
  M.forM_ delayedJoinResponseList $ \(memberId, delayed) -> do
    -- TODO: leader vs. normal member
    -- TODO: protocol name
    -- TODO: member metadata
    let resp = K.JoinGroupResponseV0 {
        errorCode = 0
      , generationId = nextGenerationId
      , protocolName = ""
      , leader = leaderMemberId
      , memberId = memberId
      , members = K.KaArray (Just $ V.fromList membersInResponse)
      }
    C.putMVar delayed resp
    H.delete delayedJoinResponses memberId
  IO.writeIORef state CompletingRebalance
  Log.info "state changed: PreparingRebalance -> CompletingRebalance"
  IO.writeIORef delayedRebalance Nothing
  Log.info "rebalancing is finished"

addMember :: Group -> Member -> IO ()
addMember Group{..} member@Member{..} = do
  -- leaderIsEmpty <- IO.readIORef leader
  IO.readIORef leader >>= \case
    Nothing -> IO.writeIORef leader (Just memberId)
    _ -> pure ()
  H.insert members memberId member

syncGroup :: GroupCoordinator -> K.SyncGroupRequestV0 -> IO K.SyncGroupResponseV0
syncGroup coordinator@GroupCoordinator{..} req@K.SyncGroupRequestV0{..} = do
  group <- getGroup coordinator groupId
  delayed <- C.newEmptyMVar
  C.withMVar (group.lock) $ \() -> do
    -- check member id
    member <- hashtableGet group.members memberId K.UNKNOWN_MEMBER_ID

    -- TODO: check generation id
    IO.readIORef group.state >>= \case
      CompletingRebalance -> doSyncGroup group req delayed
      Stable -> do
        assignment <- IO.readIORef member.assignment 
        C.putMVar delayed (K.SyncGroupResponseV0 0 assignment)
      _ -> error "INVALID STATE"
  C.readMVar delayed

doSyncGroup :: Group -> K.SyncGroupRequestV0 -> C.MVar K.SyncGroupResponseV0 -> IO ()
doSyncGroup group@Group{..} req@K.SyncGroupRequestV0{memberId=memberId, assignments=assignments} delayedResponse = do
  -- check assignment
  when (isNothing (K.unKaArray req.assignments)) $ error "TODO: INVALID ASSIGNEMNTS"

  -- set delayed response
  H.lookup delayedSyncResponses memberId >>= \case
    Nothing -> H.insert delayedSyncResponses memberId delayedResponse
    _ -> error "TODO: DUPLICATED SYNC GROUP"

  -- set assignments if this req from leader
  (Just leaderMemberId) <- IO.readIORef leader
  when (memberId == leaderMemberId) $ setAndPropagateAssignment group req

  -- set state
  IO.writeIORef state Stable

setAndPropagateAssignment :: Group -> K.SyncGroupRequestV0 -> IO ()
setAndPropagateAssignment Group{..} req = do
  -- set assignments
  let assignments = fromMaybe V.empty (K.unKaArray req.assignments)
  V.forM_ assignments $ \assignment -> do
    Just member <- H.lookup members assignment.memberId
    -- set assignments
    IO.writeIORef member.assignment assignment.assignment
    -- propagate assignments
    H.lookup delayedSyncResponses assignment.memberId >>= \case
      Nothing -> pure ()
      Just delayed -> do
        C.putMVar delayed (K.SyncGroupResponseV0 0 assignment.assignment)
        H.delete delayedJoinResponses assignment.memberId

leaveGroup :: GroupCoordinator -> K.LeaveGroupRequestV0 -> IO K.LeaveGroupResponseV0
leaveGroup group req = do
  -- check
  undefined

heartbeat :: GroupCoordinator -> K.HeartbeatRequestV0 -> IO K.HeartbeatResponseV0
heartbeat group req = do
  -- TODO: check generation id
  -- TODO: check state: rebalance
  return $ K.HeartbeatResponseV0 0

