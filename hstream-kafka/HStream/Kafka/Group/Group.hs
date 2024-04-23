{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.Kafka.Group.Group where

import qualified Control.Concurrent                     as C
import           Control.Exception                      (throw)
import           Control.Monad                          (when)
import qualified Control.Monad                          as M
import qualified Data.ByteString                        as BS
import qualified Data.HashTable.IO                      as H
import           Data.Int                               (Int32)
import qualified Data.IORef                             as IO
import qualified Data.List                              as List
import qualified Data.Map                               as Map
import           Data.Maybe                             (fromMaybe, isJust,
                                                         listToMaybe)
import qualified Data.Set                               as Set
import qualified Data.Text                              as T
import qualified Data.UUID                              as UUID
import qualified Data.UUID.V4                           as UUID
import qualified Data.Vector                            as V
import qualified HStream.Base.Time                      as Time
import qualified HStream.Common.Server.MetaData         as CM
import           HStream.Kafka.Common.KafkaException    (ErrorCodeException (ErrorCodeException))
import qualified HStream.Kafka.Common.Utils             as Utils
import           HStream.Kafka.Group.GroupOffsetManager (GroupOffsetManager)
import qualified HStream.Kafka.Group.GroupOffsetManager as GOM
import           HStream.Kafka.Group.Member
import qualified HStream.Logger                         as Log
import qualified HStream.MetaStore.Types                as Meta
import qualified Kafka.Protocol.Encoding                as K
import qualified Kafka.Protocol.Error                   as K
import qualified Kafka.Protocol.Message                 as K
import qualified Kafka.Protocol.Service                 as K

type HashTable k v = H.BasicHashTable k v

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

data Group = Group
  { lock                 :: C.MVar ()
  , groupId              :: T.Text
  , groupGenerationId    :: IO.IORef Int32
  , state                :: IO.IORef GroupState
  , groupConfig          :: GroupConfig
  , leader               :: IO.IORef (Maybe T.Text)
  , members              :: HashTable T.Text Member
  -- , pendingMembers     :: HashTable T.Text ()
  , delayedJoinResponses :: HashTable T.Text (C.MVar K.JoinGroupResponse)
  -- , pendingSyncMembers :: HashTable T.Text ()
  -- , newMemberAdded     :: IO.IORef Bool
  , delayedRebalance     :: IO.IORef (Maybe C.ThreadId)
  , delayedSync          :: IO.IORef (Maybe C.ThreadId)

  , delayedSyncResponses :: HashTable T.Text (C.MVar K.SyncGroupResponse)

  , metadataManager      :: GroupOffsetManager

  -- protocols
  , protocolType         :: IO.IORef (Maybe T.Text)
  , protocolName         :: IO.IORef (Maybe T.Text)
  , supportedProtocols   :: IO.IORef (Set.Set T.Text)

  -- metastore
  , metaHandle           :: Meta.MetaHandle

  --
  , storedMetadata       :: IO.IORef Bool
  }

data GroupConfig = GroupConfig
  { groupInitialRebalanceDelay :: Int
  } deriving (Show)

newGroup :: T.Text -> GroupOffsetManager -> Meta.MetaHandle -> GroupConfig -> IO Group
newGroup group metadataManager metaHandle config = do
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
  delayedSync <- IO.newIORef Nothing

  delayedSyncResponses <- H.new

  protocolType <- IO.newIORef Nothing
  protocolName <- IO.newIORef Nothing
  supportedProtocols <- IO.newIORef Set.empty

  storedMetadata <- IO.newIORef False

  return $ Group
    { lock = lock
    , groupId = group
    , groupGenerationId = groupGenerationId
    , state = state
    , groupConfig = config
    , leader = leader
    -- all members
    , members = members
    -- , pendingMembers = pendingMembers
    , delayedJoinResponses = delayedJoinResponses
    -- , pendingSyncMembers = pendingSyncMembers
    -- , newMemberAdded = newMemberAdded
    , delayedRebalance = delayedRebalance
    , delayedSync = delayedSync

    , delayedSyncResponses = delayedSyncResponses

    , metadataManager = metadataManager

    , protocolType = protocolType
    , protocolName = protocolName
    , supportedProtocols = supportedProtocols

    , metaHandle = metaHandle

    , storedMetadata = storedMetadata
    }

newGroupFromValue
  :: CM.GroupMetadataValue
  -> GroupOffsetManager
  -> Meta.MetaHandle
  -> GroupConfig
  -> IO Group
newGroupFromValue value metadataManager metaHandle config = do
  lock <- C.newMVar ()

  state <- IO.newIORef (if V.null value.members then Empty else Stable)

  groupGenerationId <- IO.newIORef value.generationId
  leader <- IO.newIORef value.leader

  members <- H.new

  delayedJoinResponses <- H.new
  delayedRebalance <- IO.newIORef Nothing
  delayedSync <- IO.newIORef Nothing
  delayedSyncResponses <- H.new

  protocolType <- IO.newIORef (Just value.protocolType)
  protocolName <- IO.newIORef value.prototcolName

  supportedProtocols <- IO.newIORef Set.empty

  storedMetadata <- IO.newIORef True

  let group = Group
        { lock = lock
        , groupId = value.groupId
        , groupGenerationId = groupGenerationId
        , state = state
        , groupConfig = config
        , leader = leader
        -- all members
        , members = members
        -- , pendingMembers = pendingMembers
        , delayedJoinResponses = delayedJoinResponses
        -- , pendingSyncMembers = pendingSyncMembers
        -- , newMemberAdded = newMemberAdded
        , delayedRebalance = delayedRebalance
        , delayedSync = delayedSync

        , delayedSyncResponses = delayedSyncResponses

        , metadataManager = metadataManager

        , protocolType = protocolType
        , protocolName = protocolName
        , supportedProtocols = supportedProtocols

        , metaHandle = metaHandle

        , storedMetadata = storedMetadata
        }

  -- add members
  M.forM_ (value.members) $ \mValue -> do
    member <- newMemberFromValue value mValue
    addMember group member Nothing

  -- setup delayedCheckHearts for all members
  setupDelayedCheckHeartbeat group

  return group

------------------------------------------------------------------------

joinGroup :: Group -> K.RequestContext -> K.JoinGroupRequest -> IO K.JoinGroupResponse
joinGroup group@Group{..} reqCtx req = do
  -- delayed response(join barrier)
  Log.info $ "received joinGroup request:" <> Log.buildString' req
  delayedResponse <- C.newEmptyMVar
  C.withMVar lock $ \_ -> do
    -- TODO: GROUP MAX SIZE

    checkSupportedProtocols group req

    -- check state
    IO.readIORef group.state >>= \case
      Dead -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      _ -> pure ()

    if T.null req.memberId
      then doNewMemberJoinGoup group reqCtx req delayedResponse
      else doCurrentMemeberJoinGroup group req delayedResponse

  -- waiting other consumers
  resp <- C.takeMVar delayedResponse
  Log.info $ "joinGroup: received delayed response:" <> Log.buildString' resp
    <> "group:" <> Log.build groupId
  return resp

checkSupportedProtocols :: Group -> K.JoinGroupRequest -> IO ()
checkSupportedProtocols Group{..} req = do
  IO.readIORef state >>= \case
    Empty -> do
      M.when (T.null req.protocolType) $ do
        throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
      M.when (V.null (Utils.kaArrayToVector req.protocols)) $ do
        throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
    _ -> do
      Utils.unlessIORefEq protocolType (Just req.protocolType) $ do
        throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
      ps <- IO.readIORef supportedProtocols
      let refinedRequestProtocols = (plainProtocols (refineProtocols req.protocols))
      M.when (Set.null (Set.intersection ps refinedRequestProtocols)) $ do
        throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)

cancelDelayedSyncResponses :: Group -> T.Text -> IO ()
cancelDelayedSyncResponses Group{..} reason = do
  lst <- H.toList delayedSyncResponses
  M.forM_ lst $ \(memberId, delayed) -> do
    Log.info $ "cancel delayed sync response for " <> Log.buildString' memberId
      <> ", group:" <> Log.build groupId
      <> ", reason:" <> Log.build reason
    _ <- C.tryPutMVar delayed $ K.SyncGroupResponse {throttleTimeMs=0, errorCode=K.REBALANCE_IN_PROGRESS, assignment=BS.empty}
    H.delete delayedSyncResponses memberId

cancelDelayedSync :: Group -> IO ()
cancelDelayedSync Group{..} = do
  IO.readIORef delayedSync >>= \case
    Nothing -> pure ()
    Just delayed -> do
      C.killThread delayed
      IO.atomicWriteIORef delayedSync Nothing

doNewMemberJoinGoup :: Group -> K.RequestContext -> K.JoinGroupRequest -> C.MVar K.JoinGroupResponse -> IO ()
doNewMemberJoinGoup group reqCtx req delayedResponse = do
  newMemberId <- generateMemberId reqCtx
  Log.info $ "generated member id:" <> Log.buildString' newMemberId
    <> " for group:" <> Log.build group.groupId
  doDynamicNewMemberJoinGroup group reqCtx req newMemberId delayedResponse

generateMemberId :: K.RequestContext -> IO T.Text
generateMemberId reqCtx = do
  (fromMaybe "" (M.join reqCtx.clientId) <>) . ("-" <>) . UUID.toText <$> UUID.nextRandom

doCurrentMemeberJoinGroup :: Group -> K.JoinGroupRequest -> C.MVar K.JoinGroupResponse -> IO ()
doCurrentMemeberJoinGroup group req delayedResponse = do
  member <- getMember group req.memberId
  IO.readIORef group.state >>= \case
    PreparingRebalance -> do
      updateMemberAndRebalance group member req delayedResponse
    CompletingRebalance -> do
      -- TODO: match protocols
      updateMemberAndRebalance group member req delayedResponse
    Stable -> do
      -- TODO: match protocols and leader
      updateMemberAndRebalance group member req delayedResponse
    _ -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)

doDynamicNewMemberJoinGroup :: Group -> K.RequestContext -> K.JoinGroupRequest -> T.Text -> C.MVar K.JoinGroupResponse -> IO ()
doDynamicNewMemberJoinGroup group reqCtx req newMemberId delayedResponse = do
  addMemberAndRebalance group reqCtx req newMemberId delayedResponse

addMemberAndRebalance :: Group -> K.RequestContext -> K.JoinGroupRequest -> T.Text -> C.MVar K.JoinGroupResponse -> IO ()
addMemberAndRebalance group reqCtx req newMemberId delayedResponse = do
  isGroupEmpty <- Utils.hashtableNull group.members
  member <- newMemberFromReq reqCtx req newMemberId (refineProtocols req.protocols)
  addMember group member (Just delayedResponse)
  -- TODO: check state
  -- Note: We consider the group as a new one if empty, and always wait
  --       an interval for joining. Otherwise, wait until all previous members
  --       have joined or timeout.
  -- FIXME: Is this correct?
  -- FIXME: How long to wait for a new group? Is 'group.initial.rebalance.delay.ms' correct?
  -- FIXME: Hardcoded constant!
  memberRebalanceTimeoutMs <- IO.readIORef member.rebalanceTimeoutMs
  prepareRebalance group
                   (if isGroupEmpty then return False else haveAllMembersRejoined)
                   (if isGroupEmpty then fromIntegral group.groupConfig.groupInitialRebalanceDelay
                                    else memberRebalanceTimeoutMs)
                   ("add member:" <> member.memberId)
  where
    -- Note: The new-added member is always present. So this is equivalent
    --       to check among members before.
    haveAllMembersRejoined :: IO Bool
    haveAllMembersRejoined = do
      H.foldM (\acc (mid,_) -> case acc of
                  False -> return False
                  True  -> isJust <$> H.lookup group.delayedJoinResponses mid
              ) True group.members

updateMemberAndRebalance :: Group -> Member -> K.JoinGroupRequest -> C.MVar K.JoinGroupResponse -> IO ()
updateMemberAndRebalance group member req delayedResponse = do
  updateMember group member req delayedResponse
  -- Note: On this case, the group can not be empty because at least this member
  --       is present. So we wait until all previous members have joined or timeout.
  -- FIXME: How long to wait? Is 'rebalanceTimeoutMs' of the member correct?
  timeout <- IO.readIORef member.rebalanceTimeoutMs
  prepareRebalance group
                   haveAllMembersRejoined
                   timeout
                   ("update member:" <> member.memberId)
  where
    -- Note: The new-added member is always present. So this is equivalent
    --       to check among members before.
    haveAllMembersRejoined :: IO Bool
    haveAllMembersRejoined = do
      H.foldM (\acc (mid,_) -> case acc of
                  False -> return False
                  True  -> isJust <$> H.lookup group.delayedJoinResponses mid
              ) True group.members

prepareRebalance :: Group -> IO Bool -> Int32 -> T.Text -> IO ()
prepareRebalance group@Group{..} p timeoutMs reason = do
  Log.info $ "prepare rebalance, group:" <> Log.build groupId
    <> "reason:" <> Log.build reason
  -- check state CompletingRebalance and cancel delayedSyncResponses
  Utils.whenIORefEq state CompletingRebalance $ do
    cancelDelayedSyncResponses group "rebalance"

  -- cancel delayed sync
  cancelDelayedSync group

  -- isEmptyState <- (Empty ==) <$> IO.readIORef state

  -- setup delayed rebalance if delayedRebalance is Nothing
  IO.readIORef delayedRebalance >>= \case
    Nothing -> do
      delayed <- makeDelayedRebalance group p (fromIntegral timeoutMs)
      Log.info $ "created delayed rebalance thread:" <> Log.buildString' delayed
        <> ", group:" <> Log.build groupId
      IO.atomicWriteIORef delayedRebalance (Just delayed)
      IO.atomicWriteIORef state PreparingRebalance
    _ -> pure ()

makeDelayedRebalance :: Group -> IO Bool -> Int -> IO C.ThreadId
makeDelayedRebalance group p rebalanceDelayMs =
  C.forkIO $ Utils.onOrTimeout p rebalanceDelayMs (rebalance group)

rebalance :: Group -> IO ()
rebalance group@Group{..} = do
  C.withMVar lock $ \() -> do
    Log.info $ "rebalancing is starting, group:" <> Log.build groupId
    -- remove all members who haven't joined(and maybe elect new leader)
    removeNotYetRejoinedMembers group

    nextGenerationId <- IO.atomicModifyIORef' group.groupGenerationId (\ggid -> (ggid + 1, ggid + 1))
    Log.info $ "started next generation, groupId:" <> Log.build group.groupId
      <> ", generationId:" <> Log.build nextGenerationId
    IO.readIORef leader >>= \case
      Nothing -> do
        Log.info $ "cancel rebalance without any join request, " <> Log.build groupId

        IO.atomicWriteIORef group.protocolName Nothing
        transitionTo group Empty

        IO.atomicWriteIORef delayedRebalance Nothing

        -- cancel delayedSync
        cancelDelayedSync group

        storeGroup group Map.empty
      Just leaderMemberId -> do
        doRelance group leaderMemberId

transitionTo :: Group -> GroupState -> IO ()
transitionTo group state = do
  oldState <- IO.atomicModifyIORef' group.state (state, )
  Log.info $ "group state changed, " <> Log.buildString' oldState <> " -> " <> Log.buildString' state
    <> ", group:" <> Log.build group.groupId

doRelance :: Group -> T.Text -> IO ()
doRelance group@Group{..} leaderMemberId = do
  selectedProtocolName <- computeProtocolName group

  -- state changes
  transitionTo group CompletingRebalance

  generationId <- IO.readIORef groupGenerationId
  leaderMembersInResponse <- H.toList members >>= M.mapM (\(_, m) -> getJoinResponseMember selectedProtocolName m)
  Log.info $ "members in join responses:" <> Log.buildString' leaderMembersInResponse
    <> ", group:" <> Log.build groupId

  delayedJoinResponseList <- H.toList delayedJoinResponses

  Log.info $ "set all delayed responses, response list:" <> Log.buildString' (length delayedJoinResponseList)
    <> ", group:" <> Log.build groupId
  -- response all delayedJoinResponses
  M.forM_ delayedJoinResponseList $ \(memberId, delayed) -> do
    let memebersInResponse = if leaderMemberId == memberId then leaderMembersInResponse else []
        resp = K.JoinGroupResponse
                { errorCode = 0
                , generationId = generationId
                , protocolName = selectedProtocolName
                , leader = leaderMemberId
                , memberId = memberId
                , members = K.KaArray (Just $ V.fromList memebersInResponse)
                , throttleTimeMs = 0
                }
    _ <- C.tryPutMVar delayed resp
    H.delete delayedJoinResponses memberId

  rebalanceTimeoutMs <- computeRebalnceTimeoutMs group
  -- FIXME: Is it correct to use rebalance timeout here? Or maybe session timeout?
  --        The state machine here is really weird...
  delayedSyncTid <- makeDelayedSync group generationId rebalanceTimeoutMs
  IO.atomicWriteIORef delayedSync (Just delayedSyncTid)
  Log.info $ "create delayed sync for group:" <> Log.build groupId
    <> ", threadId:" <> Log.buildString' delayedSyncTid

  IO.atomicWriteIORef delayedRebalance Nothing
  Log.info $ "rebalancing is finished, group:" <> Log.build groupId

removeNotYetRejoinedMembers :: Group -> IO ()
removeNotYetRejoinedMembers group@Group{..} = do
  (flip H.mapM_) members $ \(mid, member) -> do
    H.lookup delayedJoinResponses mid >>= \case
      Nothing -> do
        Log.info $ "remove not yet joined member: " <> Log.build mid <> " from " <> Log.build groupId
        removeMember group member
      Just _ -> pure ()

removeNotYetSyncedMembers :: Group -> IO ()
removeNotYetSyncedMembers group@Group{..} = do
  (flip H.mapM_) members $ \(mid, member) -> do
    H.lookup delayedSyncResponses mid >>= \case
      Nothing -> do
        Log.info $ "remove not yet synced member: " <> Log.build mid <> " from " <> Log.build groupId
        removeMember group member
      Just _ -> pure ()

makeDelayedSync :: Group -> Int32 -> Int32 -> IO C.ThreadId
makeDelayedSync group@Group{..} generationId timeoutMs = do
  C.forkIO $ do
    C.threadDelay (fromIntegral timeoutMs * 1000)
    C.withMVar lock $ \() -> do
      Utils.unlessIORefEq groupGenerationId generationId $ \currentGid -> do
        Log.warning $ "unexpected delayed sync with wrong generationId:" <> Log.build generationId
          <> ", current group generation id:" <> Log.build currentGid
          <> ", groupId:" <> Log.build groupId
      IO.readIORef state >>= \case
        CompletingRebalance -> do
          Log.info $ "delayed sync timeout, try to prepare Rebalance, group:" <> Log.build groupId
          removeNotYetSyncedMembers group

          -- remove itself (to avoid killing itself in prepareRebalance)
          IO.atomicWriteIORef delayedSync Nothing
          prepareRebalance group
                           (pure False) -- FIXME: Is this correct?
                           5000         -- FIXME: timeout?
                           "delayed sync timeout"
        s -> do
          Log.warning $ "unexpected delayed sync with wrong state:" <> Log.buildString' s
            <> ", group:" <> Log.build groupId

-- select max rebalanceTimeoutMs from all members
computeRebalnceTimeoutMs :: Group -> IO Int32
computeRebalnceTimeoutMs Group{..} = do
  H.foldM (\x (_, m) -> max x <$> IO.readIORef m.rebalanceTimeoutMs) 0 members

getJoinResponseMember :: T.Text -> Member -> IO K.JoinGroupResponseMember
getJoinResponseMember protocol m = do
  metadata <- getMemberMetadata m protocol
  return $ K.JoinGroupResponseMember m.memberId metadata

getMemberMetadata :: Member -> T.Text -> IO BS.ByteString
getMemberMetadata member protocol = do
  memberProtocols <- IO.readIORef member.supportedProtocols
  return $ snd . fromMaybe ("", "") $ List.find (\(n, _) -> n == protocol) memberProtocols

computeProtocolName :: Group -> IO T.Text
computeProtocolName group@Group{..} = do
  IO.readIORef protocolName >>= \case
    Nothing -> do
      pn <- chooseProtocolName group
      IO.atomicWriteIORef protocolName (Just pn)
      pure pn
    Just pn -> pure pn

-- choose protocol name from supportedProtocols
chooseProtocolName :: Group -> IO T.Text
chooseProtocolName Group {..} = do
  ps <- IO.readIORef supportedProtocols
  let pn = head $ Set.toList ps
  Log.info $ "choose protocol:" <> Log.build pn
    <> ", current supported protocols:" <> Log.buildString' ps
    <> ", group:" <> Log.build groupId
  return pn

updateSupportedProtocols :: Group -> [(T.Text, BS.ByteString)] -> IO ()
updateSupportedProtocols Group{..} protocols = do
  IO.atomicModifyIORef' supportedProtocols $ \ps ->
    if Set.null ps
      then (plainProtocols protocols, ())
      else (Set.intersection (plainProtocols protocols) ps, ())

addMember :: Group -> Member -> (Maybe (C.MVar K.JoinGroupResponse)) -> IO ()
addMember group@Group{..} member delayedResponse = do
  memberProtocols <- IO.readIORef member.supportedProtocols

  Utils.hashtableNull members >>= \case
    True -> do
      IO.atomicWriteIORef protocolType (Just member.protocolType)
      Log.info $ "group updated protocolType, groupId:" <> Log.build groupId
        <> "protocolType:" <> Log.build member.protocolType
    False -> pure ()

  Utils.whenIORefEq leader Nothing $ do
    IO.atomicWriteIORef leader (Just member.memberId)
    Log.info $ "updated leader, group:" <> Log.build groupId
      <> "leader:" <> Log.build member.memberId

  H.insert members member.memberId member
  updateSupportedProtocols group memberProtocols

  M.forM_ delayedResponse $ \delayed -> do
    H.insert delayedJoinResponses member.memberId delayed

updateMember :: Group -> Member -> K.JoinGroupRequest -> C.MVar K.JoinGroupResponse -> IO ()
updateMember group@Group{..} member req delayedResponse = do
  IO.atomicWriteIORef member.supportedProtocols (refineProtocols req.protocols)
  updateSupportedProtocols group (refineProtocols req.protocols)

  IO.atomicWriteIORef member.rebalanceTimeoutMs req.rebalanceTimeoutMs
  IO.atomicWriteIORef member.sessionTimeoutMs req.sessionTimeoutMs

  -- TODO: check delayedJoinResponses
  Log.info $ "updated member, add delayed response into response list for member:"
    <> Log.buildString' member.memberId <> ", group:" <> Log.build groupId
  H.insert delayedJoinResponses member.memberId delayedResponse

removeMember :: Group -> Member -> IO ()
removeMember Group{..} member = do
  -- stop heartbeatThread
  IO.readIORef member.heartbeatThread >>= \case
    Nothing -> pure ()
    Just tid -> C.killThread tid

  H.delete members member.memberId

  -- if the member is leader, select and set a new leader
  Utils.whenIORefEq leader (Just member.memberId) $ do
    H.toList delayedJoinResponses >>= \case
      [] -> do
        -- select from members
        H.toList members >>= \ms -> do
          let newLeader = listToMaybe (fmap fst ms)
          IO.atomicWriteIORef leader newLeader
          Log.info $ "selcet new leader from members, group:" <> Log.build groupId
            <> ", new leader:" <> Log.buildString' newLeader <> ", old leader:" <> Log.build member.memberId
      (mid, _):_ -> do
        IO.atomicWriteIORef leader (Just mid)
        Log.info $ "select new leader from delayedJoinResponses, group:" <> Log.build groupId
               <> ", new leader:" <> Log.build mid <> ", old leader:" <> Log.build member.memberId


plainProtocols :: [(T.Text, BS.ByteString)] -> Set.Set T.Text
plainProtocols = Set.fromList . (map fst)

-- should return a non-null protocol list
refineProtocols :: K.KaArray K.JoinGroupRequestProtocol -> [(T.Text, BS.ByteString)]
refineProtocols protocols = case K.unKaArray protocols of
  Nothing -> throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
  Just ps -> if (V.null ps)
    then throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
    else map (\p -> (p.name, p.metadata)) (V.toList ps)

------------------- Sync Group ----------------------

syncGroup :: Group -> K.SyncGroupRequest -> IO K.SyncGroupResponse
syncGroup group req@K.SyncGroupRequest{..} = do
  Log.info $ "received sync group request:" <> Log.buildString' req
  delayed <- C.newEmptyMVar
  C.withMVar (group.lock) $ \() -> do
    -- check member id
    member <- Utils.hashtableGet group.members memberId K.UNKNOWN_MEMBER_ID

    -- TODO: check generation id
    IO.readIORef group.state >>= \case
      CompletingRebalance -> doSyncGroup group req delayed
      Stable -> do
        assignment <- IO.readIORef member.assignment
        M.void $ C.tryPutMVar delayed (K.SyncGroupResponse {throttleTimeMs=0, errorCode=0, assignment=assignment})
      PreparingRebalance -> throw (ErrorCodeException K.REBALANCE_IN_PROGRESS)
      _ -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
  resp <- C.readMVar delayed
  Log.info $ "received delayed sync group response:" <> Log.buildString' resp
    <> ", group:" <> Log.build group.groupId
  return resp

doSyncGroup :: Group -> K.SyncGroupRequest -> C.MVar K.SyncGroupResponse -> IO ()
doSyncGroup group@Group{..} req@K.SyncGroupRequest{memberId=memberId} delayedResponse = do
  -- set delayed response
  H.lookup delayedSyncResponses memberId >>= \case
    Nothing -> H.insert delayedSyncResponses memberId delayedResponse
    _ -> do
      Log.warning $ "received duplicated sync group request:" <> Log.buildString' req <> ", rejected"
      throw (ErrorCodeException K.UNKNOWN_SERVER_ERROR)

  -- set assignments if this req from leader
  (Just leaderMemberId) <- IO.readIORef leader
  Log.info $ "sync group leaderMemberId: " <> Log.buildString' leaderMemberId
    <> " memberId:" <> Log.buildString' memberId
    <> " group:" <> Log.build groupId
    <> " isLeader:" <> Log.buildString' (memberId == leaderMemberId)
  when (memberId == leaderMemberId) $ do
    let assignmentMap = getAssignmentMap req

    storeGroup group assignmentMap

    setAndPropagateAssignment group assignmentMap

    -- setup delayedCheckHeart
    setupDelayedCheckHeartbeat group

    -- cancel delayed sync
    cancelDelayedSync group

    -- set state
    transitionTo group Stable

getAssignmentMap :: K.SyncGroupRequest -> Map.Map T.Text BS.ByteString
getAssignmentMap req =
  Map.fromList . map (\x -> (x.memberId, x.assignment)) $ Utils.kaArrayToList req.assignments

setAndPropagateAssignment :: Group -> Map.Map T.Text BS.ByteString -> IO ()
setAndPropagateAssignment group@Group{..} assignments = do
  -- set assignments
  Log.info $ "setting assignments:" <> Log.buildString' assignments
    <> "group:" <> Log.build groupId
  (flip H.mapM_) members $ \(memberId, member) -> do
    -- set assignment
    let assignment = fromMaybe "" $ Map.lookup memberId assignments
    IO.atomicWriteIORef member.assignment assignment

    -- propagate assignments
    H.lookup delayedSyncResponses memberId >>= \case
      Nothing -> pure ()
      Just delayed -> do
        M.void $ C.tryPutMVar delayed (K.SyncGroupResponse {throttleTimeMs=0, errorCode=0, assignment=assignment})
        H.delete delayedSyncResponses memberId

  -- delayedSyncResponses should have been empty,
  -- so it actually does nothing,
  -- but it is useful if delayedSyncResponses is NOT empty(bug):
  --  * log error information
  --  * avoid inconsistent state
  --  * avoid clients are getting stuck
  cancelDelayedSyncResponses group "ERROR: delayedSyncResponses should be empty after propagated assignments"
  Log.info $ "setAndPropagateAssignment completed, group:" <> Log.build groupId

leaveGroup :: Group -> K.LeaveGroupRequest -> IO K.LeaveGroupResponse
leaveGroup group@Group{..} req = do
  C.withMVar lock $ \() -> do
    member <- getMember group req.memberId
    IO.readIORef state >>= \case
      Dead -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      Empty -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      CompletingRebalance -> removeMemberAndUpdateGroup group member
      Stable -> removeMemberAndUpdateGroup group member
      PreparingRebalance -> do
          -- TODO: should NOT BE PASSIBLE in this version
          Log.warning $ "received a leave group in PreparingRebalance state, ignored it"
            <> ", groupId:" <> Log.buildString' req.groupId
            <> ", memberId:" <> Log.buildString' req.memberId
          throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)

    return $ K.LeaveGroupResponse {errorCode=0, throttleTimeMs=0}

getMember :: Group -> T.Text -> IO Member
getMember Group{..} memberId = do
  H.lookup members memberId >>= \case
    Nothing -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
    Just member -> pure member

removeMemberAndUpdateGroup :: Group -> Member -> IO ()
removeMemberAndUpdateGroup group@Group{..} member = do
  Log.info $ "member: " <> Log.build member.memberId <> " is leaving group:" <> Log.build groupId

  -- New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
  -- to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
  -- will retry the JoinGroup request if is still active.
  cancelDelayedJoinResponse group member.memberId

  removeMember group member
  prepareRebalance group
                   (pure True) -- FIXME: Is this correct?
                   5000        -- FIXME: timeout?
                   ("remove member:" <> member.memberId)

cancelDelayedJoinResponse :: Group -> T.Text -> IO ()
cancelDelayedJoinResponse Group{..} memberId = do
  H.lookup delayedJoinResponses memberId >>= \case
    Nothing -> pure ()
    Just delayed -> do
      _ <- C.tryPutMVar delayed (makeJoinResponseError memberId K.UNKNOWN_MEMBER_ID)
      H.delete delayedJoinResponses memberId

-- default heartbeat interval: 3s
heartbeat :: Group -> K.HeartbeatRequest -> IO K.HeartbeatResponse
heartbeat group@Group{..} req = do
  C.withMVar lock $ \() -> do
    -- check generation id
    checkGroupGenerationId group req.generationId

    -- check state
    IO.readIORef state >>= \case
      PreparingRebalance -> throw (ErrorCodeException K.REBALANCE_IN_PROGRESS)
      CompletingRebalance -> throw (ErrorCodeException K.REBALANCE_IN_PROGRESS)
      Dead -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      Empty -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      Stable -> pure ()

    H.lookup members req.memberId >>= \case
      Nothing -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      Just member -> updateLatestHeartbeat member
    return $ K.HeartbeatResponse {throttleTimeMs=0, errorCode=0}

checkGroupGenerationId :: Group -> Int32 -> IO ()
checkGroupGenerationId Group{..} generationId = do
  currentGenerationId <- IO.readIORef groupGenerationId
  M.unless (currentGenerationId == generationId) $ do
    Log.warning $ "invalid generation id"
      <> ", current generationId:" <> Log.buildString' currentGenerationId
      <> ", expected generationId:" <> Log.buildString' generationId
      <> ", group:" <> Log.build groupId
    throw (ErrorCodeException K.ILLEGAL_GENERATION)

updateLatestHeartbeat :: Member -> IO ()
updateLatestHeartbeat Member{..} = do
  newLastHeartbeat <- Time.getSystemMsTimestamp
  IO.atomicWriteIORef lastHeartbeat newLastHeartbeat
  Log.debug $ "lastHeartbeat updated, memeber:" <> Log.buildString' memberId
    <> ", newLastHeartbeat:" <> Log.buildString' newLastHeartbeat

setupDelayedCheckHeartbeat :: Group -> IO ()
setupDelayedCheckHeartbeat group@Group{..} = do
  (flip H.mapM_) members $ \(_, member) -> do
    updateLatestHeartbeat member
    memberSessionTimeoutMs <- IO.readIORef member.sessionTimeoutMs
    threadId <- C.forkIO $ delayedCheckHeart group member memberSessionTimeoutMs
    Log.info $ "setup delayed heartbeat check, threadId:" <> Log.buildString' threadId
      <> ", member:" <> Log.buildString' member.memberId
      <> ", group:" <> Log.build groupId
    IO.atomicWriteIORef member.heartbeatThread (Just threadId)

-- cancel all delayedCheckHearts
cancelDelayedCheckHeartbeats :: Group -> IO ()
cancelDelayedCheckHeartbeats Group{..} = do
  (flip H.mapM_) members $ \(mid, member)-> do
    IO.readIORef member.heartbeatThread >>= \case
      Nothing -> pure ()
      Just tid -> do
        Log.info $ "cancel delayedCheckHeart, member:" <> Log.buildString' mid
          <> "group:" <> Log.build groupId
        C.killThread tid
        IO.atomicWriteIORef member.heartbeatThread Nothing

delayedCheckHeart :: Group -> Member -> Int32 -> IO ()
delayedCheckHeart group member delayMs = do
  C.threadDelay (1000 * fromIntegral delayMs)
  nextDelayMs <- checkHeartbeatAndMaybeRebalance group member
  M.when (nextDelayMs > 0) $ do
    delayedCheckHeart group member nextDelayMs

-- return: nextDelayMs
--   0 or <0: timeout
--   >0: nextDelayMs
checkHeartbeatAndMaybeRebalance :: Group -> Member -> IO Int32
checkHeartbeatAndMaybeRebalance group member = do
  C.withMVar group.lock $ \() -> do
    now <- Time.getSystemMsTimestamp
    lastUpdated <- IO.readIORef member.lastHeartbeat
    memberSessionTimeoutMs <- IO.readIORef member.sessionTimeoutMs
    let nextDelayMs = memberSessionTimeoutMs - (fromIntegral (now - lastUpdated))
    M.when (nextDelayMs <= 0) $ do
      Log.info $ "heartbeat timeout, memberId:" <> Log.buildString' member.memberId
        <> ", lastHeartbeat:" <> Log.buildString' lastUpdated
        <> ", now:" <> Log.buildString' now
        <> ", sessionTimeoutMs:" <> Log.buildString' memberSessionTimeoutMs
        <> ", group:" <> Log.build group.groupId
      -- remove itself (to avoid killing itself in prepareRebalance)
      IO.atomicWriteIORef member.heartbeatThread Nothing
      removeMemberAndUpdateGroup group member
    return nextDelayMs

------------------- Commit Offsets -------------------------
-- Note: 'commitOffsets' works in a lock for the whole request, and
--       may do a pre-check, such as ACL authz on each topic.
--       That is why we pass a "validate" function ('validateReqTopic')
--       on EACH topic to it.
-- FIXME: Better method than passing a "validate" function?
commitOffsets :: Group
              -> K.OffsetCommitRequest
              -> (K.OffsetCommitRequestTopic -> IO K.ErrorCode)
              -> IO K.OffsetCommitResponse
commitOffsets group@Group{..} req validateReqTopic = do
  C.withMVar lock $ \() -> do
    validateOffsetcommit group req
    IO.readIORef state >>= \case
      CompletingRebalance -> throw (ErrorCodeException K.REBALANCE_IN_PROGRESS)
      Dead -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      _ -> do
        topics <- Utils.forKaArrayM req.topics $ \reqTopic -> do
          validateReqTopic reqTopic >>= \case
            K.NONE -> do
              res <- GOM.storeOffsets group.metadataManager reqTopic.name reqTopic.partitions
              return $ K.OffsetCommitResponseTopic
                { name       = reqTopic.name
                , partitions = res
                }
            code   ->
              return $ makeErrorTopicResponse code reqTopic

        Utils.whenIORefEq storedMetadata False $ do
          Log.info $ "commited offsets on Empty Group, storing Empty Group:" <> Log.build group.groupId
          storeGroup group Map.empty
        -- updateLatestHeartbeat
        H.lookup members req.memberId >>= \case
          Nothing -> pure ()
          Just m -> updateLatestHeartbeat m
        return K.OffsetCommitResponse {topics=topics, throttleTimeMs=0}
  where
    makeErrorTopicResponse code offsetCommitTopic =
      K.OffsetCommitResponseTopic
      { name = offsetCommitTopic.name
      , partitions =
          Utils.forKaArray offsetCommitTopic.partitions $ \offsetCommitPartition ->
            K.OffsetCommitResponsePartition
            { partitionIndex = offsetCommitPartition.partitionIndex
            , errorCode      = code
            }
       }

validateOffsetcommit :: Group -> K.OffsetCommitRequest -> IO ()
validateOffsetcommit Group{..} req = do
  currentState <- IO.readIORef state
  currentGenerationId <- IO.readIORef groupGenerationId
  if (req.generationId < 0) then do
    -- When the generation id is -1, the request comes from either the admin client
    -- or a consumer which does not use the group management facility. In this case,
    -- the request can commit offsets if the group is empty.
    when (currentState /= Empty) $ do
      throw (ErrorCodeException K.ILLEGAL_GENERATION)
  else do
    when (req.generationId /= currentGenerationId) $ do
      throw (ErrorCodeException K.ILLEGAL_GENERATION)

    H.lookup members req.memberId >>= \case
      Nothing -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      Just _ -> pure ()

------------------- Fetch Offsets -------------------------
-- Note: 'fetchOffsets' may do a pre-check, such as ACL authz
--       on each topic. That is why we pass a "validate" function
--       ('validateReqTopic') on each topic to it.
-- FIXME: Better method than passing a "validate" function?
fetchOffsets
  :: Group
  -> K.OffsetFetchRequestTopic
  -> (K.OffsetFetchRequestTopic -> IO K.ErrorCode)
  -> IO K.OffsetFetchResponseTopic
fetchOffsets Group{..} reqTopic validateReqTopic = validateReqTopic reqTopic >>= \case
  K.NONE ->
    GOM.fetchOffsets metadataManager reqTopic.name reqTopic.partitionIndexes
  code   -> do
    -- FIXME: what to return on 'Nothing'?
    let partitions' = fromMaybe V.empty (K.unKaArray reqTopic.partitionIndexes)
    return $ K.OffsetFetchResponseTopic
      { name       = reqTopic.name
      , partitions = K.KaArray (Just $ (makeErrorPartition code) <$> partitions')
      }
  where
    makeErrorPartition code idx =
      K.OffsetFetchResponsePartition
        { partitionIndex  = idx
        , committedOffset = -1
        , metadata        = Nothing
        , errorCode       = code
        }

-- Note: 'fetchAllOffsets' may do a pre-check, such as ACL authz
--       on each topic. That is why we pass a "validate" function
--       ('validateReqTopic') on each topic to it.
-- FIXME: Better method than passing a "validate" function?
fetchAllOffsets
  :: Group
  -> (K.OffsetFetchRequestTopic -> IO K.ErrorCode)
  -> IO (K.KaArray K.OffsetFetchResponseTopic)
fetchAllOffsets Group{..} validateReqTopic = do
  topicResponses <- GOM.fetchAllOffsets metadataManager
  -- WARNING: Offsets of unauthzed topics should not be leaked
  --          on "fetch all". So we just OMIT the unauthzed
  --          topic responses rather than returning an error.
  Utils.filterKaArrayM (\topicResponse -> do
    let topicFetchRequest = K.OffsetFetchRequestTopic
          { name = topicResponse.name
          , partitionIndexes = Utils.mapKaArray (\K.OffsetFetchResponsePartition{..} -> partitionIndex) topicResponse.partitions
          }
    (== K.NONE) <$> validateReqTopic topicFetchRequest
                       ) topicResponses

------------------- Group Overview(ListedGroup) -------------------------
overview :: Group -> IO K.ListedGroup
overview Group{..} = do
  pt <- fromMaybe "" <$> IO.readIORef protocolType
  return $ K.ListedGroup {groupId=groupId, protocolType = pt}

------------------- Describe Group -------------------------
describe :: Group -> IO K.DescribedGroup
describe Group{..} = do
  C.withMVar lock $ \() -> do
    protocolType' <- fromMaybe "" <$> IO.readIORef protocolType
    protocolName' <- fromMaybe "" <$> IO.readIORef protocolName
    state' <- T.pack . show <$> IO.readIORef state
    members' <- H.toList members >>= M.mapM (\(_, member) -> describeMember member protocolName')
    return $ K.DescribedGroup {
        protocolData=protocolName'
      , groupState= state'
      , errorCode=0
      , members=Utils.listToKaArray members'
      , groupId=groupId
      , protocolType=protocolType'
      }

describeMember :: Member -> T.Text -> IO K.DescribedGroupMember
describeMember member@Member{..} protocol = do
  assignment' <- IO.readIORef assignment
  memberMetadata' <- getMemberMetadata member protocol
  return $ K.DescribedGroupMember {
    memberMetadata=memberMetadata'
    , memberAssignment=assignment'
    , clientHost=clientHost
    , clientId=clientId
    , memberId=memberId
    }

------------------- Store Group -------------------------
storeGroup :: Group -> Map.Map T.Text BS.ByteString -> IO ()
storeGroup group assignments = do
  value <- getGroupValue group assignments
  Meta.upsertMeta @CM.GroupMetadataValue group.groupId value group.metaHandle
  IO.atomicWriteIORef group.storedMetadata True

getGroupValue :: Group -> Map.Map T.Text BS.ByteString -> IO CM.GroupMetadataValue
getGroupValue group assignments = do
  protocolName <- IO.readIORef group.protocolName
  protocolType <- fromMaybe "" <$> IO.readIORef group.protocolType

  leader <- IO.readIORef group.leader
  generationId <- IO.readIORef group.groupGenerationId
  members <- H.toList group.members >>= mapM (getMemberValue (fromMaybe "" protocolName) assignments . snd)

  return $ CM.GroupMetadataValue {
    prototcolName=protocolName
    , protocolType=protocolType
    , members=V.fromList members
    , leader=leader
    , groupId=group.groupId
    , generationId=generationId
    }

getMemberValue :: T.Text -> Map.Map T.Text BS.ByteString -> Member -> IO CM.MemberMetadataValue
getMemberValue protocol assignments member = do
  subscription <- Utils.encodeBase64 <$> getMemberMetadata member protocol
  let assignment = Utils.encodeBase64 . fromMaybe "" $ Map.lookup member.memberId assignments
  sessionTimeout <- IO.readIORef member.sessionTimeoutMs
  rebalanceTimeout <- IO.readIORef member.rebalanceTimeoutMs

  return $ CM.MemberMetadataValue {
    subscription=subscription
    , sessionTimeout=sessionTimeout
    , rebalanceTimeout=rebalanceTimeout
    , memberId=member.memberId
    , clientHost=member.clientHost
    , assignment=assignment
    , clientId=member.clientId
    }

------------------- Group Error Response -------------------------
makeJoinResponseError :: T.Text -> K.ErrorCode -> K.JoinGroupResponse
makeJoinResponseError memberId errorCode =
  K.JoinGroupResponse
    { errorCode = errorCode
    , generationId = -1
    , protocolName = ""
    , leader = ""
    , memberId = memberId
    , members = K.NonNullKaArray V.empty
    , throttleTimeMs = 0
    }
