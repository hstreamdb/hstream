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
import           Data.Maybe                             (fromMaybe, listToMaybe)
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

-- TODO:
-- * kafka/group config
--  * configurable
-- * group metadata manager
--  * store group information

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

data GroupConfig
  = GroupConfig
  {
  }

data Group
  = Group
  { lock                 :: C.MVar ()
  , groupId              :: T.Text
  , groupGenerationId    :: IO.IORef Int32
  , state                :: IO.IORef GroupState
  , config               :: GroupConfig
  , leader               :: IO.IORef (Maybe T.Text)
  , members              :: HashTable T.Text Member
  -- , pendingMembers     :: HashTable T.Text ()
  , delayedJoinResponses :: HashTable T.Text (C.MVar K.JoinGroupResponseV0)
  -- , pendingSyncMembers :: HashTable T.Text ()
  -- , newMemberAdded     :: IO.IORef Bool
  , delayedRebalance     :: IO.IORef (Maybe C.ThreadId)
  , delayedSync          :: IO.IORef (Maybe C.ThreadId)

  , delayedSyncResponses :: HashTable T.Text (C.MVar K.SyncGroupResponseV0)

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

newGroup :: T.Text -> GroupOffsetManager -> Meta.MetaHandle -> IO Group
newGroup group metadataManager metaHandle = do
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
    , config = GroupConfig
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

newGroupFromValue :: CM.GroupMetadataValue -> GroupOffsetManager -> Meta.MetaHandle -> IO Group
newGroupFromValue value metadataManager metaHandle = do
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
        , config = GroupConfig
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

joinGroup :: Group -> K.RequestContext -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
joinGroup group@Group{..} reqCtx req = do
  -- delayed response(join barrier)
  Log.debug $ "received joinGroup"
  delayedResponse <- C.newEmptyMVar
  C.withMVar lock $ \_ -> do
    -- TODO: GROUP MAX SIZE

    checkSupportedProtocols group req
    Log.debug $ "checked protocols"

    -- check state
    IO.readIORef group.state >>= \case
      Dead -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      _ -> pure ()

    Log.debug $ "checked state"
    if T.null req.memberId
      then doNewMemberJoinGoup group reqCtx req delayedResponse
      else doCurrentMemeberJoinGroup group req delayedResponse

  -- waiting other consumers
  resp <- C.takeMVar delayedResponse
  Log.info $ "joinGroup: received delayed response:" <> Log.buildString' resp
  return resp

checkSupportedProtocols :: Group -> K.JoinGroupRequestV0 -> IO ()
checkSupportedProtocols Group{..} req = do
  Log.debug $ "checking protocols"
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
      <> ", reason:" <> Log.build reason
    _ <- C.tryPutMVar delayed $ K.SyncGroupResponseV0 K.REBALANCE_IN_PROGRESS BS.empty
    H.delete delayedSyncResponses memberId

cancelDelayedSync :: Group -> IO ()
cancelDelayedSync Group{..} = do
  IO.readIORef delayedSync >>= \case
    Nothing -> pure ()
    Just delayed -> do
      C.killThread delayed
      IO.atomicWriteIORef delayedSync Nothing

doNewMemberJoinGoup :: Group -> K.RequestContext -> K.JoinGroupRequestV0 -> C.MVar K.JoinGroupResponseV0 -> IO ()
doNewMemberJoinGoup group reqCtx req delayedResponse = do
  newMemberId <- generateMemberId reqCtx
  Log.debug $ "generated member id:" <> Log.buildString' newMemberId
  doDynamicNewMemberJoinGroup group reqCtx req newMemberId delayedResponse

generateMemberId :: K.RequestContext -> IO T.Text
generateMemberId reqCtx = do
  (fromMaybe "" (M.join reqCtx.clientId) <>) . ("-" <>) . UUID.toText <$> UUID.nextRandom

doCurrentMemeberJoinGroup :: Group -> K.JoinGroupRequestV0 -> C.MVar K.JoinGroupResponseV0 -> IO ()
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

doDynamicNewMemberJoinGroup :: Group -> K.RequestContext -> K.JoinGroupRequestV0 -> T.Text -> C.MVar K.JoinGroupResponseV0 -> IO ()
doDynamicNewMemberJoinGroup group reqCtx req newMemberId delayedResponse = do
  addMemberAndRebalance group reqCtx req newMemberId delayedResponse

addMemberAndRebalance :: Group -> K.RequestContext -> K.JoinGroupRequestV0 -> T.Text -> C.MVar K.JoinGroupResponseV0 -> IO ()
addMemberAndRebalance group reqCtx req newMemberId delayedResponse = do
  member <- newMemberFromReq reqCtx req newMemberId (refineProtocols req.protocols)
  addMember group member (Just delayedResponse)
  -- TODO: check state
  prepareRebalance group

updateMemberAndRebalance :: Group -> Member -> K.JoinGroupRequestV0 -> C.MVar K.JoinGroupResponseV0 -> IO ()
updateMemberAndRebalance group member req delayedResponse = do
  updateMember group member req delayedResponse
  prepareRebalance group

prepareRebalance :: Group -> IO ()
prepareRebalance group@Group{..} = do
  -- check state CompletingRebalance and cancel delayedSyncResponses
  Utils.whenIORefEq state CompletingRebalance $ do
    cancelDelayedSyncResponses group "rebalance"

  -- cancel delayed sync
  cancelDelayedSync group

  -- isEmptyState <- (Empty ==) <$> IO.readIORef state

  -- setup delayed rebalance if delayedRebalance is Nothing
  -- TODO: configurable initRebalanceDelayMs, 5000 by default
  IO.readIORef delayedRebalance >>= \case
    Nothing -> do
      delayed <- makeDelayedRebalance group 5000
      Log.info $ "created delayed rebalance thread:" <> Log.buildString' delayed
      IO.atomicWriteIORef delayedRebalance (Just delayed)
      IO.atomicWriteIORef state PreparingRebalance
    _ -> pure ()

-- TODO: dynamically delay with initTimeoutMs and RebalanceTimeoutMs
makeDelayedRebalance :: Group -> Int32 -> IO C.ThreadId
makeDelayedRebalance group rebalanceDelayMs = do
  C.forkIO $ do
    C.threadDelay (1000 * fromIntegral rebalanceDelayMs)
    rebalance group

rebalance :: Group -> IO ()
rebalance group@Group{..} = do
  C.withMVar lock $ \() -> do
    Log.info "rebalancing is starting"
    -- remove all members who haven't joined(and maybe elect new leader)
    removeNotYetRejoinedMembers group

    nextGenerationId <- IO.atomicModifyIORef' group.groupGenerationId (\ggid -> (ggid + 1, ggid + 1))
    Log.info $ "started next generation, groupId:" <> Log.build group.groupId
      <> ", generationId:" <> Log.build nextGenerationId
    IO.readIORef leader >>= \case
      Nothing -> do
        Log.info "cancel rebalance without any join request"

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

doRelance :: Group -> T.Text -> IO ()
doRelance group@Group{..} leaderMemberId = do
  selectedProtocolName <- computeProtocolName group
  Log.info $ "selected protocolName:" <> Log.buildString' selectedProtocolName

  -- state changes
  transitionTo group CompletingRebalance

  generationId <- IO.readIORef groupGenerationId
  leaderMembersInResponse <- H.toList members >>= M.mapM (\(_, m) -> getJoinResponseMember selectedProtocolName m)
  Log.debug $ "members in join responses" <> Log.buildString' leaderMembersInResponse

  delayedJoinResponseList <- H.toList delayedJoinResponses

  Log.info $ "set all delayed responses, response list size:" <> Log.buildString' (length delayedJoinResponseList)
  -- response all delayedJoinResponses
  M.forM_ delayedJoinResponseList $ \(memberId, delayed) -> do
    let memebersInResponse = if leaderMemberId == memberId then leaderMembersInResponse else []
        resp = K.JoinGroupResponseV0 {
        errorCode = 0
      , generationId = generationId
      , protocolName = selectedProtocolName
      , leader = leaderMemberId
      , memberId = memberId
      , members = K.KaArray (Just $ V.fromList memebersInResponse)
      }
    Log.debug $ "set delayed response:" <> Log.buildString' resp
      <> " for " <> Log.buildString' memberId
    _ <- C.tryPutMVar delayed resp
    H.delete delayedJoinResponses memberId

  -- TODO: rebalanceTimeout
  rebalanceTimeoutMs <- computeRebalnceTimeoutMs group
  delayedSyncTid <- makeDelayedSync group generationId rebalanceTimeoutMs
  IO.atomicWriteIORef delayedSync (Just delayedSyncTid)

  IO.atomicWriteIORef delayedRebalance Nothing
  Log.info "rebalancing is finished"

removeNotYetRejoinedMembers :: Group -> IO ()
removeNotYetRejoinedMembers group@Group{..} = do
  (flip H.mapM_) members $ \(mid, member) -> do
    H.lookup delayedJoinResponses mid >>= \case
      Nothing -> do
        Log.info $ "remove member: " <> Log.build mid <> " from " <> Log.build groupId
        removeMember group member
      Just _ -> pure ()

makeDelayedSync :: Group -> Int32 -> Int32 -> IO C.ThreadId
makeDelayedSync group@Group{..} generationId timeoutMs = do
  C.forkIO $ do
    C.threadDelay (fromIntegral timeoutMs * 1000)
    C.withMVar lock $ \() -> do
      Utils.unlessIORefEq groupGenerationId generationId $ \currentGid -> do
        Log.fatal $ "unexpected delaye sync with wrong generationId:" <> Log.build generationId
          <> ", current group generation id:" <> Log.build currentGid
          <> ", groupId:" <> Log.build groupId
      IO.readIORef state >>= \case
        CompletingRebalance -> do
          Log.info $ "delayed sync timeout, try to prepare Rebalance, groupId:" <> Log.build groupId
          -- remove itself (to avoid killing itself in prepareRebalance)
          IO.atomicWriteIORef delayedSync Nothing
          prepareRebalance group
        s -> do
          Log.fatal $ "unexpected delaye sync with wrong state:" <> Log.buildString' s
            <> ", groupId:" <> Log.build groupId

-- select max rebalanceTimeoutMs from all members
computeRebalnceTimeoutMs :: Group -> IO Int32
computeRebalnceTimeoutMs Group{..} = do
  H.foldM (\x (_, m) -> max x <$> IO.readIORef m.rebalanceTimeoutMs) 0 members

getJoinResponseMember :: T.Text -> Member -> IO K.JoinGroupResponseMemberV0
getJoinResponseMember protocol m = do
  metadata <- getMemberMetadata m protocol
  return $ K.JoinGroupResponseMemberV0 m.memberId metadata

getMemberMetadata :: Member -> T.Text -> IO BS.ByteString
getMemberMetadata member protocol = do
  memberProtocols <- IO.readIORef member.supportedProtocols
  return $ snd . fromMaybe ("", "") $ List.find (\(n, _) -> n == protocol) memberProtocols

computeProtocolName :: Group -> IO T.Text
computeProtocolName group@Group{..} = do
  IO.readIORef protocolName >>= \case
    Nothing -> do
      pn <- chooseProtocolName group
      Log.debug $ "choosed protocolName" <> Log.buildString' pn
      IO.atomicWriteIORef protocolName (Just pn)
      pure pn
    Just pn -> pure pn

-- choose protocol name from supportedProtocols
chooseProtocolName :: Group -> IO T.Text
chooseProtocolName Group {..} = do
  ps <- IO.readIORef supportedProtocols
  Log.debug $ "protocols:" <> Log.buildString' ps
  return . head $ Set.toList ps

updateSupportedProtocols :: Group -> [(T.Text, BS.ByteString)] -> IO ()
updateSupportedProtocols Group{..} protocols = do
  IO.atomicModifyIORef' supportedProtocols $ \ps ->
    if Set.null ps
      then (plainProtocols protocols, ())
      else (Set.intersection (plainProtocols protocols) ps, ())

addMember :: Group -> Member -> (Maybe (C.MVar K.JoinGroupResponseV0)) -> IO ()
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
    Log.info $ "group updated leader, groupId:" <> Log.build groupId
      <> "leader:" <> Log.build member.memberId

  H.insert members member.memberId member
  updateSupportedProtocols group memberProtocols

  M.forM_ delayedResponse $ \delayed -> do
    Log.debug $ "add delayed response into response list for member:" <> Log.buildString' member.memberId
    H.insert delayedJoinResponses member.memberId delayed

updateMember :: Group -> Member -> K.JoinGroupRequestV0 -> C.MVar K.JoinGroupResponseV0 -> IO ()
updateMember group@Group{..} member req delayedResponse = do
  IO.atomicWriteIORef member.supportedProtocols (refineProtocols req.protocols)
  updateSupportedProtocols group (refineProtocols req.protocols)

  -- TODO: V1Request will include rebalanceTimeoutMs
  IO.atomicWriteIORef member.rebalanceTimeoutMs req.sessionTimeoutMs
  IO.atomicWriteIORef member.sessionTimeoutMs req.sessionTimeoutMs

  -- TODO: check delayedJoinResponses
  Log.debug $ "add delayed response into response list for member:" <> Log.buildString' member.memberId
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
          IO.atomicWriteIORef leader (listToMaybe (fmap fst ms))
      (mid, _):_ -> do
        IO.atomicWriteIORef leader (Just mid)


plainProtocols :: [(T.Text, BS.ByteString)] -> Set.Set T.Text
plainProtocols = Set.fromList . (map fst)

-- should return a non-null protocol list
refineProtocols :: K.KaArray K.JoinGroupRequestProtocolV0 -> [(T.Text, BS.ByteString)]
refineProtocols protocols = case K.unKaArray protocols of
  Nothing -> throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
  Just ps -> if (V.null ps)
    then throw (ErrorCodeException K.INCONSISTENT_GROUP_PROTOCOL)
    else map (\p -> (p.name, p.metadata)) (V.toList ps)

------------------- Sync Group ----------------------

syncGroup :: Group -> K.SyncGroupRequestV0 -> IO K.SyncGroupResponseV0
syncGroup group req@K.SyncGroupRequestV0{..} = do
  delayed <- C.newEmptyMVar
  C.withMVar (group.lock) $ \() -> do
    -- check member id
    member <- Utils.hashtableGet group.members memberId K.UNKNOWN_MEMBER_ID

    -- TODO: check generation id
    IO.readIORef group.state >>= \case
      CompletingRebalance -> doSyncGroup group req delayed
      Stable -> do
        assignment <- IO.readIORef member.assignment
        M.void $ C.tryPutMVar delayed (K.SyncGroupResponseV0 0 assignment)
      PreparingRebalance -> throw (ErrorCodeException K.REBALANCE_IN_PROGRESS)
      _ -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
  C.readMVar delayed

doSyncGroup :: Group -> K.SyncGroupRequestV0 -> C.MVar K.SyncGroupResponseV0 -> IO ()
doSyncGroup group@Group{..} req@K.SyncGroupRequestV0{memberId=memberId} delayedResponse = do
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
  when (memberId == leaderMemberId) $ do
    Log.info $ "received leader SyncGroup request, " <> Log.buildString' memberId
    let assignmentMap = getAssignmentMap req

    storeGroup group assignmentMap

    setAndPropagateAssignment group assignmentMap

    -- setup delayedCheckHeart
    setupDelayedCheckHeartbeat group

    -- set state
    IO.atomicWriteIORef state Stable

getAssignmentMap :: K.SyncGroupRequestV0 -> Map.Map T.Text BS.ByteString
getAssignmentMap req =
  Map.fromList . map (\x -> (x.memberId, x.assignment)) $ Utils.kaArrayToList req.assignments

setAndPropagateAssignment :: Group -> Map.Map T.Text BS.ByteString -> IO ()
setAndPropagateAssignment group@Group{..} assignments = do
  -- set assignments
  Log.info $ "setting assignments:" <> Log.buildString' assignments
    <> "group Id:" <> Log.build groupId
  (flip H.mapM_) members $ \(memberId, member) -> do
    -- set assignment
    let assignment = fromMaybe "" $ Map.lookup memberId assignments
    IO.atomicWriteIORef member.assignment assignment

    -- propagate assignments
    H.lookup delayedSyncResponses memberId >>= \case
      Nothing -> pure ()
      Just delayed -> do
        M.void $ C.tryPutMVar delayed (K.SyncGroupResponseV0 0 assignment)
        H.delete delayedJoinResponses memberId

  -- delayedJoinResponses should have been empty,
  -- so it actually does nothing,
  -- but it is useful if delayedJoinResponses is NOT empty(bug):
  --  * log error information
  --  * avoid inconsistent state
  --  * avoid clients are getting stuck
  cancelDelayedSyncResponses group "ERROR: delayedJoinResponses should be empty after propagated assignments"
  Log.info $ "setAndPropagateAssignment completed"

leaveGroup :: Group -> K.LeaveGroupRequestV0 -> IO K.LeaveGroupResponseV0
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

    return $ K.LeaveGroupResponseV0 0

getMember :: Group -> T.Text -> IO Member
getMember Group{..} memberId = do
  H.lookup members memberId >>= \case
    Nothing -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
    Just member -> pure member

removeMemberAndUpdateGroup :: Group -> Member -> IO ()
removeMemberAndUpdateGroup group@Group{..} member = do
  Log.info $ "member: " <> Log.build member.memberId <> " is leaving group:" <> Log.build groupId
  removeMember group member
  prepareRebalance group

-- default heartbeat interval: 3s
heartbeat :: Group -> K.HeartbeatRequestV0 -> IO K.HeartbeatResponseV0
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
    return $ K.HeartbeatResponseV0 0

checkGroupGenerationId :: Group -> Int32 -> IO ()
checkGroupGenerationId Group{..} generationId = do
  currentGenerationId <- IO.readIORef groupGenerationId
  M.unless (currentGenerationId == generationId) $ do
    Log.debug $ "invalid generation id"
      <> ", current generationId:" <> Log.buildString' currentGenerationId
      <> ", expected generationId" <> Log.buildString' generationId
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
    Log.debug $ "setup delayed heartbeat check, threadId:" <> Log.buildString' threadId
      <> ", member:" <> Log.buildString' member.memberId
    IO.atomicWriteIORef member.heartbeatThread (Just threadId)

-- cancel all delayedCheckHearts
cancelDelayedCheckHeartbeats :: Group -> IO ()
cancelDelayedCheckHeartbeats Group{..} = do
  (flip H.mapM_) members $ \(mid, member)-> do
    IO.readIORef member.heartbeatThread >>= \case
      Nothing -> pure ()
      Just tid -> do
        Log.info $ "cancel delayedCheckHeart, member:" <> Log.buildString' mid
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
      -- remove itself (to avoid killing itself in prepareRebalance)
      IO.atomicWriteIORef member.heartbeatThread Nothing
      removeMemberAndUpdateGroup group member
    return nextDelayMs

------------------- Commit Offsets -------------------------
commitOffsets :: Group -> K.OffsetCommitRequestV2 -> IO K.OffsetCommitResponseV2
commitOffsets group@Group{..} req = do
  C.withMVar lock $ \() -> do
    validateOffsetcommit group req
    IO.readIORef state >>= \case
      CompletingRebalance -> throw (ErrorCodeException K.REBALANCE_IN_PROGRESS)
      Dead -> throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      _ -> do
        -- updateLatestHeartbeat
        -- TODO: udpate heartbeat
        topics <- Utils.forKaArrayM req.topics $ \K.OffsetCommitRequestTopicV0{..} -> do
          res <- GOM.storeOffsets metadataManager name partitions
          return $ K.OffsetCommitResponseTopicV0 {partitions = res, name = name}
        Utils.whenIORefEq storedMetadata False $ do
          Log.info $ "commited offsets on Empty Group, storing Empty Group:" <> Log.build group.groupId
          storeGroup group Map.empty
        return K.OffsetCommitResponseV0 {topics=topics}

validateOffsetcommit :: Group -> K.OffsetCommitRequestV2 -> IO ()
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
fetchOffsets :: Group -> K.OffsetFetchRequestV2 -> IO K.OffsetFetchResponseV2
fetchOffsets Group{..} req = do
  case K.unKaArray req.topics of
    Nothing -> do
      Log.debug $ "fetching all offsets in group:" <> Log.build req.groupId
      topics <- GOM.fetchAllOffsets metadataManager
      return K.OffsetFetchResponseV2 {topics=topics, errorCode=0}
    Just ts -> do
      topics <- V.forM ts $ \K.OffsetFetchRequestTopicV0{..} -> do
        res <- GOM.fetchOffsets metadataManager name partitionIndexes
        return $ K.OffsetFetchResponseTopicV0 {partitions = res, name = name}
      return K.OffsetFetchResponseV2 {topics=K.KaArray (Just topics), errorCode=0}

------------------- Group Overview(ListedGroup) -------------------------
overview :: Group -> IO K.ListedGroupV0
overview Group{..} = do
  pt <- fromMaybe "" <$> IO.readIORef protocolType
  return $ K.ListedGroupV0 {groupId=groupId, protocolType = pt}

------------------- Describe Group -------------------------
describe :: Group -> IO K.DescribedGroupV0
describe Group{..} = do
  C.withMVar lock $ \() -> do
    protocolType' <- fromMaybe "" <$> IO.readIORef protocolType
    protocolName' <- fromMaybe "" <$> IO.readIORef protocolName
    state' <- T.pack . show <$> IO.readIORef state
    members' <- H.toList members >>= M.mapM (\(_, member) -> describeMember member protocolName')
    return $ K.DescribedGroupV0 {
        protocolData=protocolName'
      , groupState= state'
      , errorCode=0
      , members=Utils.listToKaArray members'
      , groupId=groupId
      , protocolType=protocolType'
      }

describeMember :: Member -> T.Text -> IO K.DescribedGroupMemberV0
describeMember member@Member{..} protocol = do
  assignment' <- IO.readIORef assignment
  memberMetadata' <- getMemberMetadata member protocol
  return $ K.DescribedGroupMemberV0 {
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
