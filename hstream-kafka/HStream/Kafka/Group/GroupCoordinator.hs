{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}

module HStream.Kafka.Group.GroupCoordinator where

import qualified Control.Concurrent                     as C
import           Control.Exception                      (handle, throw)
import qualified Control.Monad                          as M
import qualified Data.HashTable.IO                      as H
import qualified Data.Set                               as Set
import qualified Data.Text                              as T
import qualified Data.Vector                            as V
import           Data.Word                              (Word32)
import qualified HStream.Common.Server.Lookup           as Lookup
import qualified HStream.Common.Server.MetaData         as CM
import qualified HStream.Common.Server.TaskManager      as TM
import           HStream.Kafka.Common.KafkaException    (ErrorCodeException (ErrorCodeException))
import qualified HStream.Kafka.Common.Metrics           as Metrics
import qualified HStream.Kafka.Common.Utils             as Utils
import           HStream.Kafka.Group.Group              (Group)
import qualified HStream.Kafka.Group.Group              as G
import           HStream.Kafka.Group.GroupOffsetManager (mkGroupOffsetManager)
import qualified HStream.Kafka.Group.GroupOffsetManager as GOM
import qualified HStream.Logger                         as Log
import qualified HStream.MetaStore.Types                as Meta
import           HStream.Store                          (LDClient)
import qualified Kafka.Protocol.Encoding                as K
import qualified Kafka.Protocol.Error                   as K
import qualified Kafka.Protocol.Message                 as K
import qualified Kafka.Protocol.Service                 as K

data GroupCoordinator = GroupCoordinator
  { groups     :: C.MVar (Utils.HashTable T.Text Group)

  , metaHandle :: Meta.MetaHandle
  , serverId   :: Word32
  , ldClient   :: LDClient
  }

mkGroupCoordinator :: Meta.MetaHandle -> LDClient -> Word32 -> IO GroupCoordinator
mkGroupCoordinator metaHandle ldClient serverId = do
  groups <- H.new >>= C.newMVar
  return $ GroupCoordinator {..}

instance TM.TaskManager GroupCoordinator where
  resourceName _ = "Group"
  mkMetaId _ task = Lookup.kafkaResourceMetaId (Lookup.KafkaResGroup task)

  listLocalTasks gc = do
    C.withMVar gc.groups $ \gs -> do
      Set.fromList . map fst <$> H.toList gs

  listAllTasks gc = do
    V.fromList . map CM.groupId <$> Meta.listMeta @CM.GroupMetadataValue gc.metaHandle

  loadTaskAsync = loadGroupAndOffsets

  unloadTaskAsync = unloadGroup

------------------- Join Group -------------------------

joinGroup :: GroupCoordinator -> K.RequestContext -> K.JoinGroupRequest -> IO K.JoinGroupResponse
joinGroup coordinator reqCtx req = do
  handle (\((ErrorCodeException code)) -> makeErrorResponse code) $ do
    -- get or create group
    group <- getOrMaybeCreateGroup coordinator req.groupId req.memberId

    -- join group
    G.joinGroup group reqCtx req
  where
    makeErrorResponse code = return $ K.JoinGroupResponse {
        errorCode = code
      , generationId = -1
      , protocolName = ""
      , leader = ""
      , memberId = req.memberId
      , members = K.NonNullKaArray V.empty
      , throttleTimeMs = 0
      }

getOrMaybeCreateGroup :: GroupCoordinator -> T.Text -> T.Text -> IO Group
getOrMaybeCreateGroup GroupCoordinator{..} groupId memberId = do
  C.withMVar groups $ \gs -> do
    H.lookup gs groupId >>= \case
      Nothing -> if T.null memberId
        then do
          metadataManager <- mkGroupOffsetManager ldClient (fromIntegral serverId) groupId
          ng <- G.newGroup groupId metadataManager metaHandle
          H.insert gs groupId ng
          return ng
        else throw (ErrorCodeException K.UNKNOWN_MEMBER_ID)
      Just g -> return g

getGroup :: GroupCoordinator -> T.Text -> IO Group
getGroup GroupCoordinator{..} groupId = do
  C.withMVar groups $ \gs -> do
    H.lookup gs groupId >>= \case
      Nothing -> throw (ErrorCodeException K.GROUP_ID_NOT_FOUND)
      Just g -> return g

getAllGroups :: GroupCoordinator -> IO [Group]
getAllGroups GroupCoordinator{..} = do
  C.withMVar groups $ (fmap (map snd) . H.toList)

getGroups :: GroupCoordinator -> [T.Text] -> IO [(T.Text, Maybe Group)]
getGroups GroupCoordinator{..} ids = do
  C.withMVar groups $ \gs -> do
    M.forM ids $ \gid -> (gid,) <$> (H.lookup gs gid)

getGroupM :: GroupCoordinator -> T.Text -> IO (Maybe Group)
getGroupM GroupCoordinator{..} groupId = do
  C.withMVar groups $ \gs -> H.lookup gs groupId

syncGroup :: GroupCoordinator -> K.SyncGroupRequest -> IO K.SyncGroupResponse
syncGroup coordinator req@K.SyncGroupRequest{..} = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    group <- getGroup coordinator groupId
    G.syncGroup group req
  where makeErrorResponse code = return $ K.SyncGroupResponse {
      errorCode = code
    , assignment = ""
    , throttleTimeMs = 0
    }

leaveGroup :: GroupCoordinator -> K.LeaveGroupRequest -> IO K.LeaveGroupResponse
leaveGroup coordinator req = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    group <- getGroup coordinator req.groupId
    G.leaveGroup group req
  where makeErrorResponse code = return $ K.LeaveGroupResponse {errorCode=code, throttleTimeMs=0}

heartbeat :: GroupCoordinator -> K.HeartbeatRequest -> IO K.HeartbeatResponse
heartbeat coordinator req = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    group <- getGroup coordinator req.groupId
    G.heartbeat group req
  where makeErrorResponse code = return $ K.HeartbeatResponse {errorCode=code, throttleTimeMs=0}

------------------- Commit Offsets -------------------------
commitOffsets :: GroupCoordinator -> K.OffsetCommitRequest -> IO K.OffsetCommitResponse
commitOffsets coordinator req = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    Metrics.withLabel Metrics.totalOffsetCommitRequest req.groupId Metrics.incCounter
    group <- if req.generationId < 0 then do
      getOrMaybeCreateGroup coordinator req.groupId ""
    else do
      getGroup coordinator req.groupId
    G.commitOffsets group req
  where makeErrorResponse code = do
          Metrics.withLabel Metrics.totalFailedOffsetCommitRequest req.groupId Metrics.incCounter
          let resp = K.OffsetCommitResponse {topics = Utils.mapKaArray (mapTopic code) req.topics, throttleTimeMs=0}
          Log.fatal $ "commitOffsets error with code: " <> Log.build (show code)
                   <> "\n\trequest: " <> Log.build (show req)
                   <> "\n\tresponse: " <> Log.build (show resp)
          return resp
        mapTopic code topic = K.OffsetCommitResponseTopic {partitions=Utils.mapKaArray (mapPartition code) topic.partitions, name=topic.name}
        mapPartition code partition = K.OffsetCommitResponsePartition {errorCode=code, partitionIndex=partition.partitionIndex}

------------------- Fetch Offsets -------------------------
-- TODO: improve error report
fetchOffsets :: GroupCoordinator -> K.OffsetFetchRequest -> IO K.OffsetFetchResponse
fetchOffsets coordinator req = do
  handle (\(ErrorCodeException _) -> makeErrorResponse) $ do
    group <- getGroup coordinator req.groupId
    resp <- G.fetchOffsets group req
    return K.OffsetFetchResponse {topics = resp.topics, errorCode = 0, throttleTimeMs=0}
  where makeErrorResponse = return $ K.OffsetFetchResponse {
            topics = Utils.mapKaArray mapTopic req.topics
          , errorCode=0
          , throttleTimeMs=0}
        mapTopic topic = K.OffsetFetchResponseTopic {partitions=Utils.mapKaArray mapPartition topic.partitionIndexes, name=topic.name}
        mapPartition partition = K.OffsetFetchResponsePartition {
          errorCode=0
          , partitionIndex=partition
          , metadata = Nothing
          , committedOffset = -1
        }

------------------- List Groups -------------------------
listGroups :: GroupCoordinator -> K.ListGroupsRequest -> IO K.ListGroupsResponse
listGroups gc _ = do
  gs <- getAllGroups gc
  listedGroups <-  M.mapM G.overview gs
  return $ K.ListGroupsResponse {errorCode=0, groups=Utils.listToKaArray listedGroups, throttleTimeMs=0}

------------------- Describe Groups -------------------------
describeGroups :: GroupCoordinator -> K.DescribeGroupsRequest -> IO K.DescribeGroupsResponse
describeGroups gc req = do
  getGroups gc (Utils.kaArrayToList req.groups) >>= \gs -> do
    listedGroups <- M.forM gs $ \case
      (gid, Nothing) -> return $ K.DescribedGroup {
        protocolData=""
      , groupState=""
      , errorCode=K.GROUP_ID_NOT_FOUND
      , members=Utils.listToKaArray []
      , groupId=gid
      , protocolType=""
      }
      (_, Just g) -> G.describe g
    return $ K.DescribeGroupsResponse {groups=Utils.listToKaArray listedGroups, throttleTimeMs=0}

------------------- Load/Unload Group -------------------------
-- load group from meta store
loadGroupAndOffsets :: GroupCoordinator -> T.Text -> IO ()
loadGroupAndOffsets gc groupId = do
  offsetManager <- mkGroupOffsetManager gc.ldClient (fromIntegral gc.serverId) groupId
  GOM.loadOffsetsFromStorage offsetManager
  Meta.getMeta @CM.GroupMetadataValue groupId gc.metaHandle >>= \case
    Nothing -> do
      Log.warning $ "load group failed, group:" <> Log.build groupId <> " not found in metastore"
    Just value -> do
      Log.info $ "loading group from metastore, groupId:" <> Log.build groupId
        <> ", generationId:" <> Log.build value.generationId
      addGroupByValue gc value offsetManager

addGroupByValue :: GroupCoordinator -> CM.GroupMetadataValue -> GOM.GroupOffsetManager -> IO ()
addGroupByValue gc value offsetManager = do
  C.withMVar gc.groups $ \gs -> do
    H.lookup gs value.groupId >>= \case
      Nothing -> do
        ng <- G.newGroupFromValue value offsetManager gc.metaHandle
        H.insert gs value.groupId ng
      Just _ -> do
        Log.warning $ "load group failed, group:" <> Log.build value.groupId <> " is loaded"

unloadGroup :: GroupCoordinator -> T.Text -> IO ()
unloadGroup gc groupId = do
  C.withMVar gc.groups $ \gs -> do
    H.delete gs groupId
