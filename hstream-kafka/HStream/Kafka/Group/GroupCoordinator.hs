{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}

module HStream.Kafka.Group.GroupCoordinator where

import qualified Control.Concurrent                       as C
import           Control.Exception                        (handle, throw)
import qualified Control.Monad                            as M
import qualified Data.HashTable.IO                        as H
import           Data.Int                                 (Int32)
import qualified Data.Text                                as T
import           HStream.Kafka.Common.KafkaException      (ErrorCodeException (ErrorCodeException))
import qualified HStream.Kafka.Common.Utils               as Utils
import           HStream.Kafka.Group.Group                (Group)
import qualified HStream.Kafka.Group.Group                as G
import           HStream.Kafka.Group.GroupMetadataManager (mkGroupMetadataManager)
import           HStream.Store                            (LDClient)
import qualified Kafka.Protocol.Encoding                  as K
import qualified Kafka.Protocol.Error                     as K
import qualified Kafka.Protocol.Message                   as K
import qualified Kafka.Protocol.Service                   as K

newtype GroupCoordinator = GroupCoordinator
  { groups :: C.MVar (Utils.HashTable T.Text Group)
  }

-- TODO: setup from metadata
mkGroupCoordinator :: IO GroupCoordinator
mkGroupCoordinator = do
  groups <- H.new >>= C.newMVar
  return $ GroupCoordinator {..}

joinGroup :: GroupCoordinator -> K.RequestContext -> LDClient -> Int32 -> K.JoinGroupRequestV0 -> IO K.JoinGroupResponseV0
joinGroup coordinator reqCtx ldClient serverId req = do
  handle (\((ErrorCodeException code)) -> makeErrorResponse code) $ do
    -- get or create group
    group <- getOrMaybeCreateGroup coordinator ldClient serverId req.groupId req.memberId

    -- join group
    G.joinGroup group reqCtx req
  where
    makeErrorResponse code = return $ K.JoinGroupResponseV0 {
        errorCode = code
      , generationId = -1
      , protocolName = ""
      , leader = ""
      , memberId = req.memberId
      , members = K.KaArray Nothing
      }

getOrMaybeCreateGroup :: GroupCoordinator -> LDClient -> Int32 -> T.Text -> T.Text -> IO Group
getOrMaybeCreateGroup GroupCoordinator{..} ldClient serverId groupId memberId = do
  C.withMVar groups $ \gs -> do
    H.lookup gs groupId >>= \case
      Nothing -> if T.null memberId
        then do
          metadataManager <- mkGroupMetadataManager ldClient serverId groupId
          ng <- G.newGroup groupId metadataManager
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

syncGroup :: GroupCoordinator -> K.SyncGroupRequestV0 -> IO K.SyncGroupResponseV0
syncGroup coordinator req@K.SyncGroupRequestV0{..} = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    group <- getGroup coordinator groupId
    G.syncGroup group req
  where makeErrorResponse code = return $ K.SyncGroupResponseV0 {
      errorCode = code,
      assignment = ""
    }

leaveGroup :: GroupCoordinator -> K.LeaveGroupRequestV0 -> IO K.LeaveGroupResponseV0
leaveGroup coordinator req = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    group <- getGroup coordinator req.groupId
    G.leaveGroup group req
  where makeErrorResponse code = return $ K.LeaveGroupResponseV0 {errorCode=code}

heartbeat :: GroupCoordinator -> K.HeartbeatRequestV0 -> IO K.HeartbeatResponseV0
heartbeat coordinator req = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    group <- getGroup coordinator req.groupId
    G.heartbeat group req
  where makeErrorResponse code = return $ K.HeartbeatResponseV0 {errorCode=code}

------------------- Commit Offsets -------------------------
commitOffsets :: GroupCoordinator -> LDClient -> Int32 -> K.OffsetCommitRequestV0 -> IO K.OffsetCommitResponseV0
commitOffsets coordinator ldClient serverId req = do
  handle (\(ErrorCodeException code) -> makeErrorResponse code) $ do
    -- TODO: check group and generation id(and if generationId < 0 then add self-management offsets strategy support)
    group <- getOrMaybeCreateGroup coordinator ldClient serverId req.groupId ""
    G.commitOffsets group req
  where makeErrorResponse code = return $ K.OffsetCommitResponseV0 {topics = Utils.mapKaArray (mapTopic code) req.topics}
        mapTopic code topic = K.OffsetCommitResponseTopicV0 {partitions=Utils.mapKaArray (mapPartition code) topic.partitions, name=topic.name}
        mapPartition code partition = K.OffsetCommitResponsePartitionV0 {errorCode=code, partitionIndex=partition.partitionIndex}

------------------- Fetch Offsets -------------------------
-- TODO: improve error report
fetchOffsets :: GroupCoordinator -> K.OffsetFetchRequestV0 -> IO K.OffsetFetchResponseV0
fetchOffsets coordinator req = do
  handle (\(ErrorCodeException _) -> makeErrorResponse) $ do
    group <- getGroup coordinator req.groupId
    G.fetchOffsets group req
  where makeErrorResponse = return $ K.OffsetFetchResponseV0 {topics = Utils.mapKaArray mapTopic req.topics}
        mapTopic topic = K.OffsetFetchResponseTopicV0 {partitions=Utils.mapKaArray mapPartition topic.partitionIndexes, name=topic.name}
        mapPartition partition = K.OffsetFetchResponsePartitionV0 {
          errorCode=0
          , partitionIndex=partition
          , metadata = Nothing
          , committedOffset = -1
        }

------------------- List Groups -------------------------
listGroups :: GroupCoordinator -> K.ListGroupsRequestV0 -> IO K.ListGroupsResponseV0
listGroups gc _ = do
  gs <- getAllGroups gc
  listedGroups <-  M.mapM G.overview gs
  return $ K.ListGroupsResponseV0 {errorCode=0, groups=Utils.listToKaArray listedGroups}

------------------- Describe Groups -------------------------
describeGroups :: GroupCoordinator -> K.DescribeGroupsRequestV0 -> IO K.DescribeGroupsResponseV0
describeGroups gc req = do
  getGroups gc (Utils.kaArrayToList req.groups) >>= \gs -> do
    listedGroups <- M.forM gs $ \case
      (gid, Nothing) -> return $ K.DescribedGroupV0 {
        protocolData=""
      , groupState=""
      , errorCode=K.GROUP_ID_NOT_FOUND
      , members=Utils.listToKaArray []
      , groupId=gid
      , protocolType=""
      }
      (_, Just g) -> G.describe g
    return $ K.DescribeGroupsResponseV0 {groups=Utils.listToKaArray listedGroups}

