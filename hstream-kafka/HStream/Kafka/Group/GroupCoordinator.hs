{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}

module HStream.Kafka.Group.GroupCoordinator where

import qualified Control.Concurrent                     as C
import           Control.Exception                      (throw)
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
import qualified HStream.Kafka.Common.Utils             as Utils
import           HStream.Kafka.Group.Group              (Group)
import qualified HStream.Kafka.Group.Group              as G
import           HStream.Kafka.Group.GroupOffsetManager (mkGroupOffsetManager)
import qualified HStream.Kafka.Group.GroupOffsetManager as GOM
import qualified HStream.Logger                         as Log
import qualified HStream.MetaStore.Types                as Meta
import           HStream.Store                          (LDClient)
import qualified Kafka.Protocol.Error                   as K

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
