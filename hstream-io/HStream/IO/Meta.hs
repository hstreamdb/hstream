{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE TupleSections         #-}

module HStream.IO.Meta where

import qualified Data.Text                 as T
import           GHC.Stack                 (HasCallStack)

import           HStream.IO.Types
import qualified HStream.IO.Types          as Types
import           HStream.MetaStore.Types   (MetaHandle, MetaStore (..))
import qualified HStream.Server.HStreamApi as API

createIOTaskMeta :: HasCallStack => MetaHandle -> T.Text -> T.Text -> TaskInfo -> IO ()
createIOTaskMeta h taskName taskId taskInfo = do
  -- FIXME: use multiops
  insertMeta taskId (TaskMeta taskInfo NEW) h
  insertMeta taskName (TaskIdMeta taskId) h

listIOTaskMeta :: MetaHandle -> IO [API.Connector]
listIOTaskMeta h = do
  map convertTaskMeta . filter (\TaskMeta{..} -> taskStateMeta /= DELETED) <$> listMeta @TaskMeta h

getIOTaskMeta :: MetaHandle -> T.Text -> IO (Maybe TaskMeta)
getIOTaskMeta h tid = getMeta tid h

getIOTaskFromName :: MetaHandle -> T.Text -> IO (Maybe (T.Text, TaskMeta))
getIOTaskFromName h name = do
  getMeta @TaskIdMeta name h >>= \case
    Nothing             -> pure Nothing
    Just TaskIdMeta{..} -> fmap (taskIdMeta, ) <$> getIOTaskMeta h taskIdMeta

updateStatusInMeta :: MetaHandle -> T.Text -> IOTaskStatus -> IO ()
updateStatusInMeta h taskId status =
  updateMetaWith taskId (\(Just tm) -> tm {taskStateMeta = status}) Nothing h

deleteIOTaskMeta :: MetaHandle -> T.Text -> IO ()
deleteIOTaskMeta h name = do
  getMeta name h >>= \case
    Nothing -> return ()
    Just (TaskIdMeta tid) -> do
      -- FIXME: use multiops
      deleteMeta @TaskIdMeta name Nothing h
      updateStatusInMeta h tid DELETED

mapKvKey :: T.Text -> T.Text -> T.Text
mapKvKey taskId key = taskId <> "_" <> key

getTaskKv :: MetaHandle -> T.Text -> T.Text -> IO (Maybe T.Text)
getTaskKv h taskId key = fmap Types.value <$> getMeta mappedKey h
  where mappedKey = mapKvKey taskId key

setTaskKv :: MetaHandle -> T.Text -> T.Text -> T.Text -> IO ()
setTaskKv h taskId key val = do
  getTaskKv h taskId key >>= \case
    Nothing -> insertMeta mappedKey (TaskKvMeta val) h
    Just _  -> updateMeta mappedKey (TaskKvMeta val) Nothing h
  where mappedKey = mapKvKey taskId key
