{-# LANGUAGE DerivingStrategies    #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}

module HStream.IO.Meta where

import qualified Data.Text                 as T
import           GHC.Stack                 (HasCallStack)

import           HStream.IO.Types
import           HStream.MetaStore.Types   (MetaHandle, MetaStore (..))
import qualified HStream.Server.HStreamApi as API

createIOTaskMeta :: HasCallStack => MetaHandle -> T.Text -> T.Text -> TaskInfo -> IO ()
createIOTaskMeta h taskName taskId taskInfo = do
  -- FIXME: use multiops
  insertMeta taskId (TaskMeta taskInfo NEW) h
  insertMeta taskName (TaskIdMeta taskId) h

listIOTaskMeta :: MetaHandle -> IO [API.Connector]
listIOTaskMeta h = map convertTaskMeta <$> listMeta @TaskMeta h

getIOTaskMeta :: MetaHandle -> T.Text -> IO (Maybe TaskMeta)
getIOTaskMeta h tid = getMeta tid h

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
