{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.Storage where

import           Control.Monad           (forM)
import qualified Data.Aeson              as J
import qualified Data.Text               as T
import qualified Data.Text.Lazy          as LT
import qualified Data.Text.Lazy.Encoding as T
import           HStream.IO.Types
import           HStream.IO.ZkKv         (ZkKv (..))
import           ZooKeeper.Types

data Storage where
    Storage :: Kv kv => {
      tasksKv :: kv,
      statusKv :: kv
    } -> Storage


newZkStorage :: ZHandle -> IO Storage
newZkStorage zk = do
  let rootPath = "/hstream/io"
      tasksKv = ZkKv zk $ rootPath <> "/tasks"
      statusKv = ZkKv zk $ rootPath <> "/tasksStatus"
  return Storage {..}

createIOTask :: Storage -> T.Text -> TaskInfo -> IO ()
createIOTask Storage{..} taskId taskInfo = do
  set tasksKv taskId $ J.encode taskInfo
  set statusKv taskId $ ioTaskStatusToBS NEW


listIOTasks :: Storage -> IO [IOTaskItem]
listIOTasks s@Storage{..} = do
  ids <- keys tasksKv
  forM ids $ showIOTask s

showIOTask :: Storage -> T.Text -> IO IOTaskItem
showIOTask Storage{..} taskId = do
  statusBS <- get statusKv taskId
  let status = LT.toStrict . T.decodeUtf8 $ statusBS
  return IOTaskItem
    { iiTaskId = taskId
    , iiStatus = status
    }

updateStatus :: Storage -> T.Text -> IOTaskStatus -> IO ()
updateStatus Storage{..} taskId status = set statusKv taskId $ ioTaskStatusToBS status
