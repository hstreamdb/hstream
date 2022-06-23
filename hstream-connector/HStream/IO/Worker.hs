{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TupleSections      #-}

module HStream.IO.Worker where

import           Control.Concurrent (MVar, modifyMVar_, newMVar, readMVar)
import qualified Data.Aeson         as J
import qualified Data.Map.Strict    as M
import qualified Data.Text          as T
import qualified Data.Text.IO       as T
import qualified Data.UUID          as UUID
import qualified Data.UUID.V4       as UUID
import qualified HStream.IO.IOTask  as IOTask
import qualified HStream.IO.Storage as S
import           HStream.IO.Types
import           ZooKeeper.Types    (ZHandle)

data Worker
  = Worker {
    ioTasksM :: MVar (M.Map T.Text IOTask.IOTask),
    storage  :: S.Storage
    -- monitorThread :: ThreadId
  }

newWorker :: ZHandle -> IO Worker
newWorker zk = do
  Worker <$> newMVar M.empty <*> S.newZkStorage zk

-- runMonitor :: Worker -> IO ()
-- runMonitor Worker{..} = forever $ do
--   threadDelay 10000
--   withMVar ioTasksM $ \ioTasks -> do

-- TODO: tmply implement, should be passed by server
createIOTaskFromDir :: Worker -> T.Text -> IO T.Text
createIOTaskFromDir worker dir = do
  tt <- head . words <$> readFile (T.unpack (dir <> "/type"))
  let taskType = if tt == "source" then Source else Sink
  taskImage <- head . T.words <$> T.readFile (T.unpack (dir <> "/image"))
  Just connectorConfig <- J.decodeFileStrict (T.unpack (dir <> "/config.json"))
  taskId <- UUID.toText <$> UUID.nextRandom
  let hstreamConfig = HStreamConfig
                        { serviceUrl = "127.0.0.1:6570"
                        , taskId = taskId
                        , zkUrl = "127.0.0.1:53391"
                        , zkKvPath = "/hstream/io/tasksKv/" <> taskId
                        }
      taskConfig = TaskConfig hstreamConfig connectorConfig
      taskInfo = TaskInfo {..}
  createIOTask worker taskInfo

createIOTask :: Worker -> TaskInfo -> IO T.Text
createIOTask Worker{..} taskInfo@TaskInfo {..} = do
  let HStreamConfig{..} = hstreamConfig taskConfig
  task <- IOTask.newIOTask taskId storage taskInfo
  S.createIOTask storage taskId taskInfo
  IOTask.startIOTask task
  modifyMVar_ ioTasksM $ pure . M.insert taskId task
  return taskId

showIOTask :: Worker -> T.Text -> IO (Maybe J.Value)
showIOTask Worker{..} taskId = do
  ioTasks <- readMVar ioTasksM
  case M.lookup taskId ioTasks of
    Nothing  -> return Nothing
    Just task -> do
      status <- IOTask.getStatus task
      return . Just $ J.toJSON (taskId, status)

listIOTasks :: Worker -> IO [IOTaskItem]
listIOTasks Worker{..} = S.listIOTasks storage

stopIOTask :: Worker -> T.Text -> IO ()
stopIOTask worker taskId = do
  getIOTask worker taskId >>= \case
    Nothing   -> fail $ "TODO: invalid taskid: " ++ T.unpack taskId
    Just task -> IOTask.stopIOTask task

startIOTask :: Worker -> T.Text -> IO ()
startIOTask worker taskId = do
  putStrLn "startIOTask"
  getIOTask worker taskId >>= \case
    Nothing   -> fail $ "TODO: invalid taskid: " ++ T.unpack taskId
    Just task -> IOTask.startIOTask task

getIOTask :: Worker -> T.Text -> IO (Maybe IOTask.IOTask)
getIOTask Worker{..} taskId = M.lookup taskId <$> readMVar ioTasksM

