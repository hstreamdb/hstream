
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.Worker where

import qualified Control.Concurrent        as C
import           Control.Exception         (catch, throw, throwIO)
import           Control.Monad             (forM_, unless)
import qualified Data.Aeson                as J
import qualified Data.HashMap.Strict       as HM
import           Data.IORef                (newIORef, readIORef)
import qualified Data.IORef                as C
import           Data.Maybe                (fromMaybe)
import qualified Data.Text                 as T
import           GHC.Stack                 (HasCallStack)

import qualified HStream.IO.IOTask         as IOTask
import qualified HStream.IO.Meta           as M
import           HStream.IO.Types
import qualified HStream.Logger            as Log
import           HStream.MetaStore.Types   (MetaHandle (..))
import qualified HStream.Server.HStreamApi as API
import qualified HStream.SQL.Codegen       as CG
import           HStream.Utils.Validation  (validateNameAndThrow)

newWorker :: MetaHandle -> HStreamConfig -> IOOptions -> IO Worker
newWorker mHandle hsConfig options = do
  Log.info $ "new Worker with hsConfig:" <> Log.buildString (show hsConfig)
  ioTasksM <- C.newMVar HM.empty
  monitorTid <- newIORef undefined
  let workerHandle = mHandle
  let worker = Worker {..}
  tid <- C.forkIO $ monitor worker
  C.writeIORef monitorTid tid
  return worker

closeWorker :: Worker -> IO ()
closeWorker Worker{..} = do
  tid <- readIORef monitorTid
  C.throwTo tid StopWorkerException

monitor :: Worker -> IO ()
monitor worker@Worker{..} = do
  catch monitorTasks $ \case
    StopWorkerException -> pure ()
  C.threadDelay 3000000
  monitor worker
  where
    monitorTasks = do
      ioTasks <- C.readMVar ioTasksM
      forM_ ioTasks IOTask.checkProcess

createIOTask :: HasCallStack =>  Worker -> T.Text -> TaskInfo -> IO ()
createIOTask Worker{..} taskId taskInfo@TaskInfo {..} = do
  let taskPath = optTasksPath options <> "/" <> taskId
  task <- IOTask.newIOTask taskId workerHandle taskInfo taskPath
  IOTask.initIOTask task
  IOTask.checkIOTask task
  M.createIOTaskMeta workerHandle taskName taskId taskInfo
  C.modifyMVar_ ioTasksM $ \ioTasks -> do
    case HM.lookup taskName ioTasks of
      Just _ -> throwIO $ ConnectorExistedException taskName
      Nothing -> do
        IOTask.startIOTask task
        return $ HM.insert taskName task ioTasks

showIOTask :: Worker -> T.Text -> IO (Maybe API.Connector)
showIOTask Worker{..} name =
  fmap convertTaskMeta <$> M.getIOTaskMeta workerHandle name

listIOTasks :: Worker -> IO [API.Connector]
listIOTasks Worker{..} = M.listIOTaskMeta workerHandle

stopIOTask :: Worker -> T.Text -> Bool -> Bool-> IO ()
stopIOTask worker name ifIsRunning force = do
  ioTask <- getIOTask worker name
  IOTask.stopIOTask ioTask ifIsRunning force

startIOTask :: Worker -> T.Text -> IO ()
startIOTask worker name = do
  getIOTask worker name >>= IOTask.startIOTask

getIOTask :: Worker -> T.Text -> IO IOTask
getIOTask Worker{..} name = do
  ioTasks <- C.readMVar ioTasksM
  case HM.lookup name ioTasks of
    Nothing     -> throwIO $ ConnectorNotExistException name
    Just ioTask -> return ioTask

deleteIOTask :: Worker -> T.Text -> IO ()
deleteIOTask worker@Worker{..} taskName = do
  stopIOTask worker taskName True False
  M.deleteIOTaskMeta workerHandle taskName
  C.modifyMVar_ ioTasksM $ return . HM.delete taskName

makeImage :: IOTaskType -> T.Text -> IOOptions -> T.Text
makeImage typ name IOOptions{..} =
  fromMaybe
    (throw $ UnimplementedConnectorException name)
    (HM.lookup name images)
  where images = if typ == SOURCE then optSourceImages else optSinkImages
