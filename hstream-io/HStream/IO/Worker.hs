
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
import qualified Data.UUID                 as UUID
import qualified Data.UUID.V4              as UUID
import           GHC.Stack                 (HasCallStack)

import qualified HStream.IO.IOTask         as IOTask
import qualified HStream.IO.Meta           as M
import           HStream.IO.Types
import qualified HStream.Logger            as Log
import           HStream.MetaStore.Types   (MetaHandle (..))
import qualified HStream.Server.HStreamApi as API
import qualified HStream.SQL.Codegen       as CG

newWorker :: MetaHandle -> ConnectorMetaConfig -> HStreamConfig -> IOOptions -> (T.Text -> IO Bool) -> IO Worker
newWorker mHandle connectorMetaCfg hsConfig options checkNode = do
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

createIOTaskFromSql :: Worker -> T.Text -> IO API.Connector
createIOTaskFromSql worker@Worker{..} sql = do
  (CG.CreateConnectorPlan cType cName cTarget ifNotExist cfg) <- CG.streamCodegen sql
  Log.info $ "CreateConnector CodeGen"
           <> ", connector type: " <> Log.buildText cType
           <> ", connector name: " <> Log.buildText cName
           <> ", config: "         <> Log.buildString (show cfg)
  checkNode_ worker cName
  taskId <- UUID.toText <$> UUID.nextRandom
  let IOOptions {..} = options
      taskType = if cType == "SOURCE" then SOURCE else SINK
      image = makeImage taskType cTarget options
      connectorConfig =
        J.object
          [ "hstream" J..= toTaskJson hsConfig taskId
          , "kv" J..= toTaskJson connectorMetaCfg taskId
          , "connector" J..= cfg
          ]
      taskInfo = TaskInfo
        { taskName = cName
        , taskType = if cType == "SOURCE" then SOURCE else SINK
        , taskConfig = TaskConfig image optTasksNetwork
        , connectorConfig = connectorConfig
        , originSql = sql
        }
  createIOTask worker taskId taskInfo
  return $ mkConnector cName (ioTaskStatusToText NEW)

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
  checkNode_ worker name
  ioTask <- getIOTask worker name
  IOTask.stopIOTask ioTask ifIsRunning force

startIOTask :: Worker -> T.Text -> IO ()
startIOTask worker name = do
  checkNode_ worker name
  getIOTask worker name >>= IOTask.startIOTask

getIOTask :: Worker -> T.Text -> IO IOTask
getIOTask Worker{..} name = do
  ioTasks <- C.readMVar ioTasksM
  case HM.lookup name ioTasks of
    Nothing     -> throwIO $ ConnectorNotExistException name
    Just ioTask -> return ioTask

deleteIOTask :: Worker -> T.Text -> IO ()
deleteIOTask worker@Worker{..} taskName = do
  checkNode_ worker taskName
  stopIOTask worker taskName True False
  M.deleteIOTaskMeta workerHandle taskName
  C.modifyMVar_ ioTasksM $ return . HM.delete taskName

checkNode_ :: Worker -> T.Text -> IO ()
checkNode_ Worker{..} name = do
  res <- checkNode name
  unless res . throwIO $ WrongNodeException "send HStream IO request to wrong node"

makeImage :: IOTaskType -> T.Text -> IOOptions -> T.Text
makeImage typ name IOOptions{..} =
  fromMaybe
    (throw $ UnimplementedConnectorException name)
    (HM.lookup name images)
  where images = if typ == SOURCE then optSourceImages else optSinkImages
