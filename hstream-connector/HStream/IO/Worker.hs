{-# LANGUAGE DeriveGeneric, OverloadedStrings, DeriveAnyClass, DerivingStrategies, LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module HStream.IO.Worker where

import qualified HStream.IO.IOTask as IOTask
import qualified Data.Map.Strict as M
import qualified HStream.IO.KvStorage as Kv
import qualified Data.Text as T
import qualified Data.Aeson as J
import qualified Data.ByteString.Lazy as BSL
import Control.Concurrent (MVar, newMVar, readMVar, modifyMVar_)
import qualified Data.ByteString as BS
import qualified Data.UUID.V4 as UUID
import qualified Data.UUID as UUID
import qualified Data.Text.IO as T
import qualified Data.ByteString.Lazy.Char8 as BSLC

data Worker
  = Worker {
    workerId :: T.Text,
    ioTasksM :: MVar (M.Map T.Text IOTask.IOTask),
    storage :: Kv.KvStorage
    -- monitorThread :: ThreadId
  }

newWorker :: IO Worker
newWorker = do
  worker@Worker{..} <- Worker <$> (UUID.toText <$> UUID.nextRandom) <*> newMVar M.empty <*> Kv.newKvStorage
  -- let workerKay = "/workers/" <> workerId
  -- Kv.set storage workerKay (BSL.toStrict . J.encode $ ([] :: [T.Text]))
  return worker

-- runMonitor :: Worker -> IO ()
-- runMonitor Worker{..} = forever $ do
--   threadDelay 10000
--   withMVar ioTasksM $ \ioTasks -> do

createIOTaskFromDir :: Worker -> T.Text -> IO T.Text 
createIOTaskFromDir worker dir = do
  tt <- head . words <$> readFile (T.unpack (dir <> "/type"))
  let cType = if tt == "full" then IOTask.Full else IOTask.Incremental 10000 1
  srcImage <- head . T.words <$> T.readFile (T.unpack (dir <> "/src/image"))
  Just srcConfig <- J.decodeFileStrict (T.unpack (dir <> "/src/config.json"))
  Just srcCatalog <- J.decodeFileStrict (T.unpack (dir <> "/src/catalog.json"))
  Just srcState <- J.decodeFileStrict (T.unpack (dir <> "/src/state.json"))
  dstImage <- head . T.words <$> T.readFile (T.unpack (dir <> "/dst/image"))
  Just dstConfig <- J.decodeFileStrict (T.unpack (dir <> "/dst/config.json"))
  Just dstCatalog <- J.decodeFileStrict (T.unpack (dir <> "/dst/catalog.json"))
  let cSrcInfo = IOTask.SrcInfo {..}
  let cDstInfo = IOTask.DstInfo {..}
  BSLC.putStrLn $ J.encode IOTask.SrcInfo {..}
  createIOTask worker CreateIOTaskInfo{..}

createIOTaskFromConfig :: Worker -> T.Text -> IO T.Text
createIOTaskFromConfig = undefined

data CreateIOTaskInfo = CreateIOTaskInfo {
    cType :: IOTask.IOTaskType,
    cSrcInfo :: IOTask.SrcInfo,
    cDstInfo :: IOTask.DstInfo
  }

createIOTask :: Worker -> CreateIOTaskInfo -> IO T.Text
createIOTask worker@Worker{..} CreateIOTaskInfo{..} = do
  taskId <- UUID.toText <$> UUID.nextRandom
  let taskInfoKey = "/tasks/" <> taskId <> "/info"
  task <- IOTask.newIOTask taskId cType cSrcInfo cDstInfo
  -- write informations of the task to kv storage
  Kv.set storage taskInfoKey $ BSL.toStrict (J.encode (cSrcInfo, cDstInfo))
  -- init IOTask
  IOTask.initIOTask task
  -- run IOTask
  IOTask.startIOTask task (updateState worker taskId)
  modifyMVar_ ioTasksM $ pure . M.insert taskId task
  return taskId

-- for IOTask
updateState :: Worker -> T.Text -> J.Value -> IO ()
updateState Worker{..} taskId state = do
  let stateKey = "/tasks/" <> taskId <> "/" <> "latest_state"
  Kv.set storage stateKey . BSL.toStrict $ J.encode state

showIOTask :: Worker -> T.Text -> IO (Maybe J.Value)
showIOTask Worker{..} taskId = do
  ioTasks <- readMVar ioTasksM
  case M.lookup taskId ioTasks of
    Nothing  -> return Nothing
    Just task -> do
      status <- IOTask.getStatus task
      return . Just $ J.toJSON (taskId, status)

data IOTaskItem
  = IOTaskItem
      { taskId :: T.Text
      , status :: IOTask.IOTaskStatus
      , latestState :: J.Value
      } deriving (Show)

listIOTasks :: Worker -> IO [IOTaskItem]
listIOTasks Worker{..} = do
  ioTasks <- readMVar ioTasksM
  let formatIOTasks = \(taskId, task) -> IOTaskItem taskId <$> IOTask.getStatus task <*> IOTask.getLatestState task
  mapM formatIOTasks $ M.toList ioTasks

stopIOTask :: Worker -> T.Text -> IO ()
stopIOTask worker taskId = do
  putStrLn "====================== XX stop"
  getIOTask worker taskId >>= \case
    Nothing -> fail $ "TODO: invalid taskid: " ++ T.unpack taskId
    Just task -> IOTask.stopIOTask task

startIOTask :: Worker -> T.Text -> IO ()
startIOTask worker taskId = do
  putStrLn "startIOTask"
  getIOTask worker taskId >>= \case
    Nothing -> fail $ "TODO: invalid taskid: " ++ T.unpack taskId
    Just task -> IOTask.startIOTask task (updateState worker taskId)

getIOTask :: Worker -> T.Text -> IO (Maybe IOTask.IOTask)
getIOTask Worker{..} taskId = M.lookup taskId <$> readMVar ioTasksM

