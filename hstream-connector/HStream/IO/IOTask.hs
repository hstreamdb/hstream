{-# LANGUAGE DeriveGeneric, OverloadedStrings, DeriveAnyClass, DerivingStrategies, LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.IOTask where

import qualified Data.ByteString.Lazy as BSL
import qualified Data.Aeson as Aeson
import qualified System.Process.Typed as TP
import RIO.Directory (createDirectoryIfMissing)
import qualified Data.Text as T
import Data.Aeson.TH (deriveJSON, defaultOptions)
import Data.IORef (newIORef, writeIORef, IORef, readIORef)
import Control.Exception (catch, SomeException, bracket)
import qualified HStream.Logger                  as Log
import Control.Concurrent (MVar, ThreadId, newMVar, readMVar, modifyMVar_, forkIO, killThread, threadDelay)

data IOTaskType = Source | Sink
  deriving (Show, Eq)

$(deriveJSON defaultOptions ''IOTaskType)

data TaskInfo = TaskInfo { taskImage :: T.Text, taskType :: IOTaskType, taskConfig :: Aeson.Value }

$(deriveJSON defaultOptions ''TaskInfo)


data IOTaskStatus
  = NEW
  | STARTED
  | RUNNING
  | COMPLETED
  | FAILED
  | STOPPED
  deriving (Show, Eq)

$(deriveJSON defaultOptions ''IOTaskStatus)

data IOTask = IOTask
  { taskId :: T.Text,
    taskInfo :: TaskInfo,
    tidM    :: IORef (Maybe ThreadId),
    statusM :: MVar IOTaskStatus
  }

data WriteMode = Append | Overrider | Append_Dedup
  deriving (Show, Eq)

newIOTask :: T.Text -> TaskInfo -> IO IOTask
newIOTask taskId info@TaskInfo{..} = do
  let taskPath = getTaskPath taskId
      configPath = taskPath ++ "/config.json"
      kvStorePath = taskPath ++ "/kv.json"
  -- create task files
  createDirectoryIfMissing True taskPath
  BSL.writeFile configPath (Aeson.encode taskConfig)
  BSL.writeFile kvStorePath "{}"
  IOTask taskId info <$> newIORef Nothing <*> newMVar NEW

getStatus :: IOTask -> IO IOTaskStatus
getStatus IOTask{..} = readMVar statusM

getTaskPath :: T.Text -> FilePath
getTaskPath taskId = "/tmp/io/tasks/" ++ T.unpack taskId

getDockerName :: T.Text -> T.Text
getDockerName = ("IOTASK_" <>)

runIOTask :: IOTask -> IO ()
runIOTask IOTask{..} = do
  -- TODO: offset, update state
  -- getLatestState ioTask >>= BSL.writeFile srcStatePath . Aeson.encode

  putStrLn $ "taskCmd: " ++ taskCmd
  -- modifyMVar_ statusM (\_ -> pure RUNNING)
  bracket (TP.startProcess taskProcessConfig) TP.stopProcess $ \tp -> do
    exitCode <- TP.waitExitCode tp
    Log.info $ "task:" <> Log.buildText taskId <> " exited with " <> Log.buildString (show exitCode)
    return ()
  where
    TaskInfo {..} = taskInfo
    taskPath = getTaskPath taskId
    taskCmd = concat [
        "docker run --rm -i --network=host",
        " --name ", T.unpack (getDockerName taskId),
        " -v " , taskPath, ":/data",
        " " , T.unpack taskImage,
        " >> ", taskPath, "/log", " 2>&1"
      ]
    taskProcessConfig = TP.setStdin TP.closed
      . TP.setStdout TP.createPipe
      . TP.setStderr TP.closed
      $ TP.shell taskCmd

runIOTaskWithRetry :: IOTask -> IO ()
runIOTaskWithRetry task@IOTask{..} = 
  runWithRetry (1 :: Int)
  where
    duration = 1000
    maxRetry = 1
    runWithRetry retry = do
      runIOTask task
      if retry < maxRetry
      then do
        threadDelay $ duration * 1000
        runWithRetry $ retry + 1
      else
        modifyMVar_ statusM (\_ -> pure FAILED)

startIOTask :: IOTask -> IO ()
startIOTask task@IOTask{..} = do
  modifyMVar_  statusM $ \case
    status | elem status [NEW, FAILED, STOPPED]  -> do
      tid <- forkIO . printException $ runIOTask task
      writeIORef tidM $ Just tid
      return RUNNING
    _ -> fail "invalid status"

stopIOTask :: IOTask -> IO ()
stopIOTask IOTask{..} = do
  putStrLn "--------------------------XX"
  modifyMVar_  statusM $ \case
    RUNNING -> do
      _ <- TP.runProcess killProcessConfig
      readIORef tidM >>= maybe (pure ()) killThread
      return STOPPED
    _ -> fail "TODO: status error"
  where
    killDockerCmd = "docker kill " ++ T.unpack (getDockerName taskId)
    killProcessConfig = TP.shell killDockerCmd

printException :: IO () -> IO ()
printException action = catch action (\(e :: SomeException) -> print e)
