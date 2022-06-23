{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TemplateHaskell    #-}

module HStream.IO.IOTask where

import           Control.Concurrent   (MVar, ThreadId, forkIO, killThread,
                                       modifyMVar_, newMVar, readMVar,
                                       threadDelay)
import           Control.Exception    (SomeException, bracket, catch)
import qualified Data.Aeson           as J
import qualified Data.ByteString.Lazy as BSL
import           Data.IORef           (IORef, newIORef, readIORef, writeIORef)
import qualified Data.Text            as T
import qualified HStream.IO.Storage   as S
import           HStream.IO.Types
import qualified HStream.Logger       as Log
import           RIO.Directory        (createDirectoryIfMissing)
import qualified System.Process.Typed as TP

data IOTask = IOTask
  { taskId   :: T.Text,
    taskInfo :: TaskInfo,
    storage  :: S.Storage,
    tidM     :: IORef (Maybe ThreadId),
    statusM  :: MVar IOTaskStatus
  }

newIOTask :: T.Text -> S.Storage -> TaskInfo -> IO IOTask
newIOTask taskId storage info@TaskInfo{..} = do
  let taskPath = getTaskPath taskId
      configPath = taskPath ++ "/config.json"
  -- create task files
  createDirectoryIfMissing True taskPath
  BSL.writeFile configPath (J.encode taskConfig)
  IOTask taskId info storage <$> newIORef Nothing <*> newMVar NEW

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
        updateStatus task (\_ -> pure FAILED)

startIOTask :: IOTask -> IO ()
startIOTask task@IOTask{..} = do
  updateStatus task $ \case
    status | elem status [NEW, FAILED, STOPPED]  -> do
      tid <- forkIO . printException $ runIOTask task
      writeIORef tidM $ Just tid
      return RUNNING
    _ -> fail "invalid status"

stopIOTask :: IOTask -> IO ()
stopIOTask task@IOTask{..} = do
  putStrLn "--------------------------XX"
  updateStatus task $ \case
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


updateStatus :: IOTask -> (IOTaskStatus -> IO IOTaskStatus) -> IO ()
updateStatus IOTask{..} action = do
  modifyMVar_ statusM $ \status -> do
    ts <- action status
    S.updateStatus storage taskId ts
    return ts
