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
import           Control.Monad        (unless, when, void)
import qualified Data.Aeson           as J
import qualified Data.ByteString.Lazy as BSL
import           Data.IORef           (IORef, newIORef, readIORef, writeIORef)
import qualified Data.Text            as T
import qualified HStream.IO.Storage   as S
import           HStream.IO.Types
import qualified HStream.Logger       as Log
import           RIO.Directory        (createDirectoryIfMissing)
import qualified System.Process.Typed as TP
import GHC.IO.Handle.Types (Handle)
import qualified HStream.IO.Messages as MSG
import GHC.IO.Handle (hFlush)

data IOTask = IOTask
  { taskId   :: T.Text,
    taskInfo :: TaskInfo,
    taskPath :: FilePath,
    storage  :: S.Storage,
    process' :: IORef (Maybe (TP.Process Handle () ())),
    statusM  :: MVar IOTaskStatus
  }

newIOTask :: T.Text -> S.Storage -> TaskInfo -> T.Text -> IO IOTask
newIOTask taskId storage info taskPath = do
  IOTask taskId info (T.unpack taskPath) storage
    <$> newIORef Nothing
    <*> newMVar NEW

initIOTask :: IOTask -> IO ()
initIOTask IOTask{..} = do
  let configPath = taskPath ++ "/config.json"
  -- create task files
  createDirectoryIfMissing True taskPath
  BSL.writeFile configPath (J.encode connectorConfig)

getDockerName :: T.Text -> T.Text
getDockerName = ("IOTASK_" <>)

runIOTask :: IOTask -> IO ()
runIOTask IOTask{..} = do
  Log.info $ "taskCmd: " <> Log.buildString taskCmd
  tp <- TP.startProcess taskProcessConfig
  writeIORef process' (Just tp)
  return ()
  where
    TaskInfo {..} = taskInfo
    TaskConfig {..} = taskConfig
    taskCmd = concat [
        "docker run --rm -i --network=host",
        " --name ", T.unpack (getDockerName taskId),
        " -v " , taskPath, ":/data",
        " " , T.unpack tcImage,
        " >> ", taskPath, "/log", " 2>&1"
      ]
    taskProcessConfig = TP.setStdin TP.createPipe
      . TP.setStdout TP.closed
      . TP.setStderr TP.closed
      $ TP.shell taskCmd

-- runIOTaskWithRetry :: IOTask -> IO ()
-- runIOTaskWithRetry task@IOTask{..} =
--   runWithRetry (1 :: Int)
--   where
--     duration = 1000
--     maxRetry = 1
--     runWithRetry retry = do
--       runIOTask task
--       if retry < maxRetry
--       then do
--         threadDelay $ duration * 1000
--         runWithRetry $ retry + 1
--       else
--         updateStatus task (\_ -> pure FAILED)

startIOTask :: IOTask -> IO ()
startIOTask task = do
  updateStatus task $ \case
    status | elem status [NEW, FAILED, STOPPED]  -> do
      runIOTask task
      return RUNNING
    _ -> fail "invalid status"

stopIOTask :: IOTask -> Bool -> Bool -> IO ()
stopIOTask task@IOTask{..} ifIsRunning force = do
  updateStatus task $ \case
    RUNNING -> do
      if force
      then do
        readIORef process' >>= maybe (pure ()) TP.stopProcess
        void $ TP.runProcess killProcessConfig
      else do
        Just tp <- readIORef process'
        let stdin = TP.getStdin tp
        BSL.hPut stdin (J.encode MSG.InputCommandStop)
        hFlush stdin
        _ <- TP.waitExitCode tp
        TP.stopProcess tp
        writeIORef process' Nothing
      return STOPPED
    s -> do
      unless ifIsRunning $ fail "task is not RUNNING"
      return s
  where
    killDockerCmd = "docker kill " ++ T.unpack (getDockerName taskId)
    killProcessConfig = TP.shell killDockerCmd

printException :: IO () -> IO ()
printException action = catch action (\(e :: SomeException) -> print e)

updateStatus :: IOTask -> (IOTaskStatus -> IO IOTaskStatus) -> IO ()
updateStatus IOTask{..} action = do
  modifyMVar_ statusM $ \status -> do
    ts <- action status
    when (ts /= status) $ S.updateStatus storage taskId ts
    return ts

checkProcess :: IOTask -> IO ()
checkProcess ioTask@IOTask{..} = do
  updateStatus ioTask $ \status -> do
    case status of
      RUNNING -> do
        Just tp <- readIORef process'
        TP.getExitCode tp >>= \case
          Nothing -> return status
          Just _ -> return FAILED
      _ -> return status

