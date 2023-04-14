{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}

module HStream.IO.IOTask where

import qualified Control.Concurrent         as C
import           Control.Exception          (throwIO)
import qualified Control.Exception          as E
import           Control.Monad              (forever, msum, unless, void, when)
import qualified Data.Aeson                 as J
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.ByteString.Lazy.Char8 as BSLC
import           Data.IORef                 (newIORef, readIORef, writeIORef)
import qualified Data.Text                  as T
import qualified Data.Text.Lazy             as TL
import qualified GHC.IO.Handle              as IO
import           System.Directory           (createDirectoryIfMissing,
                                             removeDirectoryRecursive)
import qualified System.Process.Typed       as TP

import qualified Control.Concurrent.Async   as Async
import qualified Data.Aeson.Text            as J
import           Data.Maybe                 (isNothing)
import qualified HStream.IO.Messages        as MSG
import qualified HStream.IO.Meta            as M
import           HStream.IO.Types
import qualified HStream.Logger             as Log
import qualified HStream.MetaStore.Types    as M
import qualified HStream.Stats              as Stats
import qualified HStream.Utils              as Utils

newIOTask :: T.Text -> M.MetaHandle -> Stats.StatsHolder -> TaskInfo -> T.Text -> IO IOTask
newIOTask taskId taskHandle taskStatsHolder taskInfo path = do
  process' <- newIORef Nothing
  statusM  <- C.newMVar NEW
  let taskPath = T.unpack path
  return IOTask {..}

initIOTask :: IOTask -> Bool -> IO ()
initIOTask task@IOTask{..} clean = do
  when clean $ cleanLocalIOTask task
  let configPath = taskPath ++ "/config.json"
      TaskInfo{..} = taskInfo
  -- create task files
  createDirectoryIfMissing True taskPath
  BSL.writeFile configPath (J.encode connectorConfig)

getDockerName :: T.Text -> T.Text
getDockerName = ("IOTASK_" <>)

runIOTask :: IOTask -> IO ()
runIOTask ioTask@IOTask{..} = do
  Log.info $ "taskCmd: " <> Log.buildString taskCmd
  tp <- TP.startProcess taskProcessConfig
  writeIORef process' (Just tp)
  _ <- C.forkIO $
    E.catch
      (handleStdout ioTask (TP.getStdout tp) (TP.getStdin tp))
      (\(e :: E.SomeException) -> Log.info $ "handleStdout exited:" <> Log.buildString (show e))
  return ()
  where
    TaskInfo {..} = taskInfo
    TaskConfig {..} = taskConfig
    taskCmd = concat [
        "docker run --rm -i",
        " --network=", T.unpack tcNetwork,
        " --name ", T.unpack (getDockerName taskId),
        " -v " , taskPath, ":/data",
        " " , T.unpack tcImage,
        " run",
        " --config /data/config.json"
      ]
    taskProcessConfig = TP.setStdin TP.createPipe
      . TP.setStdout TP.createPipe
      . TP.setStderr TP.createPipe
      $ TP.shell taskCmd

handleStdout :: IOTask -> IO.Handle -> IO.Handle -> IO ()
handleStdout ioTask@IOTask{..} hStdout hStdin = forever $ do
  line <- BS.hGetLine hStdout
  let logPath = taskPath ++ "/stdout.log"
  BS.appendFile logPath (line <> "\n")
  case J.eitherDecode (BSL.fromStrict line) of
    Left _ -> pure () -- Log.info $ "decode err:" <> Log.buildString err
    Right msg -> do
      Log.info $ "connectorMsg:" <> Log.buildString (show msg)
      resp <- handleConnectorRequest ioTask msg
      Log.info $ "ConnectorRes:" <> Log.buildString (show resp)
      BSLC.hPutStrLn hStdin (J.encode resp)
      IO.hFlush hStdin

handleConnectorRequest :: IOTask -> MSG.ConnectorRequest -> IO MSG.ConnectorResponse
handleConnectorRequest ioTask MSG.ConnectorRequest{..} = do
  MSG.ConnectorResponse crId <$> handleConnectorMessage ioTask crMessage

handleConnectorMessage :: IOTask -> MSG.ConnectorMessage -> IO J.Value
handleConnectorMessage IOTask{..} (MSG.KvGet MSG.KvGetMessage{..}) = J.toJSON <$> M.getTaskKv taskHandle taskId kgKey
handleConnectorMessage IOTask{..} (MSG.KvSet MSG.KvSetMessage{..}) = J.Null <$ M.setTaskKv taskHandle taskId ksKey ksValue
handleConnectorMessage IOTask{..} (MSG.Report MSG.ReportMessage{..}) = do
  Stats.connector_stat_add_delivered_in_records taskStatsHolder cTaskName (fromIntegral rmDeliveredRecords)
  Stats.connector_stat_add_delivered_in_bytes taskStatsHolder cTaskName (fromIntegral rmDeliveredBytes)
  pure J.Null
  where cTaskName = Utils.textToCBytes (taskName taskInfo)

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
    status -> throwIO $ InvalidStatusException status

stopIOTask :: IOTask -> Bool -> Bool -> IO ()
stopIOTask task@IOTask{..} ifIsRunning force = do
  updateStatus task $ \case
    RUNNING -> do
      if force
      then do
        readIORef process' >>= maybe (pure ()) TP.stopProcess
        killIOTask task
      else do
        Just tp <- readIORef process'
        let stdin = TP.getStdin tp
        BSLC.hPutStrLn stdin (J.encode MSG.InputCommandStop <> "\n")
        IO.hFlush stdin
        result <- tryWaitProcessWithTimeout tp 10
        when (isNothing result) $ killIOTask task
        TP.stopProcess tp
        writeIORef process' Nothing
      return STOPPED
    s -> do
      unless ifIsRunning $ throwIO (InvalidStatusException s)
      return s

killIOTask :: IOTask -> IO ()
killIOTask IOTask{..} = do
  void $ TP.runProcess killProcessConfig
  where
    killDockerCmd = "docker kill " ++ T.unpack (getDockerName taskId)
    killProcessConfig = TP.shell killDockerCmd

updateStatus :: IOTask -> (IOTaskStatus -> IO IOTaskStatus) -> IO ()
updateStatus IOTask{..} action = do
  C.modifyMVar_ statusM $ \status -> do
    ts <- action status
    when (ts /= status) $ M.updateStatusInMeta taskHandle taskId ts
    return ts

checkProcess :: IOTask -> IO ()
checkProcess ioTask@IOTask{..} = do
  updateStatus ioTask $ \status -> do
    case status of
      RUNNING -> do
        Just tp <- readIORef process'
        TP.getExitCode tp >>= \case
          Nothing -> return status
          Just _  -> return FAILED
      _ -> return status

tryWaitProcessWithTimeout :: TP.Process IO.Handle IO.Handle IO.Handle -> Int -> IO (Maybe TP.ExitCode)
tryWaitProcessWithTimeout tp timeoutSec = do
  Async.race (C.threadDelay $ timeoutSec * 1000000) (TP.waitExitCode tp) >>= \case
    Left _     -> return Nothing
    Right code -> return $ Just code

checkIOTask :: IOTask -> IO ()
checkIOTask IOTask{..} = do
  Log.info $ "checkCmd:" <> Log.buildString checkCmd
  checkResult <- Async.race delay (TP.runProcess checkProcessConfig)
  case checkResult of
    Left _ -> do
      Log.warning $ Log.buildString "run process timeout"
      throwIO (RunProcessTimeoutException timeoutSec)
    Right TP.ExitSuccess -> do
      checkOutput <- BSL.readFile checkLogPath
      let (result :: Maybe MSG.CheckResult) = msum . map J.decode $ BSLC.lines checkOutput
      case result of
        Nothing -> do
          E.throwIO (CheckFailedException "check process didn't return correct result messsage")
        Just MSG.CheckResult {result=False, message=msg} -> do
          E.throwIO (CheckFailedException $ "check failed:" <> msg)
        Just _ -> pure ()
    Right exitCode -> do
      Log.warning $ Log.buildString ("check process exited: " ++ show exitCode)
      E.throwIO (CheckFailedException "check process exited unexpectedly")
  where
    checkProcessConfig = TP.setStdin TP.closed
      . TP.setStdout TP.closed
      . TP.setStderr TP.closed
      $ TP.shell checkCmd
    TaskConfig {..} = taskConfig taskInfo
    checkLogPath = taskPath ++ "/check.log"
    checkCmd = concat [
        "docker run --rm -i",
        " --network=", T.unpack tcNetwork,
        " --name ", T.unpack (getDockerName taskId),
        " -v " , taskPath, ":/data",
        " " , T.unpack tcImage,
        " check",
        " --config /data/config.json",
        " >> ", checkLogPath, " 2>&1"
      ]
    timeoutSec = 15
    delay = C.threadDelay $ timeoutSec * 1000000

getSpec :: T.Text -> IO T.Text
getSpec img = do
  Log.info $ "spec Cmd:" <> Log.buildString specCmd
  Async.race delay (TP.readProcess getSpecCfg) >>= \case
    Left () -> do
      Log.warning "run process timeout"
      throwIO (RunProcessTimeoutException timeoutSec)
    Right (TP.ExitSuccess, out, _) -> do
      case (msum . map J.decode $ BSLC.lines out :: Maybe J.Object) of
        Nothing -> do
          E.throwIO (CheckFailedException "spec process didn't return correct result messsage")
        Just val -> return . TL.toStrict $ J.encodeToLazyText val
    Right (exitCode, _, _) -> do
      Log.warning $ Log.buildString ("spec process exited: " ++ show exitCode)
      E.throwIO (CheckFailedException "spec process exited unexpectedly")
  where
    getSpecCfg = TP.setStdin TP.closed
      . TP.setStdout TP.createPipe
      . TP.setStderr TP.closed
      $ TP.shell specCmd
    specCmd = T.unpack $ "docker run --rm " <> img <> " spec"
    timeoutSec = 15
    delay = C.threadDelay $ timeoutSec * 1000000

cleanLocalIOTask :: IOTask -> IO ()
cleanLocalIOTask task@IOTask{..} = do
  killIOTask task
  removeDirectoryRecursive taskPath
