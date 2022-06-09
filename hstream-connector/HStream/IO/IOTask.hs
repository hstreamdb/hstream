{-# LANGUAGE DeriveGeneric, OverloadedStrings, DeriveAnyClass, DerivingStrategies, LambdaCase #-}
{-# LANGUAGE TemplateHaskell #-}

module HStream.IO.IOTask where

import System.Process.Typed (withProcessWait_, setStdin, createPipe, closed, setStdout, setStderr, shell, getStdout, getStdin, Process, getExitCode, startProcess, waitExitCode, stopProcess, ExitCode (ExitSuccess, ExitFailure), waitExitCodeSTM)
import Control.Monad (forever, unless, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Char8 as BSC8
import qualified Data.ByteString.Lazy.Char8 as BSLC
import qualified Data.Aeson as Aeson
import Data.Aeson ((.:), (.=))
import GHC.Int (Int64)
import GHC.Generics (Generic)
import RIO.Directory (createDirectoryIfMissing)
import qualified Data.Text as T
import Data.Aeson.TH (deriveJSON, defaultOptions)
import Control.Concurrent (MVar, newMVar, modifyMVar_, readMVar, ThreadId, newEmptyMVar, forkIO, putMVar, killThread, withMVar, threadDelay, takeMVar, tryTakeMVar)
import qualified Control.Concurrent.STM as STM
import Data.IORef (newIORef, writeIORef, IORef, readIORef)
import GHC.IO.Handle (Handle, hIsEOF, hFlush)
import Control.Exception (catch, SomeException (SomeException), throw, bracket)
import qualified Data.Text.Encoding as T

data SrcInfo = SrcInfo { srcImage :: T.Text, srcConfig :: Aeson.Value, srcCatalog :: Aeson.Value, srcState :: Aeson.Value }
data DstInfo = DstInfo { dstImage :: T.Text, dstConfig :: Aeson.Value, dstCatalog :: Aeson.Value}

$(deriveJSON defaultOptions ''SrcInfo)
$(deriveJSON defaultOptions ''DstInfo)


data IOTaskStatus
  = NEW
  | INITIALED
  | STARTED
  | RUNNING
  | COMPLETED
  | FAILED
  | STOPPED
  deriving (Show, Eq)

$(deriveJSON defaultOptions ''IOTaskStatus)

data IOTask = IOTask
  { taskId :: T.Text,
    taskType :: IOTaskType,
    srcInfo :: SrcInfo,
    dstInfo :: DstInfo,
    latestStatesM :: MVar Aeson.Value,
    tidM    :: IORef (Maybe ThreadId),
    statusM :: MVar IOTaskStatus
  }

data IOTaskType 
  = Full
  -- duration(ms), maxRetry
  | Incremental Int Int
  deriving (Show, Eq)

newIOTask :: T.Text -> IOTaskType -> SrcInfo -> DstInfo -> IO IOTask
newIOTask taskId ioTaskType src dst = 
  IOTask taskId ioTaskType src dst <$> newMVar (Aeson.object []) <*> newIORef Nothing <*> newMVar NEW

getStatus :: IOTask -> IO IOTaskStatus
getStatus IOTask{..} = readMVar statusM

getLatestState :: IOTask -> IO Aeson.Value
getLatestState IOTask{..} = readMVar latestStatesM

data AirbyteMessage
  = AirbyteRecordMessage String Aeson.Value Int64
  | AirbyteStateMessage Aeson.Value
  | AirbyteLogMessage T.Text T.Text
  deriving (Generic, Show)

instance Aeson.FromJSON AirbyteMessage where
  parseJSON (Aeson.Object v) =
    v .: "type" >>= \case
      ("RECORD" :: String) ->
        v .: "record" >>= Aeson.withObject "record"
          (\r -> AirbyteRecordMessage <$> r .: "stream" <*> r .: "data" <*> r .: "emitted_at")
      "STATE" -> v .: "state" >>= Aeson.withObject "state" 
        (\s -> AirbyteStateMessage <$> s .: "data")
      "LOG" -> v .: "log" >>= Aeson.withObject "log"
        (\l -> AirbyteLogMessage <$> l .: "level" <*> l .: "message")
      "SPEC" -> fail "unimplemented"
      "CONNECTION_STATUS" -> fail "unimplemented"
      "CATALOG" -> fail "unimplemented"
      _ -> fail ""
  parseJSON _ = fail "AirbyteMessage should include type field"

instance Aeson.ToJSON AirbyteMessage where
  toJSON (AirbyteRecordMessage stream d emitted_at) =
    Aeson.object ["type" .= ("RECORD" :: T.Text), 
      "record" .= Aeson.object ["stream" .= stream, "data" .= d, "emitted_at" .= emitted_at]]
  toJSON (AirbyteStateMessage state) =
    Aeson.object ["type" .= ("STATE" :: T.Text),
      "state" .= Aeson.object ["data" .= state] ]
  toJSON (AirbyteLogMessage level message) =
    Aeson.object ["type" .= ("LOG" :: T.Text),
      "log" .= Aeson.object ["level" .= level, "message" .= message]]

-- hstream-io state messages
data IOStateMessage
  = IOStateMessageEOF
  deriving (Eq, Show)

instance Aeson.FromJSON IOStateMessage where
  parseJSON (Aeson.Object v) = v .: "HStreamIOStateMessageType"
    >>= \case ("EOF" :: String) -> pure IOStateMessageEOF
              t -> fail $ "unknown IOStateMessage type:" ++ t
  parseJSON _ = fail "IOStateMessage should be an Object"

instance Aeson.ToJSON IOStateMessage where
  toJSON IOStateMessageEOF = Aeson.object ["HStreamIOStateMessageType" Aeson..= ("EOF" :: String)]


getTaskPath :: T.Text -> FilePath
getTaskPath taskId = "/tmp/io/tasks/" ++ T.unpack taskId

data IOTaskPaths = IOTaskPaths
  { taskPath :: FilePath,
    srcTaskPath :: FilePath,
    dstTaskPath :: FilePath,
    srcConfigPath :: FilePath,
    srcCatalogPath :: FilePath,
    srcStatePath :: FilePath,
    dstConfigPath :: FilePath,
    dstCatalogPath :: FilePath
  }

makeIOTaskPaths :: IOTask -> IOTaskPaths
makeIOTaskPaths IOTask {..} =
  IOTaskPaths
    { taskPath = taskPath,
      srcTaskPath = srcTaskPath,
      dstTaskPath = dstTaskPath,
      srcConfigPath = srcTaskPath ++ "/config.json",
      srcCatalogPath = srcTaskPath ++ "/catalog.json",
      srcStatePath = srcTaskPath ++ "/state.json",
      dstConfigPath = dstTaskPath ++ "/config.json",
      dstCatalogPath = dstTaskPath ++ "/catalog.json"
    }
  where
    taskPath = getTaskPath taskId
    srcTaskPath = taskPath ++ "/src"
    dstTaskPath = taskPath ++ "/dst"

initIOTask :: IOTask -> IO ()
initIOTask ioTask@IOTask {..} = do
  -- create task files
  createDirectoryIfMissing True srcTaskPath
  createDirectoryIfMissing True dstTaskPath
  BSL.writeFile srcConfigPath (Aeson.encode srcConfig)
  BSL.writeFile srcCatalogPath (Aeson.encode srcCatalog)
  BSL.writeFile srcStatePath (Aeson.encode srcState)
  BSL.writeFile dstConfigPath (Aeson.encode dstConfig)
  BSL.writeFile dstCatalogPath (Aeson.encode dstCatalog)
  modifyMVar_ statusM (\_ -> pure INITIALED)
  where
    SrcInfo {..} = srcInfo
    DstInfo {..} = dstInfo
    IOTaskPaths {..} = makeIOTaskPaths ioTask

runIOTask :: IOTask -> (Aeson.Value -> IO ()) -> IO Bool
runIOTask ioTask@IOTask{..} updateState = do
  -- update state
  getLatestState ioTask >>= BSL.writeFile srcStatePath . Aeson.encode

  putStrLn $ "readCmd: " ++ readCmd
  putStrLn $ "writeCmd: " ++ writeCmd
  bracket (startProcess readProcessConfig) stopProcess $ \rp -> do
    putStrLn "start read process"
    bracket (startProcess writeProcessConfig) stopProcess $ \wp -> do
      putStrLn "start write process"
      let rStdout = getStdout rp
          wStdin = getStdin wp
          wStdout = getStdout wp
      modifyMVar_ statusM (\_ -> pure RUNNING)
      isCompleted <- STM.newEmptyTMVarIO
      let readMessages = forever $ do
            putStrLn "loop"
            eof <- hIsEOF rStdout
            if eof 
            then do
              BSLC.hPutStrLn wStdin . Aeson.encode . AirbyteStateMessage $ Aeson.toJSON IOStateMessageEOF
              hFlush wStdin
              fail "readCompleted"
            else do
              srcOutput <- BS.hGetLine rStdout
              putStrLn $ "read >>>>>: " ++ T.unpack (T.decodeUtf8 srcOutput)
              case Aeson.decodeStrict srcOutput of
                Nothing -> putStrLn "Invalid srcOutput json"
                Just AirbyteStateMessage {} -> do
                  BSC8.hPutStrLn wStdin srcOutput
                  putStrLn "write state !!!!!!!!!!!!!!!!!!!!!"
                Just AirbyteRecordMessage {} -> do
                  BSC8.hPutStrLn wStdin srcOutput
                  putStrLn "write record !!!!!!!!!!!!!!!!!!!!!"
                Just (AirbyteLogMessage level msg) -> do
                  putStrLn $ "read log msg" ++ "[" ++ T.unpack level ++ "]:" ++ T.unpack msg
          writeMessages = forever $ do
            putStrLn "wp reading from stdout"
            dstOutput <- BS.hGetLine wStdout
            putStrLn $ "write >>>>>: " ++ T.unpack (T.decodeUtf8 dstOutput)
            case Aeson.decodeStrict dstOutput of
              Nothing -> putStrLn "Invalid dstOutput json"
              Just (AirbyteStateMessage state) -> do
                case Aeson.fromJSON state of
                  (Aeson.Success IOStateMessageEOF) -> do
                    putStrLn "received IOStateMessageEOF"
                    STM.atomically $ STM.putTMVar isCompleted ExitSuccess
                    fail "write completed"
                  (Aeson.Error _) -> do
                    -- update local
                    modifyMVar_ latestStatesM (\_ -> pure state)
                    -- update storage
                    updateState state
              Just (AirbyteLogMessage level msg) -> do
                putStrLn $ "write log msg" ++ "[" ++ T.unpack level ++ "]:" ++ T.unpack msg
              _ -> fail "invalid airbyteMessage"
      bracket (forkIO readMessages) killThread $ \_ -> do
        bracket (forkIO writeMessages) killThread $ \_ -> do
          rpExitCode <- waitExitCode rp
          putStrLn $ "read exited, " ++ show rpExitCode

          (wpExitCode, msg) <- STM.atomically $ do 
            STM.orElse
              (STM.takeTMVar isCompleted >> pure (ExitSuccess, []))
              (waitExitCodeSTM wp 
                >>= \case ExitSuccess -> pure (ExitFailure 1, "partial complete")
                          code -> pure (code, "unexpected exit"))
          putStrLn $ "write exited, " ++ show wpExitCode ++ ", " ++ msg

          return $ (rpExitCode, wpExitCode) == (ExitSuccess, ExitSuccess)
  where
    SrcInfo {..} = srcInfo
    DstInfo {..} = dstInfo
    IOTaskPaths {..} = makeIOTaskPaths ioTask
    readCmd = concat [
        "docker run --rm -i --network=host",
        " -v " , srcTaskPath, ":/data",
        " " , T.unpack srcImage,
        " read",
        " --config /data/config.json",
        " --catalog /data/catalog.json",
        " --state /data/state.json"
      ]
    writeCmd = concat [
        "docker run --rm -i --network=host",
        " -v ", dstTaskPath, ":/data",
        " ", T.unpack dstImage,
        " write",
        " --config /data/config.json",
        " --catalog /data/catalog.json"
      ]
    readProcessConfig = setStdin closed
      . setStdout createPipe
      . setStderr closed
      $ shell readCmd
    writeProcessConfig = setStdin createPipe
      . setStdout createPipe
      . setStderr closed
      $ shell writeCmd

runWrappedIOTask :: IOTask -> (Aeson.Value -> IO ()) -> IO ()
runWrappedIOTask task@IOTask{taskType = Full, ..} updateState = 
  runIOTask task updateState
    >>= \case True -> modifyMVar_ statusM (\_ -> pure COMPLETED)
              False -> modifyMVar_ statusM (\_ -> pure FAILED)
runWrappedIOTask task@IOTask{taskType = Incremental duration maxRetry, ..} updateState = do
  runWithRetry (1 :: Int)
  where
    runWithRetry retry = do
      success <- runIOTask task updateState
      if success
      then do
        threadDelay $ duration * 1000
        runWithRetry 1
      else if retry < maxRetry
        then do
          threadDelay 1000000
          runWithRetry $ retry + 1
        else
          modifyMVar_ statusM (\_ -> pure FAILED)

startIOTask :: IOTask -> (Aeson.Value -> IO ()) -> IO ()
startIOTask task@IOTask{..} updateState = do
  modifyMVar_  statusM $ \case
    status | elem status [INITIALED, FAILED, STOPPED]  -> do
      tid <- forkIO . printException $ runWrappedIOTask task updateState
      writeIORef tidM $ Just tid
      return STARTED
    _ -> fail "TODO"

stopIOTask :: IOTask -> IO ()
stopIOTask IOTask{..} = do
  putStrLn "--------------------------XX"
  modifyMVar_  statusM $ \case
    RUNNING -> do
      readIORef tidM >>= maybe (pure ()) killThread
      return STOPPED
    _ -> fail "TODO: status error"

printException :: IO () -> IO ()
printException action = catch action (\(e :: SomeException) -> print e)
