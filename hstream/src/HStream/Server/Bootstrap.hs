{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Bootstrap where

import           Control.Concurrent                (forkIO, newEmptyMVar,
                                                    takeMVar, threadDelay)
import           Control.Concurrent.STM.TChan      (readTChan)
import           Control.Monad                     (unless, void, when)
import           Control.Monad.STM                 (atomically)
import qualified Data.Aeson                        as Aeson
import           Data.IORef                        (IORef, modifyIORef,
                                                    newIORef, readIORef,
                                                    writeIORef)
import           System.Exit                       (exitFailure)
import           System.IO.Unsafe                  (unsafePerformIO)
import           Z.Data.CBytes                     (CBytes)
import           ZooKeeper
import           ZooKeeper.Types

import qualified HStream.Logger                    as Log
import           HStream.Server.Initialization
import           HStream.Server.Persistence        (initializeAncestors,
                                                    rootPath, serverRootPath,
                                                    setNodeStatus)
import           HStream.Server.Persistence.Config (HServerConfig (..))
import           HStream.Server.Types
import           HStream.Server.Watcher
import           HStream.Utils                     (bytesToLazyByteString,
                                                    valueToBytes)

--------------------------------------------------------------------------------

retryCount :: IORef Int
retryCount = unsafePerformIO $ newIORef 1
{-# NOINLINE retryCount #-}

--------------------------------------------------------------------------------

startServer
  :: ZHandle
  -> ServerOpts
  -> IO ()
  -> IO ()
startServer zk opts@ServerOpts {..} myApp = do
    let configPath = rootPath <> "/config"
        serverUri  = _serverAddress <> ":" <> show _serverPort
        serverUriInternal = _serverAddress <> ":" <> show _serverInternalPort
    -- 1. Check persistent paths
    initializeAncestors zk

    -- 2. Check the consistency of the server config
    configExists <- zooExists zk configPath
    case configExists of
      Just _  -> do
        (DataCompletion val _) <- zooGet zk configPath
        case Aeson.decode' . bytesToLazyByteString =<< val of
          Just (HServerConfig minServers)
            | minServers == _serverMinNum -> return ()
            | otherwise  -> do
                Log.fatal . Log.buildString $
                  "Server config min-servers is set to "
                  <> show _serverMinNum <> ", which does not match "
                  <> show minServers <> " in zookeeper"
                exitFailure
          Nothing -> Log.fatal "Server error: broken config is found"
      Nothing -> do
        let serverConfig = HServerConfig { hserverMinServers = _serverMinNum }
        void $ zooCreate zk configPath (Just $ valueToBytes serverConfig) zooOpenAclUnsafe ZooEphemeral

    -- 3. Run the monitoring service
    watchSetDone <- newEmptyMVar
    void . forkIO $ watchChildrenForever zk serverRootPath watchSetDone
    void $ takeMVar watchSetDone

    -- 4. Run Server
    initNodePath zk _serverName serverUri serverUriInternal
    bootStrap zk opts
    runServer zk _serverName myApp

    -- 5. Create initial server node (Ready)
    threadDelay (maxBound :: Int)

--------------------------------------------------------------------------------

runServer :: ZHandle -> CBytes -> IO a -> IO ()
runServer zk self app = do
  isRunning <- readIORef isSelfRunning
  unless isRunning $ do
    Log.debug "Cluster is ready, starting hstream server..."
    writeIORef isSelfRunning True
  void app
  setNodeStatus zk self Working        -- boot

bootStrap :: ZHandle -> ServerOpts -> IO ()
bootStrap zkHandle opt@ServerOpts{..} = do
  event <- atomically $ readTChan serverEvents
  readyServers <- getReadyServers zkHandle
  Log.debug . Log.buildString $ "Event " <> show event <> " detected"
  when (event == ZooDeleteEvent && readyServers >= _serverMinNum) $ -- impossible!
    Log.fatal "Internal server error"
  cnt <- readIORef retryCount
  when (readyServers < _serverMinNum) $ do
    clusterNotReady cnt readyServers _serverMinNum
    bootStrap zkHandle opt -- wait

clusterNotReady :: Int -> Int -> Int -> IO ()
clusterNotReady cnt readyServers minServer = do
  Log.warning . Log.buildString $
    "Trial #" <> show cnt <> ": There are " <> show readyServers <> " ready servers"
    <> ", which is fewer than the minimum requirement " <> show minServer
  modifyIORef retryCount (+ 1)
