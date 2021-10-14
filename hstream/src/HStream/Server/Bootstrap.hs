{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Bootstrap
  ( startServer
  ) where

import           Control.Concurrent            (forkIO, newEmptyMVar, takeMVar)
import           Control.Concurrent.STM.TChan  (readTChan)
import           Control.Monad                 (unless, void, when)
import           Control.Monad.STM             (atomically)
import           Data.IORef                    (IORef, modifyIORef, newIORef,
                                                readIORef, writeIORef)
import qualified Data.Text.Lazy                as TL
import           System.IO.Unsafe              (unsafePerformIO)
import           Z.Data.CBytes                 (CBytes)
import           ZooKeeper.Types

import qualified HStream.Logger                as Log
import           HStream.Server.Initialization (initNodePath)
import           HStream.Server.Persistence
import           HStream.Server.Types          (ServerOpts (..))
import           HStream.Server.Watcher        (serverEvents,
                                                watchChildrenForever)

--------------------------------------------------------------------------------

startServer :: ZHandle -> ServerOpts -> IO () -> IO ()
startServer zk opts@ServerOpts {..} myApp = do
    let serverUri          = TL.pack $ _serverAddress <> ":" <> show _serverPort
        serverInternalUri  = TL.pack $ _serverAddress <> ":" <> show _serverInternalPort
    -- 1. Check persistent paths
    initializeAncestors zk

    -- 2. Check the consistency of the server config
    checkConfigConsistent opts zk

    -- 3. Run the monitoring service
    watchSetDone <- newEmptyMVar
    void . forkIO $ watchChildrenForever zk serverRootPath watchSetDone
    void $ takeMVar watchSetDone

    -- 4. Run Server
    initNodePath zk _serverName serverUri serverInternalUri
    bootStrap zk opts
    runServer zk _serverName myApp

--------------------------------------------------------------------------------

retryCount :: IORef Int
retryCount = unsafePerformIO $ newIORef 1
{-# NOINLINE retryCount #-}

isSelfRunning :: IORef Bool
isSelfRunning = unsafePerformIO $ newIORef False
{-# NOINLINE isSelfRunning #-}

--------------------------------------------------------------------------------

runServer :: ZHandle -> CBytes -> IO () -> IO ()
runServer zk self app = do
  isRunning <- readIORef isSelfRunning
  unless isRunning $ do
    Log.debug "Cluster is ready, starting hstream server..."
    writeIORef isSelfRunning True
  setNodeStatus zk self Working
  app

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
