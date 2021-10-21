{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Bootstrap
  ( startServer
  , actionTriggedByNodesChange
  ) where

import           Control.Concurrent            (readMVar)
import           Control.Monad                 (void)
import           Data.IORef                    (IORef, newIORef, readIORef,
                                                writeIORef)
import           Data.Set                      (Set)
import qualified Data.Set                      as Set
import qualified Data.Text.Lazy                as TL
import           GHC.IO                        (unsafePerformIO)
import           Z.Data.CBytes                 (CBytes)
import           ZooKeeper                     (zooGetChildren,
                                                zooWatchGetChildren)
import           ZooKeeper.Types

import qualified HStream.Logger                as Log
import           HStream.Server.Initialization (initNodePath)
import           HStream.Server.LoadBalance    (updateLoadReports)
import           HStream.Server.Persistence
import           HStream.Server.Types          (LoadManager (..),
                                                ServerContext (..),
                                                ServerOpts (..))

--------------------------------------------------------------------------------

startServer :: ZHandle -> ServerOpts -> IO () -> IO ()
startServer zk ServerOpts {..} myApp = do
  let serverUri         = TL.pack $ _serverAddress <> ":" <> show _serverPort
      serverInternalUri = TL.pack $ _serverAddress <> ":" <> show _serverInternalPort
  initializeAncestors zk
  initNodePath zk _serverName serverUri serverInternalUri
  setNodeStatus zk _serverName Working
  myApp

actionTriggedByNodesChange :: ServerContext -> LoadManager -> IO ()
actionTriggedByNodesChange sc@ServerContext{..} lm@LoadManager{..} = do
  zooWatchGetChildren zkHandle serverRootPath callback result
  where
    callback HsWatcherCtx{..} = do
      (StringsCompletion (StringVector children))
        <- zooGetChildren watcherCtxZHandle serverRootPath
      oldChs <- readIORef prevServers
      let newChs = Set.fromList children
      act oldChs newChs
      void $ actionTriggedByNodesChange sc lm
    result (StringsCompletion (StringVector children)) = do
      writeIORef prevServers (Set.fromList children)

    act oldChs newChs
      | oldChs `Set.isSubsetOf` newChs = updateLoadReports zkHandle loadReports
      | newChs `Set.isSubsetOf` oldChs = do
        leader <- readMVar leaderName
        Log.info "Some node failed. "
        Log.debug . Log.buildCBytes $ "Current Leader is " <> leader
      | otherwise = error "Unknown internal error"

prevServers :: IORef (Set CBytes)
prevServers = unsafePerformIO $ newIORef Set.empty
{-# NOINLINE prevServers #-}
