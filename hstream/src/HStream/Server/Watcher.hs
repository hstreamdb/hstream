{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Watcher
  ( actionTriggedByNodesChange
  , watchChildrenForever
  , serverEvents
  ) where

import           Control.Concurrent         (MVar, tryPutMVar)
import           Control.Concurrent.STM     (TChan, atomically, newTChanIO,
                                             readTChan, writeTChan)
import           Control.Monad              (forever, void)
import           Data.IORef                 (IORef, newIORef, readIORef,
                                             writeIORef)
import           Data.Set                   (Set)
import qualified Data.Set                   as Set
import           GHC.IO                     (unsafePerformIO)
import           Z.Data.CBytes              (CBytes)
import           ZooKeeper                  (zooGetChildren,
                                             zooWatchGetChildren)
import           ZooKeeper.Types

import qualified HStream.Logger             as Log
import           HStream.Server.LoadBalance (updateLoadReports)
import           HStream.Server.Persistence (NodeStatus (..), getReadyServers,
                                             setNodeStatus)
import           HStream.Server.Types       (LoadManager (..),
                                             ServerContext (..))

actionTriggedByNodesChange :: ServerContext -> LoadManager  -> IO ()
actionTriggedByNodesChange ServerContext{..} LoadManager{..} = forever $ do
  event <- atomically $ readTChan serverEvents
  readyServers <- getReadyServers zkHandle
  case event of
    ZooCreateEvent
      | readyServers >= minServers -> do -- do nothing, or update load reports
          updateLoadReports zkHandle loadReports
      | readyServers  < minServers -> do -- impossible!
          Log.fatal "Internal server error"
          return ()
    ZooChangedEvent
      | readyServers < minServers ->  do -- exception? stop app?
        setNodeStatus zkHandle serverName Ready
        Log.warning "No enough nodes found, server may not work properly "
      | readyServers >= minServers -> do -- still enough
        return ()
    ZooDeleteEvent
      | readyServers < minServers ->  do -- exception? stop app?
          setNodeStatus zkHandle serverName Ready
          Log.warning "No enough nodes found, server may not work properly "
      | readyServers >= minServers -> do -- still enough
          Log.info "Some node failed. "
    _ -> return ()

watchChildrenForever :: ZHandle -> CBytes -> MVar () ->  IO ()
watchChildrenForever zk path watchSetDone = do
  zooWatchGetChildren zk path callback ret
  where
    callback HsWatcherCtx{..} = do
      (StringsCompletion (StringVector children)) <- zooGetChildren watcherCtxZHandle path
      oldChs <- readIORef prevServers
      let newChs = Set.fromList children
      Log.d . Log.buildString $ show newChs
      act oldChs newChs
      void $ watchChildrenForever watcherCtxZHandle path watchSetDone
    ret (StringsCompletion (StringVector children)) = do
      writeIORef prevServers (Set.fromList children)
      void $ tryPutMVar watchSetDone ()
    act oldChs newChs
      | oldChs `Set.isSubsetOf` newChs =
        atomically $ writeTChan serverEvents ZooCreateEvent
      | newChs `Set.isSubsetOf` oldChs = do
        writeIORef diffServers (oldChs `Set.difference` newChs)
        atomically $ writeTChan serverEvents ZooDeleteEvent
      | otherwise = error "Unknown internal error"

serverEvents :: TChan ZooEvent
serverEvents = unsafePerformIO newTChanIO
{-# NOINLINE serverEvents #-}

diffServers :: IORef (Set CBytes)
diffServers = unsafePerformIO $ newIORef Set.empty
{-# NOINLINE diffServers #-}

prevServers :: IORef (Set CBytes)
prevServers = unsafePerformIO $ newIORef Set.empty
{-# NOINLINE prevServers #-}
