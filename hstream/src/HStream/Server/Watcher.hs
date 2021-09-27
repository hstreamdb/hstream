{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.Server.Watcher where

import           Control.Concurrent         (MVar, readMVar, tryPutMVar)
import           Control.Concurrent.STM     (TChan, atomically, newTChanIO,
                                             readTChan, writeTChan)
import           Control.Exception          (SomeException, try)
import           Control.Monad              (forM, forever, void, when)
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
import           HStream.Server.LoadBalance
import           HStream.Server.Persistence
import           HStream.Server.Types

actionTriggedByNodesChange :: ServerOpts -> ServerContext -> LoadManager  -> IO ()
actionTriggedByNodesChange ServerOpts{..} sc@ServerContext{..} LoadManager{..} = forever $ do
  event <- atomically $ readTChan serverEvents
  readyServers <- getReadyServers zkHandle
  case event of
    ZooCreateEvent
      | readyServers >= _serverMinNum -> do -- do nothing, or update load reports
          updateLoadReports zkHandle loadReports ranking
      | readyServers  < _serverMinNum -> do -- impossible!
          Log.fatal "Internal server error"
          return ()
    ZooChangedEvent
      | readyServers < _serverMinNum ->  do -- exception? stop app?
        setNodeStatus zkHandle _serverName Ready
        Log.warning "No enough nodes found, server may not work properly "
      | readyServers >= _serverMinNum -> do -- still enough
        return ()
    ZooDeleteEvent
      | readyServers < _serverMinNum ->  do -- exception? stop app?
          setNodeStatus zkHandle _serverName Ready
          Log.warning "No enough nodes found, server may not work properly"
      | readyServers >= _serverMinNum -> do -- still enough
          Log.debug "Some node failed, trying to recover tasks"
          leader <- readMVar leaderName
          Log.debug . Log.buildCBytes $ "Current Leader is " <> leader
          -- when (leader == serverName) $ do
          --   recoverTasks sc
          --   recoverSubscriptions sc
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

isSelfRunning :: IORef Bool
isSelfRunning = unsafePerformIO $ newIORef False
{-# NOINLINE isSelfRunning #-}

diffServers :: IORef (Set CBytes)
diffServers = unsafePerformIO $ newIORef Set.empty
{-# NOINLINE diffServers #-}

getReadyServers :: ZHandle -> IO Int
getReadyServers zk = do
  (StringsCompletion (StringVector servers)) <- zooGetChildren zk serverRootPath
  (sum <$>) . forM servers $ \name -> do
    (e' :: Either SomeException NodeStatus) <- try $ getNodeStatus zk name
    case e' of
      Right Ready   -> return (1 :: Int)
      Right Working -> return (1 :: Int)
      _             -> return (0 :: Int)

prevServers :: IORef (Set CBytes)
prevServers = unsafePerformIO $ newIORef Set.empty
{-# NOINLINE prevServers #-}
