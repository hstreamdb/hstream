{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Common.Server.TaskManager where

import           Control.Concurrent               (forkIO)
import qualified Control.Concurrent               as C
import qualified Control.Concurrent.STM           as C
import qualified Control.Exception                as E
import qualified Control.Monad                    as M
import           Data.Int
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32)
import           HStream.Common.ConsistentHashing (HashRing)
import           HStream.Common.Server.Lookup     (lookupNodePersist)
import qualified HStream.Exception                as HE
import           HStream.Gossip.Types             (Epoch, GossipContext)
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          (MetaHandle)
import           HStream.Server.HStreamApi        (ServerNode (serverNodeId))

-------------------------------------------------------------------------------

-- e.g. IO.Worker, QueryManager, GroupCoordinator
class TaskManager tm where
  resourceName :: tm -> T.Text
  mkMetaId :: tm -> T.Text -> T.Text
  listLocalTasks :: tm -> IO (Set.Set T.Text)

  listAllTasks :: tm -> IO (V.Vector T.Text)
  -- ^ typically list from metastore,
  --    Nothing means metastore is unavailable

  loadTaskAsync :: tm -> T.Text -> IO ()
  unloadTaskAsync :: tm -> T.Text -> IO ()

  -- TODO: for Network Partition
  -- unloadAllTasks :: tm -> IO ()

newtype TaskDetectorConfig
  = TaskDetectorConfig
  { intervalMs :: Int32
  }

-- Only run on a Single Thread
data TaskDetector
  = forall tm. TaskManager tm => TaskDetector
  { managers               :: V.Vector tm
  , config                 :: TaskDetectorConfig

  , metaHandle             :: MetaHandle
  , gossipContext          :: GossipContext
  , loadBalanceHashRing    :: C.TVar (Epoch, HashRing)
  , advertisedListenersKey :: Maybe T.Text
  , serverID               :: Word32
  }

runTaskDetector :: TaskDetector -> IO ()
runTaskDetector td = do
  tid <- forkIO loop
  Log.info $ "running task detector on thread:" <> Log.buildString' tid
  where
    loop = do
      detectTasks td
      C.threadDelay $ (fromIntegral td.config.intervalMs) * 1000
      loop

-- detect all tasks, and load/unload tasks in TaskManager
detectTasks :: TaskDetector -> IO ()
detectTasks TaskDetector{..} = do
  V.forM_ managers $ \tm -> do
    allTasks <- listAllTasks tm
    localTasks <- listLocalTasks tm
    -- TODO: lookup diff all tasks and instead of lookup a single task
    V.forM_ allTasks $ \task -> do
      (flip E.catches) (handleExceptions task) $ do
        let metaId = mkMetaId tm task
            loaded = Set.member task localTasks
        lookupResult <- lookupNodePersist metaHandle gossipContext loadBalanceHashRing task metaId advertisedListenersKey
        if (lookupResult.serverNodeId == serverID) then do
          M.unless loaded $ do
            Log.info $ "loading task, resourceName:" <> Log.build (resourceName tm)
              <> ", taskId:" <> Log.build task
            loadTaskAsync tm task
        else do
          M.when loaded $ do
            Log.info $ "unloading task, resourceName:" <> Log.build (resourceName tm)
              <> ", taskId:" <> Log.build task
            unloadTaskAsync tm task
  where handleExceptions task = [
            E.Handler (\(_ :: HE.QueryAlreadyTerminated) -> return ())
          , E.Handler (\(err :: E.SomeException) ->
              Log.warning $ "detech and load/unload task failed, task:" <> Log.build task
                <> ", reason:" <> Log.build (show err)
            )
          ]
