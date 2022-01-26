module HStream.Server.LoadBalance where

import           Control.Concurrent               (MVar, modifyMVar_, readMVar)
import           Control.Concurrent.STM           (atomically, modifyTVar',
                                                   readTMVar, readTVar,
                                                   readTVarIO, retry, writeTVar)
import           Control.Monad                    (unless, when)
import           Data.HashMap.Strict              (mapWithKey)
import           Data.List                        (sort)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (zooWatchGetChildren)
import           ZooKeeper.Types                  (HsWatcherCtx (..),
                                                   StringVector (..),
                                                   StringsCompletion (..),
                                                   ZHandle)

import qualified Data.HashMap.Strict              as HM
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap,
                                                   getAllocatedNodeId)
import           HStream.Server.Persistence       (getServerNode',
                                                   serverRootPath)
import           HStream.Server.Types             (ServerContext (..),
                                                   SubscribeContext (..),
                                                   SubscribeContextNewWrapper (..),
                                                   SubscribeState (..))
import qualified ZooKeeper.Recipe                 as ZK

actionTriggeredByServerNodeChange :: ServerContext -> IO ()
actionTriggeredByServerNodeChange sc@ServerContext {..}= do
  zooWatchGetChildren zkHandle serverRootPath
    callback action
  where
    callback HsWatcherCtx {..} =
      actionTriggeredByServerNodeChange sc
    action (StringsCompletion (StringVector children)) = do
      updateHashRing zkHandle children loadBalanceHashRing
      validateAndStopSub sc

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
-- TODO: Instead of reconstruction, we should use the operation insert/delete.
updateHashRing :: ZHandle -> [CBytes] -> MVar HashRing -> IO ()
updateHashRing zk children mhr = modifyMVar_ mhr $ \_ -> do
  serverNodes <- mapM (getServerNode' zk) children
  pure $ constructServerMap . sort $ serverNodes

validateAndStopSub :: ServerContext -> IO ()
validateAndStopSub ServerContext {..} = do
  hr <- readMVar loadBalanceHashRing
  subMap <- readTVarIO scSubscribeContexts
  sequence_ (flip mapWithKey subMap $ \ subId SubscribeContextNewWrapper{..} ->
    when (serverID /= getAllocatedNodeId hr subId) $ do
      lockedPath <- atomically $ do
        modifyTVar' scnwState (const SubscribeStateStopping)
        scwContext <- readTMVar scnwContext
        waitingStopped scwContext scnwState
      ZK.unlock zkHandle lockedPath
      atomically (removeSubFromCtx subId)
    )
  where
    waitingStopped SubscribeContext{..} subState = do
      consumers <- readTVar subConsumerContexts
      unless (HM.null consumers) retry
      writeTVar subState SubscribeStateStopped
      return subLock
    removeSubFromCtx subId =  do
      scs <- readTVar scSubscribeContexts
      writeTVar scSubscribeContexts (HM.delete subId scs)
