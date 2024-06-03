module HStream.Common.Server.HashRing
  ( LoadBalanceHashRing
  , readLoadBalanceHashRing
  , initializeHashRing
  , updateHashRing
  ) where

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Monad
import           Data.List                        (sort)
import           Data.Maybe                       (fromMaybe)
import           System.Environment               (lookupEnv)
import           Text.Read                        (readMaybe)

import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import           HStream.Gossip.Types             (Epoch, GossipContext)
import           HStream.Gossip.Utils             (getMemberListWithEpochSTM)
import qualified HStream.Logger                   as Log

-- FIXME: The 'Bool' flag means "if we think the HashRing can be used for
--        resource allocation now". This is because a server node can
--        only see a part of the cluster during the early stage of startup.
-- FIXME: This is just a mitigation for the consistency problem.
type LoadBalanceHashRing = TVar (Epoch, HashRing, Bool)

readLoadBalanceHashRing :: LoadBalanceHashRing -> STM (Epoch, HashRing)
readLoadBalanceHashRing hashRing = do
  (epoch, hashRing, isReady) <- readTVar hashRing
  if isReady
    then return (epoch, hashRing)
    else retry

initializeHashRing :: GossipContext -> IO LoadBalanceHashRing
initializeHashRing gc = atomically $ do
  (epoch, serverNodes) <- getMemberListWithEpochSTM gc
  newTVar (epoch, constructServerMap . sort $ serverNodes, False)

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
-- FIXME: We delayed for several seconds to make sure the node has seen
--        the whole cluster. This is only a mitigation. See the comment
--        above.
-- FIXME: Hard-coded constant.
-- WARNING: This should be called exactly once on startup!
updateHashRing :: GossipContext -> LoadBalanceHashRing -> IO ()
updateHashRing gc hashRing = do
  let defaultMs = 5000
  delayMs <- lookupEnv "HSTREAM_INTERNAL_STARTUP_EXTRA_DELAY_MS" >>= \case
    Nothing -> return defaultMs
    Just ms -> return (fromMaybe defaultMs (readMaybe ms))
  void $ forkIO (earlyStageDelay delayMs)
  loop (0,[])
  where
    earlyStageDelay timeoutMs = do
      Log.info $ "Delaying for " <> Log.buildString' timeoutMs <> "ms before I can make resource allocation decisions..."
      threadDelay (timeoutMs * 1000)
      atomically $ modifyTVar' hashRing (\(epoch, hashRing, _) -> (epoch, hashRing, True))
      Log.info "Cluster is ready!"

    loop (epoch, list)=
      loop =<< atomically
        ( do (epoch', list') <- getMemberListWithEpochSTM gc
             when (epoch == epoch' && list == list') retry
             modifyTVar' hashRing
                         (\(_,_,isReady) -> (epoch', constructServerMap list', isReady))
             return (epoch', list')
        )
