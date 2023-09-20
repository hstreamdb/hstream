module HStream.Common.Server.HashRing
  ( LoadBalanceHashRing
  , initializeHashRing
  , updateHashRing
  ) where

import           Control.Concurrent.STM
import           Control.Monad
import           Data.List                        (sort)

import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import           HStream.Gossip.Types             (Epoch, GossipContext)
import           HStream.Gossip.Utils             (getMemberListWithEpochSTM)

type LoadBalanceHashRing = TVar (Epoch, HashRing)

initializeHashRing :: GossipContext -> IO LoadBalanceHashRing
initializeHashRing gc = atomically $ do
  (epoch, serverNodes) <- getMemberListWithEpochSTM gc
  newTVar (epoch, constructServerMap . sort $ serverNodes)

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
updateHashRing :: GossipContext -> LoadBalanceHashRing -> IO ()
updateHashRing gc hashRing = loop (0,[])
  where
    loop (epoch, list)=
      loop =<< atomically
        ( do (epoch', list') <- getMemberListWithEpochSTM gc
             when (epoch == epoch' && list == list') retry
             writeTVar hashRing (epoch', constructServerMap list')
             return (epoch', list')
        )
