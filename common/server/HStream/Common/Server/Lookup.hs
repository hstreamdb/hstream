module HStream.Common.Server.Lookup
  ( lookupNode
  ) where

import           Control.Concurrent.STM
import           Data.Text                        (Text)

import           HStream.Common.ConsistentHashing (getResNode)
import           HStream.Common.Server.HashRing   (LoadBalanceHashRing)
import qualified HStream.Server.HStreamApi        as A

lookupNode :: LoadBalanceHashRing -> Text -> Maybe Text -> IO A.ServerNode
lookupNode loadBalanceHashRing key advertisedListenersKey = do
  (_, hashRing) <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing key advertisedListenersKey
  return theNode
