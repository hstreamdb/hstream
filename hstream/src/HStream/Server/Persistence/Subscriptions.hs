{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Persistence.Subscriptions where

import           Control.Monad                     (forM)
import           Data.Bifunctor                    (second)
import qualified Data.Map                          as Map
import           Data.Maybe                        (fromJust, isJust)
import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Utils
import           HStream.Utils                     (cBytesToText)
import           ZooKeeper                         (zooExists, zooGetChildren)
import           ZooKeeper.Types                   (StringVector (unStrVec),
                                                    StringsCompletion (strsCompletionValues),
                                                    ZHandle)

-------------------------------------------------------------------------------

instance SubscriptionPersistence ZHandle where
  storeSubscription subId val zk = do
    createInsert zk subPath (encodeValueToBytes val)
    where subPath = mkSubscriptionPath subId

  getSubscription subId zk = decodeZNodeValue zk (cBytesToText subPath)
    where subPath = mkSubscriptionPath subId

  checkIfExist subId zk = isJust <$> zooExists zk (mkSubscriptionPath subId)

  listSubscriptions zk = do
    sIds <- fmap cBytesToText . unStrVec . strsCompletionValues <$>
            zooGetChildren zk subscriptionsPath
    ms <- forM sIds (`getSubscription` zk)
    return $ Map.fromList $ second fromJust <$> filter (\(_,x) -> isJust x) (sIds `zip` ms)

  removeSubscription subId zk = tryDeletePath zk $ mkSubscriptionPath subId

  removeAllSubscriptions zk = tryDeleteAllPath zk subscriptionsPath
