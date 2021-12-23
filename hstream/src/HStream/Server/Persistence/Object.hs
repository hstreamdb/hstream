{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module HStream.Server.Persistence.Object where

import           Control.Monad                     (forM, void)
import           Data.Bifunctor                    (second)
import qualified Data.Map                          as Map
import           Data.Maybe                        (fromJust, isJust)
import           Data.Unique                       (hashUnique, newUnique)
import           HStream.Server.HStreamApi         (Subscription)
import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Utils
import           HStream.Utils                     (cBytesToText)
import qualified Z.Data.CBytes                     as CB
import           ZooKeeper                         (zooExists, zooGetChildren,
                                                    zooSet)
import           ZooKeeper.Recipe                  as Recipe
import           ZooKeeper.Types                   (StringVector (unStrVec),
                                                    StringsCompletion (strsCompletionValues),
                                                    ZHandle)

-------------------------------------------------------------------------------

instance {-# OVERLAPPABLE #-} BasicObjectPersistence ZHandle ('SubRep :: ObjRepType) Subscription where
  storeObject objId val zk = do
    uniq <- newUnique
    Recipe.withLock zk subscriptionsLockPath (CB.pack . show . hashUnique $ uniq) $ do
      zooExists zk subPath >>= \case
        Just _  -> void $ zooSet zk subPath (Just $ encodeValueToBytes val) Nothing
        Nothing -> createInsert zk subPath (encodeValueToBytes val)
    where subPath = mkSubscriptionPath objId

  getObject objId zk = decodeZNodeValue zk subPath
    where subPath = mkSubscriptionPath objId

  checkIfExist objId zk = isJust <$> zooExists zk (mkSubscriptionPath objId)

  listObjects zk = do
    sIds <- fmap cBytesToText . unStrVec . strsCompletionValues <$>
            zooGetChildren zk subscriptionsPath
    ms <- forM sIds (`getObject` zk)
    return $ Map.fromList $ second fromJust <$> filter (\(_,x) -> isJust x) (sIds `zip` ms)

  removeObject objId zk = tryDeletePath zk $ mkSubscriptionPath objId

  removeAllObjects zk = tryDeleteAllPath zk subscriptionsPath
