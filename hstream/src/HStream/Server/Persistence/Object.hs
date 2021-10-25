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
import           HStream.Server.HStreamApi         (Subscription)
import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Utils
import           HStream.Server.Types              (ProducerContext,
                                                    SubscriptionContext)
import           HStream.Utils                     (cBytesToText)
import           ZooKeeper                         (zooExists, zooGetChildren,
                                                    zooSet)
import           ZooKeeper.Types                   (StringVector (unStrVec),
                                                    StringsCompletion (strsCompletionValues),
                                                    ZHandle)

-------------------------------------------------------------------------------

instance {-# OVERLAPPABLE #-} BasicObjectPersistence ZHandle ('SubRep :: ObjRepType) Subscription where
  storeObject objId val zk = do
    createInsert zk subPath (encodeValueToBytes val)
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

instance {-# OVERLAPPABLE #-} BasicObjectPersistence ZHandle ('SubCtxRep :: ObjRepType) SubscriptionContext where
  storeObject objId val zk = do
    zooExists zk subPath >>= \case
      Just _  -> void $ zooSet zk subPath (Just $ encodeValueToBytes val) Nothing
      Nothing -> createInsert zk subPath (encodeValueToBytes val)
    where subPath = mkSubscriptionCtxPath objId

  getObject objId zk = decodeZNodeValue zk subPath
    where subPath = mkSubscriptionCtxPath objId

  checkIfExist objId zk = isJust <$> zooExists zk (mkSubscriptionCtxPath objId)

  listObjects zk = do
    sIds <- fmap cBytesToText . unStrVec . strsCompletionValues <$>
            zooGetChildren zk subscriptionCtxsPath
    ms <- forM sIds (`getObject` zk)
    return $ Map.fromList $ second fromJust <$> filter (\(_,x) -> isJust x) (sIds `zip` ms)

  removeObject objId zk = tryDeletePath zk $ mkSubscriptionCtxPath objId

  removeAllObjects zk = tryDeleteAllPath zk subscriptionCtxsPath

instance {-# OVERLAPPABLE #-} BasicObjectPersistence ZHandle ('PrdCtxRep :: ObjRepType) ProducerContext where
  storeObject objId val zk = do
    zooExists zk subPath >>= \case
      Just _  -> void $ zooSet zk subPath (Just $ encodeValueToBytes val) Nothing
      Nothing -> createInsert zk subPath (encodeValueToBytes val)
    where subPath = mkProducerCtxPath objId

  getObject objId zk = decodeZNodeValue zk subPath
    where subPath = mkProducerCtxPath objId

  checkIfExist objId zk = isJust <$> zooExists zk (mkProducerCtxPath objId)

  listObjects zk = do
    sIds <- fmap cBytesToText . unStrVec . strsCompletionValues <$>
            zooGetChildren zk producerCtxsPath
    ms <- forM sIds (`getObject` zk)
    return $ Map.fromList $ second fromJust <$> filter (\(_,x) -> isJust x) (sIds `zip` ms)

  removeObject objId zk = tryDeletePath zk $ mkProducerCtxPath objId

  removeAllObjects zk = tryDeleteAllPath zk producerCtxsPath
