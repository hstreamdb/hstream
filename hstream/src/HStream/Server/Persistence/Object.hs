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

import           Control.Exception                 (handle)
import           Control.Monad                     (forM)
import           Data.Bifunctor                    (second)
import qualified Data.Map                          as Map
import           Data.Maybe                        (fromJust, isJust)
import           ZooKeeper                         (zooExists, zooGetChildren)
import           ZooKeeper.Exception
import           ZooKeeper.Types                   (StringVector (..),
                                                    StringsCompletion (..),
                                                    ZHandle)

import           HStream.Server.Exception
import           HStream.Server.HStreamApi         (Subscription)
import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Utils
import           HStream.Utils                     (cBytesToText)

-------------------------------------------------------------------------------

instance {-# OVERLAPPABLE #-} BasicObjectPersistence ZHandle ('SubRep :: ObjRepType) Subscription where
  storeObject objId val zk =
    handleExist $ createInsert zk subPath (encodeValueToBytes val)
    where
      subPath = mkSubscriptionPath objId
      handleExist = handle (\(_ :: ZNODEEXISTS) -> throwIO $ SubscriptionIdOccupied objId)

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
