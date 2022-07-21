{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TypeFamilies          #-}

module HStream.Server.Persistence.Object where

import           Control.Exception                    (handle)
import           Control.Monad                        (forM, void)
import           Data.Bifunctor                       (second)
import qualified Data.Map                             as Map
import           Data.Maybe                           (catMaybes, fromJust,
                                                       isJust)
import           Data.Text                            (Text)
import           Data.Unique                          (hashUnique, newUnique)
import qualified Z.Data.CBytes                        as CB
import           ZooKeeper                            (zooExists,
                                                       zooGetChildren, zooMulti,
                                                       zooSetOpInit)
import           ZooKeeper.Exception
import qualified ZooKeeper.Recipe                     as Recipe
import           ZooKeeper.Types                      (StringVector (..),
                                                       StringsCompletion (..),
                                                       ZHandle)

import           HStream.Server.HStreamApi            (Subscription (..))
import           HStream.Server.Persistence.Common
import           HStream.Server.Persistence.Exception
import           HStream.Server.Persistence.Utils
import           HStream.Server.Types                 (SubscriptionWrap (..))
import           HStream.Utils                        (cBytesToText)

-------------------------------------------------------------------------------

instance {-# OVERLAPPABLE #-} BasicObjectPersistence ZHandle ('SubRep :: ObjRepType) SubscriptionWrap where
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

  removeObject objId zk = deletePath zk $ mkSubscriptionPath objId

  removeAllObjects zk = tryDeleteAllPath zk subscriptionsPath

getSubscriptions :: ZHandle -> IO [SubscriptionWrap]
getSubscriptions zk = do
  subscriptionIds <- map cBytesToText <$> tryGetChildren zk subscriptionsPath
  catMaybes <$> mapM (flip (getObject @ZHandle @'SubRep) zk) subscriptionIds

getSubscriptionWithStream :: ZHandle -> Text -> IO [SubscriptionWrap]
getSubscriptionWithStream zk sName = do
  subscriptionIds <- map cBytesToText <$> tryGetChildren zk subscriptionsPath
  catMaybes <$> mapM (filterWithStream zk) subscriptionIds
  where
    filterWithStream zkHandle subId = do
      maybeSub <- getObject @ZHandle @'SubRep subId zkHandle
      return $ case maybeSub of
        Nothing -> Nothing
        Just sub@SubscriptionWrap{originSub=Subscription{..}}
          | subscriptionStreamName == sName -> Just sub
          | otherwise -> Nothing

updateSubscription :: ZHandle -> Text -> Text ->  IO ()
updateSubscription zk sName sName' = do
  withSubscriptionsLock zk $ do
    subs <- getSubscriptions zk
    let ops = [ zooSetOpInit (mkSubscriptionPath subscriptionSubscriptionId)
                             (Just $ encodeValueToBytes $ update sub wrap)
                             Nothing
              | wrap@SubscriptionWrap{originSub=sub@Subscription{..}} <- subs, subscriptionStreamName == sName ]
    zooMulti zk ops
 where
   update sub wrap = let newSub = sub { subscriptionStreamName = sName' }
                      in wrap { originSub = newSub }

withSubscriptionsLock :: ZHandle -> IO a -> IO ()
withSubscriptionsLock zk action = do
  uniq <- newUnique
  void $ Recipe.withLock zk subscriptionsLockPath (CB.pack . show . hashUnique $ uniq) action
