{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module HStream.Server.Persistence.Subscriptions where

import           Control.Monad                    (forM)
import           Data.Maybe                       (catMaybes, isJust)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           GHC.Stack                        (HasCallStack)
import           ZooKeeper                        (zooExists, zooGetChildren)
import           ZooKeeper.Types                  (StringVector (unStrVec),
                                                   StringsCompletion (strsCompletionValues),
                                                   ZHandle)

import qualified HStream.Logger                   as Log
import qualified HStream.Server.HStreamApi        as Api
import           HStream.Server.Persistence.Utils
import           HStream.Utils                    (cBytesToText)

class SubPersistence handle where
  -- | persistent a subscription to store
  storeSubscription :: HasCallStack => Api.Subscription -> handle -> IO()
  -- | if specific subscription exist, getSubscription will return the subscription, else it
  --   will return nothing
  getSubscription :: HasCallStack => T.Text -> handle -> IO (Maybe Api.Subscription)
  -- | check if specified subscription exist
  checkIfExist :: HasCallStack => T.Text -> handle -> IO Bool
  -- | return all subscriptions
  listSubscriptions :: HasCallStack => handle -> IO [Api.Subscription]
  -- | remove specified subscription
  removeSubscription :: HasCallStack => T.Text -> handle -> IO()
  -- | remove all subscriptions
  removeAllSubscriptions :: HasCallStack => handle -> IO ()

-------------------------------------------------------------------------------

instance SubPersistence ZHandle where
  storeSubscription sub@Api.Subscription{..} zk = createInsert zk subPath . encodeSubscription $ sub
    where
      sid = TL.toStrict subscriptionSubscriptionId
      subPath = mkSubscriptionPath sid

  getSubscription sid zk = do
    res <- getNodeValue zk sid
    case res of
      Just value -> do
        return $ decodeSubscription value
      Nothing -> do
        Log.debug $ "getSubscription get nothing, subscriptionID = " <> Log.buildText sid
        return Nothing

  checkIfExist sid zk = isJust <$> zooExists zk (mkSubscriptionPath sid)

  listSubscriptions zk = do
    sIds <- fmap cBytesToText . unStrVec . strsCompletionValues <$> zooGetChildren zk subscriptionsPath
    catMaybes <$> forM sIds (`getSubscription` zk)

  removeSubscription subscriptionID zk = tryDeletePath zk $ mkSubscriptionPath subscriptionID

  removeAllSubscriptions zk = tryDeleteAllPath zk subscriptionsPath
