module HStream.Server.MetaData.Utils where

--------------------------------------------------------------------------------
-- Path

import           Control.Monad                    (void)
import qualified Data.Text                        as T
import           GHC.Stack                        (HasCallStack)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.MetaStore.RqliteUtils    (createTable)
import           HStream.MetaStore.Types          (MetaHandle, MetaMulti (..),
                                                   MetaStore (..), RHandle (..))
import           HStream.MetaStore.ZookeeperUtils (tryCreate)
import           HStream.Server.HStreamApi        (Subscription (..))
import           HStream.Server.MetaData.Types    ()
import           HStream.Server.MetaData.Value    (paths, tables)
import           HStream.Server.Types             (SubscriptionWrap (..))

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = do
  mapM_ (tryCreate zk) paths

initializeTables :: RHandle -> IO ()
initializeTables (RHandle m url) = do
  mapM_ (createTable m url) tables

-- FIXME: Concurrency
updateSubscription :: MetaHandle -> T.Text -> T.Text ->  IO ()
updateSubscription h sName sName' = do
  subs <- listMeta @SubscriptionWrap h
  let ops = [ updateMetaOp subscriptionSubscriptionId (update sub wrap) Nothing h
            | wrap@SubscriptionWrap{originSub=sub@Subscription{..}} <- subs, subscriptionStreamName == sName ]
  void $ metaMulti ops h
  where
   update sub wrap = let newSub = sub { subscriptionStreamName = sName' }
                      in wrap { originSub = newSub }
