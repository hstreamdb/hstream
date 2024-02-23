module HStream.Server.MetaData.Utils where

import           Control.Monad
import qualified Data.Text                      as T
import           GHC.Stack                      (HasCallStack)
import           ZooKeeper.Types                (ZHandle)

import           HStream.Common.Server.MetaData (initializeFileTables,
                                                 initializeRqTables,
                                                 initializeZkPaths)
import           HStream.MetaStore.Types        (FHandle, MetaHandle,
                                                 MetaMulti (..), MetaStore (..),
                                                 RHandle (..))
import           HStream.Server.HStreamApi      (Subscription (..))
import           HStream.Server.MetaData.Value  (fileTables, paths, tables)
import           HStream.Server.Types           (SubscriptionWrap (..))

initHStreamZkPaths :: HasCallStack => ZHandle -> IO ()
initHStreamZkPaths zk = initializeZkPaths zk paths

initHStreamRqTables ::  RHandle -> IO ()
initHStreamRqTables rq = initializeRqTables rq tables

initHStreamFileTables :: HasCallStack => FHandle -> IO ()
initHStreamFileTables fh = initializeFileTables fh fileTables

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
