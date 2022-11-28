module HStream.Server.MetaData.Utils where

--------------------------------------------------------------------------------
-- Path

import           Control.Exception                (handle)
import           Control.Monad                    (unless, void)
import qualified Data.Text                        as T
import           GHC.Stack                        (HasCallStack)
import           ZooKeeper.Types                  (ZHandle)

import qualified Data.Aeson                       as A
import qualified Data.ByteString.Lazy             as BSL
import           HStream.Exception                (RQLiteTableAlreadyExists)
import           HStream.MetaStore.FileUtils      (Contents, createTables)
import           HStream.MetaStore.RqliteUtils    (createTable)
import           HStream.MetaStore.Types          (FHandle, MetaHandle,
                                                   MetaMulti (..),
                                                   MetaStore (..), RHandle (..))
import           HStream.MetaStore.ZookeeperUtils (tryCreate)
import           HStream.Server.HStreamApi        (Subscription (..))
import           HStream.Server.MetaData.Types    ()
import           HStream.Server.MetaData.Value    (fileTables, paths, tables)
import           HStream.Server.Types             (SubscriptionWrap (..))
import           System.Directory                 (doesFileExist)
import           System.FileLock

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = do
  mapM_ (tryCreate zk) paths

initializeTables :: RHandle -> IO ()
initializeTables (RHandle m url) = do
  mapM_ (handleExists . createTable m url) tables
  where
    handleExists = handle (\(_:: RQLiteTableAlreadyExists) -> pure ())

initializeFile :: FHandle -> IO ()
initializeFile fp = do
  fileExists <- doesFileExist fp
  unless fileExists $ void $ withTryFileLock fp Exclusive $ \_ -> BSL.writeFile fp (A.encode (mempty :: Contents))
  createTables fileTables fp

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
