{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.Server.Persistence.Utils where

--------------------------------------------------------------------------------
-- Path

import           Control.Exception                    (Exception, catch, handle,
                                                       throw)
import           Control.Monad                        (void)
import qualified Data.ByteString.Lazy.Char8           as BSL
import qualified Data.Text                            as T
import           GHC.Stack                            (HasCallStack)
import qualified Proto3.Suite.Class                   as Pb
import           Z.Data.CBytes                        (CBytes)
import           Z.Data.JSON                          (JSON, decode)
import           Z.Data.Vector                        (Bytes)
import qualified Z.Foreign                            as ZF
import           ZooKeeper                            (Resource, zooCreate,
                                                       zooCreateOpInit,
                                                       zooDelete, zooDeleteAll,
                                                       zooGet, zooSet,
                                                       zookeeperResInit)
import           ZooKeeper.Exception
import           ZooKeeper.Types

import qualified HStream.Logger                       as Log
import qualified HStream.Server.HStreamApi            as Api
import           HStream.Server.Persistence.Exception
import           HStream.Utils                        (textToCBytes)

rootPath :: CBytes
rootPath = "/hstreamdb/hstream"

serverRootPath :: CBytes
serverRootPath = rootPath <> "/servers"

leaderPath :: CBytes
leaderPath = rootPath <> "/leader"

queriesPath :: CBytes
queriesPath = rootPath <> "/queries"

connectorsPath :: CBytes
connectorsPath = rootPath <> "/connectors"

serverLoadPath :: CBytes
serverLoadPath = rootPath <> "/loadReports"

subscriptionsPath :: CBytes
subscriptionsPath = rootPath <> "/subscriptions"

paths :: [CBytes]
paths = ["/hstreamdb", rootPath, serverRootPath, leaderPath, serverLoadPath, queriesPath, connectorsPath, subscriptionsPath]

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = mapM_ (tryCreate zk) paths

mkQueryPath :: CBytes -> CBytes
mkQueryPath x = queriesPath <> "/" <> x

mkConnectorPath :: CBytes -> CBytes
mkConnectorPath x = connectorsPath <> "/" <> x

mkSubscriptionPath :: T.Text -> CBytes
mkSubscriptionPath x = subscriptionsPath <> "/" <> textToCBytes x

createInsert :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
createInsert zk path contents = do
  Log.debug . Log.buildString $ "create path " <> show path <> " with value"
  void $ zooCreate zk path (Just contents) zooOpenAclUnsafe ZooPersistent

createInsertOp :: HasCallStack => CBytes -> Bytes -> IO ZooOp
createInsertOp path contents = do
  Log.debug . Log.buildString $ "create path " <> show path <> " with value"
  return $ zooCreateOpInit path (Just contents) 64 zooOpenAclUnsafe ZooPersistent

setZkData :: HasCallStack => ZHandle -> CBytes -> Bytes -> IO ()
setZkData zk path contents =
  void $ zooSet zk path (Just contents) Nothing

tryCreate :: HasCallStack => ZHandle -> CBytes -> IO ()
tryCreate zk path = catch (createPath zk path) $
  \(_ :: ZNODEEXISTS) -> do
    -- Log.warning . Log.buildString $ "create path failed: " <> show path <> " has existed in zk"
    pure ()

createPath :: HasCallStack => ZHandle -> CBytes -> IO ()
createPath zk path = do
  Log.debug . Log.buildString $ "create path " <> show path
  void $ zooCreate zk path Nothing zooOpenAclUnsafe ZooPersistent

createPathOp :: HasCallStack => CBytes -> IO ZooOp
createPathOp path = do
  Log.debug . Log.buildString $ "create path " <> show path
  return $ zooCreateOpInit path Nothing 64 zooOpenAclUnsafe ZooPersistent

deletePath :: HasCallStack => ZHandle -> CBytes -> IO ()
deletePath zk path = do
  Log.debug . Log.buildString $ "delete path " <> show path
  void $ zooDelete zk path Nothing

deleteAllPath :: HasCallStack => ZHandle -> CBytes -> IO ()
deleteAllPath zk path = do
  Log.debug . Log.buildString $ "delete all path " <> show path
  void $ zooDeleteAll zk path

tryDeletePath :: HasCallStack => ZHandle -> CBytes -> IO ()
tryDeletePath zk path = catch (deletePath zk path) $
  \(_ :: ZNONODE) -> do
    Log.warning . Log.buildString $ "delete path error: " <> show path <> " not exist."
    pure ()

tryDeleteAllPath :: HasCallStack => ZHandle -> CBytes -> IO ()
tryDeleteAllPath zk path = catch (deleteAllPath zk path) $
  \(_ :: ZNONODE) -> do
    Log.warning . Log.buildString $ "delete all path error: " <> show path <> " not exist."
    pure ()

decodeQ :: JSON a => DataCompletion -> a
decodeQ = (\case { Right x -> x ; _ -> throw FailedToDecode}) . snd . decode
        . (\case { Nothing -> ""; Just x -> x}) . dataCompletionValue

ifThrow :: Exception e => e -> IO a -> IO a
ifThrow e = handle (\(_ :: ZooException) -> throwIO e)

getNodeValue :: ZHandle -> T.Text -> IO (Maybe Bytes)
getNodeValue zk sid = do
  let path = mkSubscriptionPath sid
  catch ((dataCompletionValue <$>) . zooGet zk $ path) $ \(err::ZooException) -> do
    Log.warning . Log.buildString $ "get node value from " <> show path <> "err: " <> show err
    return Nothing

encodeSubscription :: Api.Subscription -> Bytes
encodeSubscription = ZF.fromByteString . BSL.toStrict . Pb.toLazyByteString

decodeSubscription :: Bytes -> Maybe Api.Subscription
decodeSubscription origin =
  let sub = Pb.fromByteString . ZF.toByteString $ origin
   in case sub of
        Right res -> Just res
        Left _    -> Nothing

defaultHandle :: HasCallStack => CBytes -> Resource ZHandle
defaultHandle network = zookeeperResInit network 5000 Nothing 0
