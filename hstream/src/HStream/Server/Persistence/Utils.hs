{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Persistence.Utils
  ( defaultHandle

  , rootPath
  , serverRootPath
  , leaderPath
  , lockPath
  , queriesPath
  , connectorsPath
  , serverLoadPath
  , serverIdPath
  , subscriptionsPath
  , subscriptionCtxsPath
  , producerCtxsPath
  , subscriptionsLockPath
  , subscriptionCtxsLockPath
  , producerCtxsLockPath
  , paths

  , initializeAncestors
  , mkQueryPath
  , mkConnectorPath
  , mkSubscriptionPath
  , mkSubscriptionCtxPath
  , mkProducerCtxPath

  , createInsert
  , createInsertOp
  , createPath
  , tryCreate
  , createPathOp
  , setZkData
  , deletePath
  , tryDeletePath
  , deleteAllPath
  , tryDeleteAllPath
  , decodeDataCompletion
  , decodeDataCompletion'
  , decodeZNodeValue
  , decodeZNodeValue'
  , encodeValueToBytes

  , ifThrow
  ) where

--------------------------------------------------------------------------------
-- Path

import           Control.Exception                    (Exception, catch, handle,
                                                       throw, try)
import           Control.Monad                        (void)
import           Data.Aeson                           (FromJSON, ToJSON)
import qualified Data.Aeson                           as Aeson
import qualified Data.ByteString.Lazy                 as BL
import           Data.Functor                         ((<&>))
import qualified Data.Text                            as T
import           GHC.Stack                            (HasCallStack)
import           Z.Data.CBytes                        (CBytes)
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
import           HStream.Server.Persistence.Exception
import           HStream.Utils                        (textToCBytes)

defaultHandle :: HasCallStack => CBytes -> Resource ZHandle
defaultHandle network = zookeeperResInit network 5000 Nothing 0

rootPath :: CBytes
rootPath = "/hstreamdb/hstream"

serverRootPath :: CBytes
serverRootPath = rootPath <> "/servers"

serverIdPath :: CBytes
serverIdPath = rootPath <> "/serverIds"

leaderPath :: CBytes
leaderPath = rootPath <> "/leader"

lockPath :: CBytes
lockPath = rootPath <> "/lock"

queriesPath :: CBytes
queriesPath = rootPath <> "/queries"

connectorsPath :: CBytes
connectorsPath = rootPath <> "/connectors"

serverLoadPath :: CBytes
serverLoadPath = rootPath <> "/loadReports"

subscriptionsPath :: CBytes
subscriptionsPath = rootPath <> "/subscriptions"

subscriptionsLockPath :: CBytes
subscriptionsLockPath = lockPath <> "/subscriptions"

subscriptionCtxsPath :: CBytes
subscriptionCtxsPath = rootPath <> "/subscriptionCtxs"

subscriptionCtxsLockPath :: CBytes
subscriptionCtxsLockPath = lockPath <> "/subscriptionCtxs"

producerCtxsPath :: CBytes
producerCtxsPath = rootPath <> "/producerCtxs"

producerCtxsLockPath :: CBytes
producerCtxsLockPath = lockPath <> "/producerCtxs"

paths :: [CBytes]
paths = [ "/hstreamdb"
        , rootPath
        , serverRootPath
        , serverIdPath
        , leaderPath
        , lockPath
        , serverLoadPath
        , queriesPath
        , connectorsPath
        , subscriptionsPath
        , subscriptionCtxsPath
        , producerCtxsPath
        , subscriptionsLockPath
        , subscriptionCtxsLockPath
        , producerCtxsLockPath
        ]

initializeAncestors :: HasCallStack => ZHandle -> IO ()
initializeAncestors zk = do
  mapM_ (tryCreate zk) paths

mkQueryPath :: CBytes -> CBytes
mkQueryPath x = queriesPath <> "/" <> x

mkConnectorPath :: CBytes -> CBytes
mkConnectorPath x = connectorsPath <> "/" <> x

mkSubscriptionPath :: T.Text -> CBytes
mkSubscriptionPath x = subscriptionsPath <> "/" <> textToCBytes x

mkSubscriptionCtxPath :: T.Text -> CBytes
mkSubscriptionCtxPath x = subscriptionCtxsPath <> "/" <> textToCBytes x

mkProducerCtxPath :: T.Text -> CBytes
mkProducerCtxPath x = producerCtxsPath <> "/" <> textToCBytes x

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
  \(_ :: ZNODEEXISTS) -> pure ()

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
    pure ()

tryDeleteAllPath :: HasCallStack => ZHandle -> CBytes -> IO ()
tryDeleteAllPath zk path = catch (deleteAllPath zk path) $
  \(_ :: ZNONODE) -> do
    pure ()

decodeDataCompletion :: FromJSON a => DataCompletion -> Maybe a
decodeDataCompletion (DataCompletion (Just x) _) =
  case Aeson.eitherDecode' . BL.fromStrict . ZF.toByteString $ x of
    Right a -> Just a
    Left _  -> Nothing
decodeDataCompletion (DataCompletion Nothing _) = Nothing

-- FIXME: let 'Nothing' lead to more detailed exception?
decodeDataCompletion' :: FromJSON a => DataCompletion -> a
decodeDataCompletion' (DataCompletion (Just x) _) =
  case Aeson.eitherDecode' . BL.fromStrict . ZF.toByteString $ x of
    Right a -> a
    Left _  -> throw FailedToDecode
decodeDataCompletion' (DataCompletion Nothing _) = throw FailedToDecode

decodeZNodeValue :: FromJSON a => ZHandle -> CBytes -> IO (Maybe a)
decodeZNodeValue zk nodePath = do
  e_a <- try $ zooGet zk nodePath
  case e_a of
    Left (_ :: ZooException) -> return Nothing
    Right a                  -> return $ decodeDataCompletion a

decodeZNodeValue' :: FromJSON a => ZHandle -> CBytes -> IO a
decodeZNodeValue' zk nodePath =
  zooGet zk nodePath <&> decodeDataCompletion'

encodeValueToBytes :: ToJSON a => a -> Bytes
encodeValueToBytes = ZF.fromByteString . BL.toStrict . Aeson.encode

ifThrow :: Exception e => e -> IO a -> IO a
ifThrow e = handle (\(_ :: ZooException) -> throwIO e)
