{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Kafka.Common.AclStore where

import           Control.Monad
import qualified Data.Map.Strict                as Map
import qualified Data.Text                      as T

import qualified HStream.Common.Server.MetaData as Meta
import           HStream.Common.ZookeeperClient (ZookeeperClient)
import           HStream.Kafka.Common.AclEntry
import           HStream.Kafka.Common.Resource
import           HStream.MetaStore.Types        ()
import qualified HStream.MetaStore.Types        as Meta

instance Meta.HasPath AclResourceNode ZookeeperClient where
  myRootPath = Meta.kafkaRootPath <> "/acls"
instance Meta.HasPath AclResourceNode Meta.RHandle where
  myRootPath = "acls"
instance Meta.HasPath AclResourceNode Meta.FHandle where
  myRootPath = "acls"

loadAllAcls :: (Meta.MetaType AclResourceNode a)
            => a
            -> (ResourcePattern -> Acls -> IO ())
            -> IO ()
loadAllAcls a aclsConsumer = do
  aclNodes <- Meta.getAllMeta @AclResourceNode a
  forM_ (Map.toList aclNodes) $ \(key, node) -> do
    case resourcePatternFromMetadataKey key of
      Nothing     -> error $ "Invalid key of resource pattern: " <> T.unpack key -- FIXME: error
      Just resPat -> do
        aclsConsumer resPat (aclResNodeAcls node)
