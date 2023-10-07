module HStream.Common.Server.Lookup
  ( KafkaResource (..)
  , lookupKafka
  , lookupKafkaPersist

    -- * Internals
  , lookupNode
  , lookupNodePersist
  ) where

import           Control.Concurrent.STM
import           Control.Exception                (SomeException (..), throwIO,
                                                   try)
import           Data.List                        (find)
import           Data.Text                        (Text)
import qualified Data.Vector                      as V

import           HStream.Common.ConsistentHashing (getResNode)
import           HStream.Common.Server.HashRing   (LoadBalanceHashRing)
import           HStream.Common.Server.MetaData   (TaskAllocation (..))
import           HStream.Common.Types             (fromInternalServerNodeWithKey)
import qualified HStream.Exception                as HE
import           HStream.Gossip                   (GossipContext, getMemberList)
import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.Types          as M
import qualified HStream.Server.HStreamApi        as A

lookupNode :: LoadBalanceHashRing -> Text -> Maybe Text -> IO A.ServerNode
lookupNode loadBalanceHashRing key advertisedListenersKey = do
  (_, hashRing) <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing key advertisedListenersKey
  return theNode

lookupNodePersist
  :: M.MetaHandle
  -> GossipContext
  -> LoadBalanceHashRing
  -> Text
  -> Text
  -> Maybe Text
  -> IO A.ServerNode
lookupNodePersist metaHandle gossipContext loadBalanceHashRing
                  key metaId advertisedListenersKey = do
  -- FIXME: it will insert the results of lookup no matter the resource exists
  -- or not
  M.getMetaWithVer @TaskAllocation metaId metaHandle >>= \case
    Nothing -> do
      (epoch, hashRing) <- readTVarIO loadBalanceHashRing
      theNode <- getResNode hashRing key advertisedListenersKey
      try (M.insertMeta @TaskAllocation
             metaId
             (TaskAllocation epoch (A.serverNodeId theNode))
             metaHandle) >>= \case
        Left (e :: SomeException) -> do
          -- TODO: add a retry limit here
          Log.warning $ "lookupNodePersist exception: " <> Log.buildString' e
                     <> ", retry..."
          lookupNodePersist metaHandle gossipContext loadBalanceHashRing
                            key metaId advertisedListenersKey
        Right () -> return theNode
    Just (TaskAllocation epoch nodeId, version) -> do
      serverList <- getMemberList gossipContext >>=
        fmap V.concat . mapM (fromInternalServerNodeWithKey advertisedListenersKey)
      case find ((nodeId == ) . A.serverNodeId) serverList of
        Just theNode -> return theNode
        Nothing -> do
          (epoch', hashRing) <- readTVarIO loadBalanceHashRing
          if epoch' > epoch
            then do
              theNode' <- getResNode hashRing key advertisedListenersKey
              try (M.updateMeta @TaskAllocation metaId
                     (TaskAllocation epoch' (A.serverNodeId theNode'))
                     (Just version) metaHandle) >>= \case
                Left (e :: SomeException) -> do
                  -- TODO: add a retry limit here
                  Log.warning $ "lookupNodePersist exception: " <> Log.buildString' e
                             <> ", retry..."
                  lookupNodePersist metaHandle gossipContext loadBalanceHashRing
                                    key metaId advertisedListenersKey
                Right () -> return theNode'
            else do
              let errmsg = "the server has not yet synced with the latest member list"
              Log.warning $ "lookupNodePersist: " <> Log.buildString errmsg
              throwIO $ HE.ResourceAllocationException errmsg

data KafkaResource
  = KafkaResTopic Text
  | KafkaResGroup Text

kafkaResourceKey :: KafkaResource -> Text
kafkaResourceKey (KafkaResTopic name) = name
kafkaResourceKey (KafkaResGroup name) = name

kafkaResourceMetaId :: KafkaResource -> Text
kafkaResourceMetaId (KafkaResTopic name) = "KafkaResTopic_" <> name
kafkaResourceMetaId (KafkaResGroup name) = "KafkaResGruop_" <> name

lookupKafka :: LoadBalanceHashRing -> Maybe Text -> KafkaResource -> IO A.ServerNode
lookupKafka lbhr alk res = lookupNode lbhr (kafkaResourceKey res) alk

lookupKafkaPersist
  :: M.MetaHandle
  -> GossipContext
  -> LoadBalanceHashRing
  -> Maybe Text
  -> KafkaResource
  -> IO A.ServerNode
lookupKafkaPersist mh gc lbhr alk kafkaResource =
  let key = kafkaResourceKey kafkaResource
      metaId = kafkaResourceMetaId kafkaResource
   in lookupNodePersist mh gc lbhr key metaId alk
