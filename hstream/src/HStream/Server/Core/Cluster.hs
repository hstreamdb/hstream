{-# LANGUAGE PatternSynonyms #-}

module HStream.Server.Core.Cluster
  ( describeCluster
  , lookupShard
  , lookupSubscription
  , lookupShardReader
  , lookupConnector
  ) where

import           Control.Concurrent               (tryReadMVar)
import           Control.Concurrent.STM           (readTVarIO)
import           Control.Exception                (SomeException, throwIO, try)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V

import           HStream.Common.ConsistentHashing (HashRing, getAllocatedNode)
import           HStream.Common.Types             (fromInternalServerNodeWithKey)
import qualified HStream.Exception                as HE
import           HStream.Gossip                   (GossipContext (..), getEpoch,
                                                   getFailedNodes,
                                                   getMemberList)
import           HStream.Gossip.Types             (ServerStatus (..))
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          (MetaStore (..))
import           HStream.Server.Core.Common       (ResourceType (..),
                                                   mkAllocationKey)
import           HStream.Server.HStreamApi
import           HStream.Server.MetaData          (TaskAllocation (..))
import           HStream.Server.MetaData.Value    (clusterStartTimeId)
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import qualified HStream.ThirdParty.Protobuf      as Proto
import           HStream.Utils                    (getProtoTimestamp,
                                                   pattern EnumPB)

describeCluster :: ServerContext -> IO DescribeClusterResponse
describeCluster ServerContext{gossipContext = gc@GossipContext{..}, ..} = do
  let protocolVer = Types.protocolVersion
      serverVer   = Types.serverVersion
  isReady <- tryReadMVar clusterReady
  self    <- getListeners serverSelf
  alives  <- readTVarIO serverList >>= fmap V.concat . mapM (getListeners . serverInfo) . Map.elems . snd
  deads   <- getFailedNodes gc     >>= fmap V.concat . mapM getListeners
  let self'   = helper (case isReady of Just _  -> NodeStateRunning; Nothing -> NodeStateStarting) <$> self
  let alives' = helper NodeStateRunning <$> alives
  let deads'  = helper NodeStateDead    <$> deads
  _currentTime@(Proto.Timestamp cSec _) <- getProtoTimestamp
  startTime <- getMeta @Proto.Timestamp clusterStartTimeId metaHandle
  return $ DescribeClusterResponse
    { describeClusterResponseProtocolVersion   = protocolVer
    , describeClusterResponseServerVersion     = serverVer
      -- TODO : If Cluster is not ready this should return empty
    , describeClusterResponseServerNodes       = self <> alives
    , describeClusterResponseServerNodesStatus = self' <> alives' <> deads'
    , describeClusterResponseClusterUpTime     = fromIntegral $ cSec - maybe cSec Proto.timestampSeconds startTime
    }
  where
    getListeners = fromInternalServerNodeWithKey scAdvertisedListenersKey
    helper state node = ServerNodeStatus
      { serverNodeStatusNode  = Just node
      , serverNodeStatusState = EnumPB state}


-- TODO: Currently we use the old version of lookup for minimal impact on performance
lookupShard :: ServerContext -> LookupShardRequest -> IO LookupShardResponse
lookupShard sc@ServerContext{..} req@LookupShardRequest {
  lookupShardRequestShardId = shardId} = do
  hashRing <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing (T.pack $ show shardId) scAdvertisedListenersKey
  Log.info $ "receive lookupShard request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardResponse
    { lookupShardResponseShardId    = shardId
    , lookupShardResponseServerNode = Just theNode
    }

lookupConnector
  :: ServerContext
  -> LookupConnectorRequest
  -> IO LookupConnectorResponse
lookupConnector sc req@LookupConnectorRequest{
  lookupConnectorRequestName = name} = do
  Log.info $ "receive lookupConnector request: " <> Log.buildString (show req)
  theNode <- lookupResource sc ResConnector name
  return $ LookupConnectorResponse
    { lookupConnectorResponseName = name
    , lookupConnectorResponseServerNode     = Just theNode
    }

lookupSubscription
  :: ServerContext
  -> LookupSubscriptionRequest
  -> IO LookupSubscriptionResponse
lookupSubscription sc req@LookupSubscriptionRequest{
  lookupSubscriptionRequestSubscriptionId = subId} = do
  Log.info $ "receive lookupSubscription request: " <> Log.buildString (show req)
  theNode <- lookupResource sc ResSubscription subId
  return $ LookupSubscriptionResponse
    { lookupSubscriptionResponseSubscriptionId = subId
    , lookupSubscriptionResponseServerNode     = Just theNode
    }

lookupShardReader :: ServerContext -> LookupShardReaderRequest -> IO LookupShardReaderResponse
lookupShardReader sc@ServerContext{..} req@LookupShardReaderRequest{lookupShardReaderRequestReaderId=readerId} = do
  theNode <- lookupResource sc ResShardReader readerId
  Log.info $ "receive lookupShardReader request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardReaderResponse
    { lookupShardReaderResponseReaderId    = readerId
    , lookupShardReaderResponseServerNode  = Just theNode
    }

lookupResource :: ServerContext -> ResourceType -> Text -> IO ServerNode
lookupResource sc@ServerContext{..} rtype rid = do
  let metaId = mkAllocationKey rtype rid
  -- FIXME: it will insert the results of lookup no matter the resource exists or not
  getMetaWithVer @TaskAllocation metaId metaHandle >>= \case
    Nothing -> do
      hashRing <- readTVarIO loadBalanceHashRing
      epoch <- getEpoch gossipContext
      theNode <- getResNode hashRing rid scAdvertisedListenersKey
      try (insertMeta @TaskAllocation metaId (TaskAllocation epoch theNode) metaHandle) >>=
        \case
          Left (e :: SomeException) -> lookupResource sc rtype rid
          Right ()                  -> return theNode
    Just (TaskAllocation epoch theNode, version) -> do
      serverList <- getMemberList gossipContext >>= fmap V.concat . mapM (fromInternalServerNodeWithKey scAdvertisedListenersKey)
      epoch' <- getEpoch gossipContext
      if theNode `V.elem` serverList
        then return theNode
        else do
          if epoch' > epoch
            then do
              hashRing <- readTVarIO loadBalanceHashRing
              theNode' <- getResNode hashRing rid scAdvertisedListenersKey
              try (updateMeta @TaskAllocation metaId (TaskAllocation epoch' theNode') (Just version) metaHandle) >>=
                \case
                  Left (e :: SomeException) -> lookupResource sc rtype rid
                  Right ()                  -> return theNode'
            else do
              Log.warning "LookupResource: the server has not yet synced with the latest member list "
              throwIO $ HE.ResourceAllocationException "the server has not yet synced with the latest member list"

-------------------------------------------------------------------------------

getResNode :: HashRing -> Text -> Maybe Text -> IO ServerNode
getResNode hashRing hashKey listenerKey = do
  let serverNode = getAllocatedNode hashRing hashKey
  theNodes <- fromInternalServerNodeWithKey listenerKey serverNode
  if V.null theNodes then throwIO $ HE.NodesNotFound "Got empty nodes"
                     else pure $ V.head theNodes
