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
import           Control.Exception                (throwIO)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V

import           HStream.Common.ConsistentHashing (HashRing, getAllocatedNode)
import           HStream.Common.Types             (fromInternalServerNodeWithKey)
import qualified HStream.Exception                as HE
import           HStream.Gossip                   (GossipContext (..),
                                                   getFailedNodes)
import           HStream.Gossip.Types             (ServerStatus (..))
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import           HStream.Utils                    (pattern EnumPB)

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

  return $ DescribeClusterResponse
    { describeClusterResponseProtocolVersion   = protocolVer
    , describeClusterResponseServerVersion     = serverVer
      -- TODO : If Cluster is not ready this should return empty
    , describeClusterResponseServerNodes       = self <> alives
    , describeClusterResponseServerNodesStatus = self' <> alives' <> deads'
    }
  where
    getListeners = fromInternalServerNodeWithKey scAdvertisedListenersKey
    helper state node = ServerNodeStatus
      { serverNodeStatusNode  = Just node
      , serverNodeStatusState = EnumPB state}

lookupShard :: ServerContext -> LookupShardRequest -> IO LookupShardResponse
lookupShard ServerContext{..} req@LookupShardRequest {
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
lookupConnector ServerContext{..} req@LookupConnectorRequest{
  lookupConnectorRequestName = name} = do
  Log.info $ "receive lookupConnector request: " <> Log.buildString (show req)
  hashRing <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing name scAdvertisedListenersKey
  return $ LookupConnectorResponse
    { lookupConnectorResponseName = name
    , lookupConnectorResponseServerNode     = Just theNode
    }

lookupSubscription
  :: ServerContext
  -> LookupSubscriptionRequest
  -> IO LookupSubscriptionResponse
lookupSubscription ServerContext{..} req@LookupSubscriptionRequest{
  lookupSubscriptionRequestSubscriptionId = subId} = do
  Log.info $ "receive lookupSubscription request: " <> Log.buildString (show req)
  hashRing <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing subId scAdvertisedListenersKey
  return $ LookupSubscriptionResponse
    { lookupSubscriptionResponseSubscriptionId = subId
    , lookupSubscriptionResponseServerNode     = Just theNode
    }

lookupShardReader :: ServerContext -> LookupShardReaderRequest -> IO LookupShardReaderResponse
lookupShardReader ServerContext{..} req@LookupShardReaderRequest{lookupShardReaderRequestReaderId=readerId} = do
  hashRing <- readTVarIO loadBalanceHashRing
  theNode  <- getResNode hashRing readerId scAdvertisedListenersKey
  Log.info $ "receive lookupShardReader request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardReaderResponse
    { lookupShardReaderResponseReaderId    = readerId
    , lookupShardReaderResponseServerNode  = Just theNode
    }

-------------------------------------------------------------------------------

getResNode :: HashRing -> Text -> Maybe Text -> IO ServerNode
getResNode hashRing hashKey listenerKey = do
  let serverNode = getAllocatedNode hashRing hashKey
  theNodes <- fromInternalServerNodeWithKey listenerKey serverNode
  if V.null theNodes then throwIO $ HE.NodesNotFound "Got empty nodes"
                     else pure $ V.head theNodes
