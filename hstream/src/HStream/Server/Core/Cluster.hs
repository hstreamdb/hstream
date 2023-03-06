{-# LANGUAGE PatternSynonyms #-}

module HStream.Server.Core.Cluster
  ( describeCluster
  , lookupResource

  , lookupShard
  , lookupSubscription
  , lookupShardReader
  , lookupConnector
  ) where

import           Control.Concurrent            (tryReadMVar)
import           Control.Concurrent.STM        (readTVarIO)
import           Control.Exception             (throwIO)
import qualified Data.List                     as L
import qualified Data.Map.Strict               as Map
import qualified Data.Text                     as T
import qualified Data.Vector                   as V
import           Proto3.Suite                  (Enumerated (..))

import           HStream.Common.Types          (fromInternalServerNodeWithKey)
import qualified HStream.Exception             as HE
import           HStream.Gossip                (GossipContext (..),
                                                getFailedNodes, getMemberList)
import           HStream.Gossip.Types          (ServerStatus (..))
import qualified HStream.Logger                as Log
import           HStream.MetaStore.Types       (MetaStore (..))
import           HStream.Server.Core.Common    (getResNode, lookupResource')
import           HStream.Server.HStreamApi
import           HStream.Server.MetaData.Value (clusterStartTimeId)
import           HStream.Server.Types          (ServerContext (..))
import qualified HStream.Server.Types          as Types
import qualified HStream.ThirdParty.Protobuf   as Proto
import           HStream.Utils                 (ResourceType (..),
                                                getProtoTimestamp,
                                                pattern EnumPB)

describeCluster :: ServerContext -> IO DescribeClusterResponse
describeCluster ServerContext{gossipContext = gc@GossipContext{..}, ..} = do
  let protocolVer = Types.protocolVersion
      serverVer   = Types.serverVersion
  isReady <- tryReadMVar clusterReady
  self    <- getListeners serverSelf
  alives  <- getMemberList gc >>= fmap V.concat . mapM getListeners . (L.delete serverSelf)
  deads   <- getFailedNodes gc >>= fmap V.concat . mapM getListeners
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

lookupResource :: ServerContext -> LookupResourceRequest -> IO ServerNode
lookupResource sc LookupResourceRequest{..} = do
  case lookupResourceRequestResType of
    Enumerated (Right rType) -> lookupResource' sc rType lookupResourceRequestResId
    x -> throwIO $ HE.InvalidResourceType (show x)

-- TODO: Currently we use the old version of lookup for minimal impact on performance
lookupShard :: ServerContext -> LookupShardRequest -> IO LookupShardResponse
lookupShard ServerContext{..} req@LookupShardRequest {
  lookupShardRequestShardId = shardId} = do
  (_, hashRing) <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing (T.pack $ show shardId) scAdvertisedListenersKey
  Log.info $ "receive lookupShard request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardResponse
    { lookupShardResponseShardId    = shardId
    , lookupShardResponseServerNode = Just theNode
    }

{-# DEPRECATED lookupConnector "Use lookupResource instead" #-}
lookupConnector
  :: ServerContext
  -> LookupConnectorRequest
  -> IO LookupConnectorResponse
lookupConnector sc req@LookupConnectorRequest{
  lookupConnectorRequestName = name} = do
  Log.info $ "receive lookupConnector request: " <> Log.buildString (show req)
  theNode <- lookupResource' sc ResConnector name
  return $ LookupConnectorResponse
    { lookupConnectorResponseName = name
    , lookupConnectorResponseServerNode     = Just theNode
    }

{-# DEPRECATED lookupSubscription "Use lookupResource instead" #-}
lookupSubscription
  :: ServerContext
  -> LookupSubscriptionRequest
  -> IO LookupSubscriptionResponse
lookupSubscription sc req@LookupSubscriptionRequest{
  lookupSubscriptionRequestSubscriptionId = subId} = do
  Log.info $ "receive lookupSubscription request: " <> Log.buildString (show req)
  theNode <- lookupResource' sc ResSubscription subId
  return $ LookupSubscriptionResponse
    { lookupSubscriptionResponseSubscriptionId = subId
    , lookupSubscriptionResponseServerNode     = Just theNode
    }

{-# DEPRECATED lookupShardReader "Use lookupResource instead" #-}
lookupShardReader :: ServerContext -> LookupShardReaderRequest -> IO LookupShardReaderResponse
lookupShardReader sc req@LookupShardReaderRequest{lookupShardReaderRequestReaderId=readerId} = do
  theNode <- lookupResource' sc ResShardReader readerId
  Log.info $ "receive lookupShardReader request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardReaderResponse
    { lookupShardReaderResponseReaderId    = readerId
    , lookupShardReaderResponseServerNode  = Just theNode
    }
