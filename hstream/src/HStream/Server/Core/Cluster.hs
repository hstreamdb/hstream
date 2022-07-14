{-# LANGUAGE PatternSynonyms #-}

module HStream.Server.Core.Cluster
  ( describeCluster
  , lookupStream
  , lookupSubscription
  ) where

import           Control.Concurrent.STM           (readTVarIO)
import           Control.Exception                (throwIO)
import           Data.Text                        (Text)
import qualified Data.Vector                      as V

import           HStream.Common.ConsistentHashing (HashRing, getAllocatedNode)
import           HStream.Common.Types             (fromInternalServerNodeWithKey)
import           HStream.Gossip                   (getFailedNodes,
                                                   getMemberList)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception
import           HStream.Server.Handler.Common    (alignDefault)
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import           HStream.Utils                    (pattern EnumPB)

describeCluster :: ServerContext -> IO DescribeClusterResponse
describeCluster ServerContext{..} = do
  let protocolVer = Types.protocolVersion
      serverVer   = Types.serverVersion
  alives <- getMemberList gossipContext
  deads  <- getFailedNodes gossipContext
  alives' <- V.concat <$> mapM (fromInternalServerNodeWithKey scAdvertisedListenersKey) alives
  deads'  <- V.concat <$> mapM (fromInternalServerNodeWithKey scAdvertisedListenersKey) deads
  let nodesStatus =
          fmap (helper NodeStateRunning) alives'
       <> fmap (helper NodeStateDead   ) deads'

  return $ DescribeClusterResponse
    { describeClusterResponseProtocolVersion   = protocolVer
    , describeClusterResponseServerVersion     = serverVer
    , describeClusterResponseServerNodes       = alives'
    , describeClusterResponseServerNodesStatus = nodesStatus
    }
 where
  helper state node = ServerNodeStatus
    { serverNodeStatusNode  = Just node
    , serverNodeStatusState = EnumPB state
    }

lookupStream :: ServerContext -> LookupStreamRequest -> IO LookupStreamResponse
lookupStream ServerContext{..} req@LookupStreamRequest {
  lookupStreamRequestStreamName  = stream,
  lookupStreamRequestOrderingKey = orderingKey} = do
  Log.info $ "receive lookupStream request: " <> Log.buildString' req
  hashRing <- readTVarIO loadBalanceHashRing
  let key      = alignDefault orderingKey
  theNode <- getResNode hashRing (stream <> key) scAdvertisedListenersKey
  return $ LookupStreamResponse
    { lookupStreamResponseStreamName  = stream
    , lookupStreamResponseOrderingKey = orderingKey
    , lookupStreamResponseServerNode  = Just theNode
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

-------------------------------------------------------------------------------

getResNode :: HashRing -> Text -> Maybe Text -> IO ServerNode
getResNode hashRing hashKey listenerKey = do
  let serverNode = getAllocatedNode hashRing hashKey
  theNodes <- fromInternalServerNodeWithKey listenerKey serverNode
  if V.null theNodes then throwIO ObjectNotExist
                     else pure $ V.head theNodes
