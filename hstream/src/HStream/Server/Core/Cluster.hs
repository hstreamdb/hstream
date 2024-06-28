module HStream.Server.Core.Cluster
  ( describeCluster
  , lookupResource

  , lookupShard
  , lookupSubscription
  , lookupShardReader
  , lookupKey

  , nodeChangeEventHandler
  , recoverLocalTasks
  ) where

import           Control.Concurrent             (MVar, withMVar)
import           Control.Exception              (Exception (displayException),
                                                 Handler (..),
                                                 SomeException (..), catches)
import           Control.Monad                  (forM_, when)
import qualified Data.Text                      as T

import           HStream.Common.Server.Lookup   (lookupNode)
import           HStream.Common.Server.MetaData (clusterStartTimeId)
import qualified HStream.Exception              as HE
import           HStream.Gossip                 (GossipContext (..))
import qualified HStream.Gossip                 as Gossip
import qualified HStream.Gossip.Types           as Gossip
import qualified HStream.Logger                 as Log
import           HStream.MetaStore.Types        (MetaStore (..))
import           HStream.Server.Core.Common     (lookupResource)
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamInternal as I
import           HStream.Server.QueryWorker     (QueryWorker (QueryWorker))
import           HStream.Server.Types           (ServerContext (..))
import qualified HStream.Server.Types           as Types
import qualified HStream.ThirdParty.Protobuf    as Proto
import           HStream.Utils                  (ResourceType (..),
                                                 getProtoTimestamp)

describeCluster :: ServerContext -> IO DescribeClusterResponse
describeCluster ServerContext{gossipContext = gc@GossipContext{..}, ..} = do
  let protocolVer = Types.protocolVersion
  (serverNodes, serverNodesStatus) <- Gossip.describeCluster gc scAdvertisedListenersKey
  _currentTime@(Proto.Timestamp cSec _) <- getProtoTimestamp
  startTime <- getMeta @Proto.Timestamp clusterStartTimeId metaHandle
  return $ DescribeClusterResponse
    { describeClusterResponseProtocolVersion   = protocolVer
    , describeClusterResponseServerNodes       = serverNodes
    , describeClusterResponseServerNodesStatus = serverNodesStatus
    , describeClusterResponseClusterUpTime     = fromIntegral $ cSec - maybe cSec Proto.timestampSeconds startTime
    }

-- TODO: Currently we use the old version of lookup for minimal impact on performance
lookupShard :: ServerContext -> LookupShardRequest -> IO LookupShardResponse
lookupShard ServerContext{..} req@LookupShardRequest {
  lookupShardRequestShardId = shardId} = do
  theNode <- lookupNode loadBalanceHashRing (T.pack $ show shardId) scAdvertisedListenersKey
  Log.info $ "receive lookupShard request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardResponse
    { lookupShardResponseShardId    = shardId
    , lookupShardResponseServerNode = Just theNode
    }

lookupKey :: ServerContext -> LookupKeyRequest -> IO ServerNode
lookupKey ServerContext{..} req@LookupKeyRequest{..} = do
  theNode <- lookupNode loadBalanceHashRing lookupKeyRequestPartitionKey scAdvertisedListenersKey
  Log.info $ "receive lookupKey request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return theNode

{-# DEPRECATED lookupSubscription "Use lookupResource instead" #-}
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

{-# DEPRECATED lookupShardReader "Use lookupResource instead" #-}
lookupShardReader :: ServerContext -> LookupShardReaderRequest -> IO LookupShardReaderResponse
lookupShardReader sc req@LookupShardReaderRequest{lookupShardReaderRequestReaderId=readerId} = do
  theNode <- lookupResource sc ResShardReader readerId
  Log.info $ "receive lookupShardReader request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardReaderResponse
    { lookupShardReaderResponseReaderId    = readerId
    , lookupShardReaderResponseServerNode  = Just theNode
    }

nodeChangeEventHandler :: MVar ServerContext -> Gossip.ServerState -> I.ServerNode -> IO ()
nodeChangeEventHandler scMVar Gossip.ServerDead I.ServerNode {..} = do
  Log.info $ "handle Server Dead event for server: " <> Log.buildString' serverNodeId
  withMVar scMVar $ \sc@ServerContext{..} -> do
    recoverDeadNodeTasks sc scIOWorker serverNodeId
    recoverDeadNodeTasks sc (QueryWorker sc) serverNodeId
nodeChangeEventHandler _ _ _ = return ()

-- getNodeResources :: Meta.MetaHandle -> ResourceType -> Types.ServerID -> IO [T.Text]
-- getNodeResources h rt nodeId = do
--   allocations <- Meta.getAllMeta @Meta.TaskAllocation h
--   let taskIds = map parseAllocationKey . Map.keys . Map.filter ((== nodeId) . Meta.taskAllocationServerId) $ allocations
--   return [tid | Right (rt', tid) <- taskIds, rt == rt']

recoverDeadNodeTasks :: Types.TaskManager a => ServerContext -> a -> Types.ServerID -> IO ()
recoverDeadNodeTasks sc tm deadNodeId = do
  tasks <- Types.listRecoverableResources tm
  recoverTasks sc tm tasks

-- only for restarting
recoverLocalTasks :: Types.TaskManager a => ServerContext -> a -> IO ()
recoverLocalTasks sc@ServerContext{..} tm = do
  tasks <- Types.listResources tm
  recoverTasks sc tm tasks

recoverTasks ::  Types.TaskManager a => ServerContext -> a -> [T.Text] -> IO ()
recoverTasks sc@ServerContext{..} tm tasks =
  forM_ tasks $ \task -> do
    taskNode <- lookupResource sc (Types.resourceType tm) task
    when (serverID == serverNodeId taskNode) $ do
      Log.info $ "recover " <> Log.build (show $ Types.resourceType tm) <> " task: " <> Log.build task
      catches (Types.recoverTask tm task) [
          Handler (\(err :: HE.QueryAlreadyTerminated) -> return ())
        , Handler (\(err :: SomeException) ->
            Log.fatal $ "Failed to recover dead node task" <> Log.buildString' (Types.resourceType tm)
                     <> " with name" <> Log.build task
                     <> ", error: " <> Log.build (displayException err)
          )
        ]
