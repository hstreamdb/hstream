{-# LANGUAGE PatternSynonyms #-}

module HStream.Server.Core.Cluster
  ( describeCluster
  , lookupResource

  , lookupShard
  , lookupSubscription
  , lookupShardReader

  , nodeChangeEventHandler
  , recoverLocalTasks
  ) where

import           Control.Concurrent             (MVar, modifyMVar_, tryReadMVar,
                                                 withMVar)
import           Control.Concurrent.STM         (atomically, readTVarIO, retry)
import           Control.Exception              (Handler (..),
                                                 SomeException (..), catches,
                                                 throwIO)
import           Control.Monad                  (forM_, unless, when)
import qualified Data.List                      as L
import qualified Data.Map.Strict                as Map
import qualified Data.Text                      as T
import qualified Data.Vector                    as V
import           Proto3.Suite                   (Enumerated (..))

import           HStream.Common.Types           (fromInternalServerNodeWithKey)
import qualified HStream.Exception              as HE
import           HStream.Gossip                 (GossipContext (..),
                                                 getFailedNodes,
                                                 getFailedNodesSTM,
                                                 getMemberList)
import           HStream.Gossip.Types           (ServerStatus (..))
import qualified HStream.Gossip.Types           as Gossip
import qualified HStream.Logger                 as Log
import           HStream.MetaStore.Types        (MetaStore (..))
import qualified HStream.MetaStore.Types        as Meta
import           HStream.Server.Core.Common     (getResNode, lookupResource',
                                                 parseAllocationKey)
import           HStream.Server.HStreamApi
import qualified HStream.Server.HStreamApi      as API
import qualified HStream.Server.HStreamInternal as I
import qualified HStream.Server.MetaData        as Meta
import           HStream.Server.MetaData.Value  (clusterStartTimeId)
import           HStream.Server.QueryWorker     (QueryWorker (QueryWorker))
import           HStream.Server.Types           (ServerContext (..))
import qualified HStream.Server.Types           as Types
import qualified HStream.ThirdParty.Protobuf    as Proto
import           HStream.Utils                  (ResourceType (..),
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
lookupShardReader ServerContext{..} req@LookupShardReaderRequest{lookupShardReaderRequestReaderId=readerId} = do
  (_, hashRing) <- readTVarIO loadBalanceHashRing
  theNode <- getResNode hashRing readerId scAdvertisedListenersKey
  Log.info $ "receive lookupShardReader request: " <> Log.buildString' req <> ", should send to " <> Log.buildString' (show theNode)
  return $ LookupShardReaderResponse
    { lookupShardReaderResponseReaderId    = readerId
    , lookupShardReaderResponseServerNode  = Just theNode
    }

nodeChangeEventHandler :: MVar ServerContext -> Gossip.ServerState -> I.ServerNode -> IO ()
nodeChangeEventHandler scMVar Gossip.ServerDead I.ServerNode {..} = do
  Log.info $ "handle Server Dead event: " <> Log.buildString' serverNodeId
  withMVar scMVar $ \sc@ServerContext{..} -> do
    recoverDeadNodeTasks sc scIOWorker serverNodeId
    recoverDeadNodeTasks sc (QueryWorker sc) serverNodeId
nodeChangeEventHandler _ _ _ = return ()

getNodeResources :: Meta.MetaHandle -> ResourceType -> Types.ServerID -> IO [T.Text]
getNodeResources h rt nodeId = do
  allocations <- Meta.getAllMeta @Meta.TaskAllocation h
  let taskIds = map parseAllocationKey . Map.keys . Map.filter ((== nodeId) . Meta.taskAllocationServerId) $ allocations
  return [tid | Right (rt', tid) <- taskIds, rt == rt']

recoverDeadNodeTasks :: Types.TaskManager a => ServerContext -> a -> Types.ServerID -> IO ()
recoverDeadNodeTasks sc tm deadNodeId = do
  tasks <- getNodeResources (metaHandle sc) (Types.resourceType tm) deadNodeId
  recoverTasks sc tm tasks

-- only for restarting
recoverLocalTasks :: Types.TaskManager a => ServerContext -> a -> IO ()
recoverLocalTasks sc@ServerContext{..} tm = do
  tasks <- Types.listResources tm
  recoverTasks sc tm tasks

recoverTasks ::  Types.TaskManager a => ServerContext -> a -> [T.Text] -> IO ()
recoverTasks sc@ServerContext{..} tm tasks =
  forM_ tasks $ \task -> do
    taskNode <- lookupResource' sc (Types.resourceType tm) task
    when (serverID == API.serverNodeId taskNode) $
      catches (Types.recoverTask tm task) [
          Handler (\(err :: HE.QueryAlreadyTerminated) -> return ())
        , Handler (\(err :: SomeException) ->
            Log.warning $ "Failed to recover dead node task" <> Log.buildString' (Types.resourceType tm)
                        <> " with name" <> Log.build task
          )
        ]
