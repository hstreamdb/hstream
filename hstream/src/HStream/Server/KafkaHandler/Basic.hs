module HStream.Server.KafkaHandler.Basic
  ( -- 18: ApiVersions
    handleApiversionsV0
  , handleApiversionsV1
  , handleApiversionsV2
    -- 3: Metadata
  , handleMetadataV0
  , handleMetadataV1
  ) where

import           Control.Concurrent.STM        (readTVarIO)
import           Control.Exception
import           Control.Monad
import           Data.Functor                  ((<&>))
import           Data.Int                      (Int32)
import qualified Data.List                     as L
import qualified Data.Map.Strict               as M
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import qualified Data.Vector                   as V

import qualified HStream.Server.Core.Cluster   as Core
import qualified HStream.Server.Core.Common    as Core
import qualified HStream.Server.HStreamApi     as GRPC
import           HStream.Server.Types          (ServerContext (..),
                                                transToStreamName)
import qualified HStream.Store                 as S
import qualified HStream.Utils                 as Utils

import qualified HStream.Logger                as Log
import           HStream.Server.HStreamApi     (ServerNode (serverNodeId))
import           HStream.Store                 (StreamId (streamName))
import qualified Kafka.Protocol.Error          as K
import qualified Kafka.Protocol.Message.Struct as K
import qualified Kafka.Protocol.Service        as K

--------------------
-- 18: ApiVersions
--------------------
handleApiversionsV0
  :: K.RequestContext -> K.ApiVersionsRequestV0 -> IO K.ApiVersionsResponseV0
handleApiversionsV0 _ _ = do
  let apiKeys = Just $ V.fromList K.supportedApiVersions
  pure $ K.ApiVersionsResponseV0 K.NONE apiKeys

handleApiversionsV1
  :: K.RequestContext -> K.ApiVersionsRequestV1 -> IO K.ApiVersionsResponseV1
handleApiversionsV1 _ _ = do
  let apiKeys = Just $ V.fromList K.supportedApiVersions
  pure $ K.ApiVersionsResponseV1 K.NONE apiKeys 0{- throttle_time_ms -}

handleApiversionsV2
  :: K.RequestContext -> K.ApiVersionsRequestV2 -> IO K.ApiVersionsResponseV2
handleApiversionsV2 = handleApiversionsV1

--------------------
--  3: Metadata
--------------------
handleMetadataV0
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV0 -> IO K.MetadataResponseV0
handleMetadataV0 ctx reqCtx req = do
  (K.MetadataResponseV1 brokers _ topics) <- handleMetadataV1 ctx reqCtx req
  return $ K.MetadataResponseV0 (V.map respBrokerV1toV0 <$> brokers)
                                (V.map respTopicV1toV0  <$> topics)

  where
    respBrokerV1toV0 :: K.MetadataResponseBrokerV1 -> K.MetadataResponseBrokerV0
    respBrokerV1toV0 K.MetadataResponseBrokerV1{..} =
      K.MetadataResponseBrokerV0 nodeId host port

    respTopicV1toV0 :: K.MetadataResponseTopicV1 -> K.MetadataResponseTopicV0
    respTopicV1toV0 K.MetadataResponseTopicV1{..} =
      K.MetadataResponseTopicV0 errorCode name partitions

handleMetadataV1
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV1 -> IO K.MetadataResponseV1
handleMetadataV1 ctx _ req = do
  respBrokers <- getBrokers
  -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
  --        causing a potential overflow.
  when ((serverID ctx) > fromIntegral (maxBound :: Int32)) $
    Log.warning $ "ServerID " <> Log.build (serverID ctx) <>
    " is too large, it should be less than " <> Log.build (maxBound :: Int32)
  let ctlId = fromIntegral (serverID ctx)
  let (K.MetadataRequestV0 reqTopics) = req
  case reqTopics of
    Nothing -> do
      allStreamNames <- S.findStreams (scLDClient ctx) S.StreamTypeStream <&> (fmap (Utils.cBytesToText . streamName))
      respTopics <- forM allStreamNames getRespTopic <&> V.fromList
      return $ K.MetadataResponseV1 (Just respBrokers) ctlId (Just respTopics)
    Just v  -> do
      let topicNames = V.map (\K.MetadataRequestTopicV0{..} -> name) v
      respTopics <- forM topicNames getRespTopic
      return $ K.MetadataResponseV1 (Just respBrokers) ctlId (Just respTopics)
  where
    getBrokers :: IO (V.Vector K.MetadataResponseBrokerV1)
    getBrokers = do
      GRPC.DescribeClusterResponse{..} <- Core.describeCluster ctx
      let brokers = V.map (\GRPC.ServerNode{..} ->
                              K.MetadataResponseBrokerV1
                              { nodeId   = fromIntegral serverNodeId
                              , host     = serverNodeHost
                              , port     = 9092 -- FIXME: hardcoded port
                              , rack     = Nothing
                              }
                          ) describeClusterResponseServerNodes
      return brokers

    getRespTopic :: Text -> IO K.MetadataResponseTopicV1
    getRespTopic topicName = do
      let streamId = transToStreamName topicName
      shards_e <- try (M.elems <$> S.listStreamPartitions (scLDClient ctx) streamId)
      case shards_e of
        -- FIXME: We catch all exceptions here. Is this proper?
        -- FIXME: Are the following error codes proper?
        -- FIXME: We passed `Nothing` as partitions when an error occurs. Is this proper?
        Left (_ :: SomeException) ->
          return $ K.MetadataResponseTopicV1 K.UNKNOWN_TOPIC_OR_PARTITION topicName False Nothing
        Right shards
          | L.null shards ->
            return $ K.MetadataResponseTopicV1 K.INVALID_TOPIC_EXCEPTION topicName False Nothing
          | length shards > fromIntegral (maxBound :: Int32) ->
            return $ K.MetadataResponseTopicV1 K.INVALID_PARTITIONS topicName False Nothing
          | otherwise -> do
              let sortedShards = L.sort shards `zip` [0..]
              respPartitions <-
                    mapM (\(shardId, i) -> do
                             -- Note: we choose the leader using the same method as `lookupShard` of the old GRPC server.
                             (_, hashRing) <- readTVarIO (loadBalanceHashRing ctx)
                             theNode <- Core.getResNode hashRing (T.pack $ show shardId) (scAdvertisedListenersKey ctx)
                             -- FIXME: Convert from `Word32` to `Int32`, possible overflow!
                             when ((serverNodeId theNode) > fromIntegral (maxBound :: Int32)) $
                               Log.warning $ "ServerID " <> Log.build (serverNodeId theNode) <>
                               " is too large, it should be less than " <> Log.build (maxBound :: Int32)
                             let (theNodeId :: Int32) = fromIntegral (serverNodeId theNode)
                             return K.MetadataResponsePartitionV0
                                       { errorCode      = K.NONE
                                       , partitionIndex = i
                                       , leaderId       = theNodeId
                                       , replicaNodes   = Just (V.singleton theNodeId) -- FIXME: fit replicate factor
                                       , isrNodes       = Just (V.singleton theNodeId) -- FIXME: what should this value be?
                                       }
                          ) sortedShards
              return $
                K.MetadataResponseTopicV1 K.NONE topicName False (Just $ V.fromList respPartitions)
