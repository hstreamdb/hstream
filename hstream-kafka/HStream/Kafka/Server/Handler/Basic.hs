module HStream.Kafka.Server.Handler.Basic
  ( -- 18: ApiVersions
    handleApiversionsV0
  , handleApiversionsV1
  , handleApiversionsV2
  , handleApiversionsV3
    -- 3: Metadata
  , handleMetadataV0
  , handleMetadataV1
  ) where

import           Control.Exception
import           Control.Monad
import           Data.Functor                 ((<&>))
import           Data.Int                     (Int32)
import           Data.Text                    (Text)
import qualified Data.Text                    as Text
import qualified Data.Vector                  as V

import           HStream.Common.Server.Lookup (KafkaResource (..),
                                               lookupKafkaPersist)
import qualified HStream.Gossip               as Gossip
import           HStream.Kafka.Server.Types   (ServerContext (..))
import qualified HStream.Logger               as Log
import qualified HStream.Server.HStreamApi    as A
import qualified HStream.Store                as S
import qualified HStream.Utils                as Utils
import qualified Kafka.Protocol.Encoding      as K
import qualified Kafka.Protocol.Error         as K
import qualified Kafka.Protocol.Message       as K
import qualified Kafka.Protocol.Service       as K

--------------------
-- 18: ApiVersions
--------------------
handleApiversionsV0
  :: K.RequestContext -> K.ApiVersionsRequestV0 -> IO K.ApiVersionsResponseV0
handleApiversionsV0 _ _ = do
  let apiKeys = K.KaArray $ Just $ V.fromList K.supportedApiVersions
  pure $ K.ApiVersionsResponseV0 K.NONE apiKeys

handleApiversionsV1
  :: K.RequestContext -> K.ApiVersionsRequestV1 -> IO K.ApiVersionsResponseV1
handleApiversionsV1 _ _ = do
  let apiKeys = K.KaArray $ Just $ V.fromList K.supportedApiVersions
  pure $ K.ApiVersionsResponseV1 K.NONE apiKeys 0{- throttle_time_ms -}

handleApiversionsV2
  :: K.RequestContext -> K.ApiVersionsRequestV2 -> IO K.ApiVersionsResponseV2
handleApiversionsV2 = handleApiversionsV1

handleApiversionsV3
  :: K.RequestContext -> K.ApiVersionsRequestV3 -> IO K.ApiVersionsResponseV3
handleApiversionsV3 _ req = do
  let apiKeys = K.CompactKaArray
              . Just
              . (V.map apiVersionV0ToV3)
              . V.fromList
              $ K.supportedApiVersions
  pure $ K.ApiVersionsResponseV3 K.NONE apiKeys 0{- throttle_time_ms -}
                                 K.EmptyTaggedFields

--------------------
--  3: Metadata
--------------------
handleMetadataV0
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV0 -> IO K.MetadataResponseV0
handleMetadataV0 ctx reqCtx req = do
  (K.MetadataResponseV1 (K.KaArray brokers) _ (K.KaArray topics)) <- handleMetadataV1 ctx reqCtx req
  return $ K.MetadataResponseV0 (K.KaArray $ V.map respBrokerV1toV0 <$> brokers)
                                (K.KaArray $ V.map respTopicV1toV0  <$> topics)

  where
    respBrokerV1toV0 :: K.MetadataResponseBrokerV1 -> K.MetadataResponseBrokerV0
    respBrokerV1toV0 K.MetadataResponseBrokerV1{..} =
      K.MetadataResponseBrokerV0 nodeId host port

    respTopicV1toV0 :: K.MetadataResponseTopicV1 -> K.MetadataResponseTopicV0
    respTopicV1toV0 K.MetadataResponseTopicV1{..} =
      K.MetadataResponseTopicV0 errorCode name partitions

handleMetadataV1
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV1 -> IO K.MetadataResponseV1
handleMetadataV1 ctx@ServerContext{..} _ req = do
  respBrokers <- getBrokers
  -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
  -- causing a potential overflow.
  let ctlId = fromIntegral serverID
  let (K.MetadataRequestV0 reqTopics) = req
  case reqTopics of
    K.KaArray Nothing -> returnAllTopics respBrokers ctlId
    K.KaArray (Just v)
      | V.null v  -> returnAllTopics respBrokers ctlId
      | otherwise -> do
          let topicNames = V.map (\K.MetadataRequestTopicV0{..} -> name) v
          respTopics <- forM topicNames getRespTopic
          return $ K.MetadataResponseV1 (K.KaArray $ Just respBrokers) ctlId (K.KaArray $ Just respTopics)
  where
    returnAllTopics :: V.Vector K.MetadataResponseBrokerV1
                    -> Int32
                    -> IO K.MetadataResponseV1
    returnAllTopics respBrokers_ ctlId_ = do
      -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
      -- causing a potential overflow.
      allStreamNames <- S.findStreams scLDClient S.StreamTypeStream <&> (fmap (Utils.cBytesToText . S.streamName))
      respTopics <- forM allStreamNames getRespTopic <&> V.fromList
      return $ K.MetadataResponseV1 (K.KaArray $ Just respBrokers_) ctlId_ (K.KaArray $ Just respTopics)

    getBrokers :: IO (V.Vector K.MetadataResponseBrokerV1)
    getBrokers = do
      (nodes, nodesStatus) <- Gossip.describeCluster gossipContext scAdvertisedListenersKey
      let brokers = V.map (\A.ServerNode{..} ->
                              K.MetadataResponseBrokerV1
                              { nodeId   = fromIntegral serverNodeId
                              , host     = serverNodeHost
                              , port     = fromIntegral serverNodePort
                              , rack     = Nothing
                              }
                          ) nodes
      return brokers

    getRespTopic :: Text -> IO K.MetadataResponseTopicV1
    getRespTopic topicName = do
      let streamId = S.transToTopicStreamName topicName
      shards_e <- try ((V.map snd) <$> S.listStreamPartitionsOrdered scLDClient streamId)
      case shards_e of
        -- FIXME: We catch all exceptions here. Is this proper?
        -- FIXME: Are the following error codes proper?
        -- FIXME: We passed `Nothing` as partitions when an error occurs. Is this proper?
        Left (_ :: SomeException) ->
          return $ K.MetadataResponseTopicV1 K.UNKNOWN_TOPIC_OR_PARTITION topicName False (K.KaArray Nothing)
        Right shards
          | V.null shards ->
              return $ K.MetadataResponseTopicV1 K.INVALID_TOPIC_EXCEPTION topicName False (K.KaArray Nothing)
          | V.length shards > fromIntegral (maxBound :: Int32) ->
              return $ K.MetadataResponseTopicV1 K.INVALID_PARTITIONS topicName False (K.KaArray Nothing)
          | otherwise -> do
              respPartitions <-
                V.iforM shards $ \idx shardId -> do
                  theNode <- lookupKafkaPersist metaHandle gossipContext
                               loadBalanceHashRing scAdvertisedListenersKey
                               (KafkaResTopic $ Text.pack $ show shardId)
                  -- FIXME: Convert from `Word32` to `Int32`, possible overflow!
                  when ((A.serverNodeId theNode) > fromIntegral (maxBound :: Int32)) $
                    Log.warning $ "ServerID " <> Log.build (A.serverNodeId theNode)
                               <> " is too large, it should be less than "
                               <> Log.build (maxBound :: Int32)
                  let (theNodeId :: Int32) = fromIntegral (A.serverNodeId theNode)
                  pure $ K.MetadataResponsePartitionV0
                           { errorCode      = K.NONE
                           , partitionIndex = (fromIntegral idx)
                           , leaderId       = theNodeId
                           , replicaNodes   = K.KaArray $ Just (V.singleton theNodeId) -- FIXME: what should it be?
                           , isrNodes       = K.KaArray $ Just (V.singleton theNodeId) -- FIXME: what should it be?
                           }
              return $
                K.MetadataResponseTopicV1 K.NONE topicName False (K.KaArray $ Just respPartitions)

-------------------------------------------------------------------------------

apiVersionV0ToV3 :: K.ApiVersionV0 -> K.ApiVersionV3
apiVersionV0ToV3 K.ApiVersionV0{..} =
  let taggedFields = K.EmptyTaggedFields in K.ApiVersionV3{..}
