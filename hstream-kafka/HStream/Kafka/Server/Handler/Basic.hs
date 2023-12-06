{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

module HStream.Kafka.Server.Handler.Basic
  ( -- 18: ApiVersions
    handleApiVersions
    -- 3: Metadata
  , handleMetadataV0
  , handleMetadataV1
  , handleMetadataV2
  , handleMetadataV3
  , handleMetadataV4

    --
  , handleDescribeConfigs
  ) where

import           Control.Exception
import           Control.Monad
import qualified Data.Foldable                                  as FD
import           Data.Functor                                   ((<&>))
import           Data.Int                                       (Int32)
import qualified Data.List                                      as L
import qualified Data.Map                                       as Map
import qualified Data.Set                                       as S
import           Data.Text                                      (Text)
import qualified Data.Text                                      as Text
import qualified Data.Vector                                    as V

import qualified Data.Text                                      as T
import           HStream.Common.Server.Lookup                   (KafkaResource (..),
                                                                 lookupKafkaPersist)
import qualified HStream.Gossip                                 as Gossip
import qualified HStream.Kafka.Common.Utils                     as K
import qualified HStream.Kafka.Common.Utils                     as Utils
import qualified HStream.Kafka.Server.Config.KafkaConfig        as KC
import qualified HStream.Kafka.Server.Config.KafkaConfigManager as KCM
import           HStream.Kafka.Server.Core.Topic                (createTopic)
import           HStream.Kafka.Server.Types                     (ServerContext (..))
import qualified HStream.Logger                                 as Log
import qualified HStream.Server.HStreamApi                      as A
import qualified HStream.Store                                  as S
import qualified HStream.Utils                                  as Utils
import qualified Kafka.Protocol.Encoding                        as K
import qualified Kafka.Protocol.Error                           as K
import qualified Kafka.Protocol.Message                         as K
import qualified Kafka.Protocol.Service                         as K

--------------------
-- 18: ApiVersions
--------------------

handleApiVersions
  :: ServerContext
  -> K.RequestContext
  -> K.ApiVersionsRequest
  -> IO K.ApiVersionsResponse
handleApiVersions _ _ _ = do
  let apiKeys = K.KaArray
              . Just
              . (V.map apiVersionV0To)
              . V.fromList
              $ K.supportedApiVersions
  pure $ K.ApiVersionsResponse K.NONE apiKeys 0{- throttle_time_ms -}
                               K.EmptyTaggedFields

--------------------
--  3: Metadata
--------------------
handleMetadataV0
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV0 -> IO K.MetadataResponseV0
handleMetadataV0 ctx reqCtx req = do
  -- In version 0, an empty array indicates "request metadata for all topics."  In version 1 and
  -- higher, an empty array indicates "request metadata for no topics," and a null array is used to
  -- indiate "request metadata for all topics."
  let K.NonNullKaArray topicVec = req.topics
      v1Topics = if (V.null topicVec) then K.KaArray Nothing else K.NonNullKaArray topicVec
      v1Req = K.MetadataRequestV0 {topics=v1Topics}
  (K.MetadataResponseV1 (K.KaArray brokers) _ (K.KaArray topics)) <- handleMetadataV1 ctx reqCtx v1Req
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
handleMetadataV1 ctx reqCtx req = do
  respV3 <- handleMetadataV3 ctx reqCtx req
  return $ K.MetadataResponseV1 {
    controllerId=respV3.controllerId
    , topics=respV3.topics
    , brokers=respV3.brokers}

handleMetadataV2
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV2 -> IO K.MetadataResponseV2
handleMetadataV2 ctx reqCtx req = do
  respV3 <- handleMetadataV3 ctx reqCtx req
  return $ K.MetadataResponseV2 {
    controllerId=respV3.controllerId
    , clusterId=respV3.clusterId
    , topics=respV3.topics
    , brokers=respV3.brokers}

handleMetadataV3
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV3 -> IO K.MetadataResponseV3
handleMetadataV3 ctx reqCtx req = do
  let reqV4 = K.MetadataRequestV4 {allowAutoTopicCreation=False, topics=req.topics}
  handleMetadataV4 ctx reqCtx reqV4

handleMetadataV4
  :: ServerContext -> K.RequestContext -> K.MetadataRequestV4 -> IO K.MetadataResponseV4
handleMetadataV4 ctx@ServerContext{..} _ req@K.MetadataRequestV4{..} = do
  respBrokers <- getBrokers
  -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
  -- causing a potential overflow.
  let ctlId = fromIntegral serverID
  let reqTopics = req.topics
  case reqTopics of
    K.KaArray Nothing -> returnAllTopics respBrokers ctlId
    K.KaArray (Just v)
      | V.null v  -> return $ K.MetadataResponseV3 {
          throttleTimeMs=0
          , clusterId=Nothing
          , controllerId=ctlId
          , topics=K.NonNullKaArray V.empty
          , brokers=K.NonNullKaArray respBrokers
          }
      | otherwise -> do
          let topicNames = S.fromList . V.toList $ V.map (\K.MetadataRequestTopicV0{..} -> name) v
          allStreamNames <- S.findStreams scLDClient S.StreamTypeTopic <&> S.fromList . L.map (Utils.cBytesToText . S.streamName)
          let needCreate = S.toList $ topicNames S.\\ allStreamNames
          let alreadyExist = V.fromList . S.toList $ topicNames `S.intersection` allStreamNames

          createResp <-
            if kafkaBrokerConfigs.autoCreateTopicsEnable._value && allowAutoTopicCreation
              then do
                let defaultReplicas = kafkaBrokerConfigs.defaultReplicationFactor._value
                    defaultNumPartitions = kafkaBrokerConfigs.numPartitions._value
                resp <- forM needCreate $ \topic -> do
                  (code, shards) <- createTopic ctx topic (fromIntegral defaultReplicas) (fromIntegral defaultNumPartitions) Map.empty
                  if code /= K.NONE
                    then do
                      return $ K.MetadataResponseTopicV1 code topic False K.emptyKaArray
                    else mkResponse topic (V.fromList shards)
                return $ V.fromList resp
              else do
                let f topic acc = K.MetadataResponseTopicV1 K.UNKNOWN_TOPIC_OR_PARTITION topic False K.emptyKaArray : acc
                return . V.fromList $ FD.foldr' f [] needCreate
          unless (V.null createResp) $ Log.info $ "auto create topic response: " <> Log.build (show createResp)

          respTopics <- forM alreadyExist getRespTopic
          let respTopics' = respTopics <> createResp
          -- return $ K.MetadataResponseV4 (K.KaArray $ Just respBrokers) ctlId (K.KaArray $ Just respTopics)
          -- TODO: implement read cluster id
          return $ K.MetadataResponseV3 {
              clusterId=Nothing
            , controllerId=ctlId
            , throttleTimeMs=0
            , topics=K.NonNullKaArray respTopics'
            , brokers=K.NonNullKaArray respBrokers
            }
  where
    returnAllTopics :: V.Vector K.MetadataResponseBrokerV1
                    -> Int32
                    -> IO K.MetadataResponseV3
    returnAllTopics respBrokers_ ctlId_ = do
      -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
      -- causing a potential overflow.
      allStreamNames <- S.findStreams scLDClient S.StreamTypeTopic <&> (fmap (Utils.cBytesToText . S.streamName))
      respTopics <- forM allStreamNames getRespTopic <&> V.fromList
      -- return $ K.MetadataResponseV1 (K.KaArray $ Just respBrokers_) ctlId_ (K.KaArray $ Just respTopics)
      return $ K.MetadataResponseV3 {
          clusterId=Nothing
        , controllerId=ctlId_
        , throttleTimeMs=0
        , topics=K.NonNullKaArray respTopics
        , brokers=K.NonNullKaArray respBrokers_
        }

    getBrokers :: IO (V.Vector K.MetadataResponseBrokerV1)
    getBrokers = do
      (nodes, _) <- Gossip.describeCluster gossipContext scAdvertisedListenersKey
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
        -- FIXME: Are the following error codes proper?
        -- FIXME: We passed `Nothing` as partitions when an error occurs. Is this proper?
        Left (e :: SomeException)
          | Just (_ :: S.NOTFOUND) <- fromException e ->
              return $ K.MetadataResponseTopicV1 K.UNKNOWN_TOPIC_OR_PARTITION topicName False K.emptyKaArray
          | otherwise ->
              return $ K.MetadataResponseTopicV1 K.UNKNOWN_SERVER_ERROR topicName False K.emptyKaArray
        Right shards
          | V.null shards ->
              return $ K.MetadataResponseTopicV1 K.INVALID_TOPIC_EXCEPTION topicName False K.emptyKaArray
          | V.length shards > fromIntegral (maxBound :: Int32) ->
              return $ K.MetadataResponseTopicV1 K.INVALID_PARTITIONS topicName False K.emptyKaArray
          | otherwise -> mkResponse topicName shards

    mkResponse topicName shards = do
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

apiVersionV0To :: K.ApiVersionV0 -> K.ApiVersion
apiVersionV0To K.ApiVersionV0{..} =
  let taggedFields = K.EmptyTaggedFields in K.ApiVersion{..}

---------------------------------------------------------------------------
--  32: DescribeConfigs
---------------------------------------------------------------------------
handleDescribeConfigs
  :: ServerContext
  -> K.RequestContext
  -> K.DescribeConfigsRequest
  -> IO K.DescribeConfigsResponse
handleDescribeConfigs serverCtx _ req = do
  manager <- KCM.mkKafkaConfigManager serverCtx.scLDClient serverCtx.kafkaBrokerConfigs
  results <- V.forM (Utils.kaArrayToVector req.resources) $ \resource -> do
    case toEnum (fromIntegral resource.resourceType) of
      KC.TOPIC -> KCM.listTopicConfigs manager resource.resourceName resource.configurationKeys
      KC.BROKER -> do
        if T.pack (show serverCtx.serverID) == resource.resourceName
        then KCM.listBrokerConfigs manager resource.resourceName resource.configurationKeys
        else return $ KCM.getErrorResponse KC.BROKER resource.resourceName ("invalid broker id:" <> resource.resourceName)
      rt -> return $ KCM.getErrorResponse rt resource.resourceName ("unsupported resource type:" <> T.pack (show rt))
  return $ K.DescribeConfigsResponse {results=K.NonNullKaArray results, throttleTimeMs=0}

