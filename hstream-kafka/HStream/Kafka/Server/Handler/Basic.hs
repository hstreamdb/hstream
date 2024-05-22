{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Server.Handler.Basic
  ( -- 18: ApiVersions
    handleApiVersions
    -- 3: Metadata
  , handleMetadata
    -- 32: DescribeConfigs
  , handleDescribeConfigs

  , handleFindCoordinator
  ) where

import           Control.Exception
import           Control.Monad
import           Control.Monad.Extra                            ((&&^))
import qualified Data.Foldable                                  as FD
import           Data.Functor                                   ((<&>))
import           Data.Int                                       (Int32)
import qualified Data.List                                      as L
import qualified Data.Map                                       as Map
import           Data.Maybe                                     (fromJust)
import qualified Data.Set                                       as Set
import           Data.Text                                      (Text)
import qualified Data.Text                                      as Text
import qualified Data.Vector                                    as V

import qualified Data.Text                                      as T
import           HStream.Common.Server.Lookup                   (KafkaResource (..),
                                                                 lookupKafkaPersist)
import qualified HStream.Gossip                                 as Gossip
import qualified HStream.Kafka.Common.Acl                       as K
import qualified HStream.Kafka.Common.Authorizer.Class          as K
import qualified HStream.Kafka.Common.Resource                  as K
import qualified HStream.Kafka.Common.Utils                     as K
import qualified HStream.Kafka.Common.Utils                     as Utils
import qualified HStream.Kafka.Server.Config.KafkaConfig        as KC
import qualified HStream.Kafka.Server.Config.KafkaConfigManager as KCM
import           HStream.Kafka.Server.Core.Topic                (createTopic)
import qualified HStream.Kafka.Server.Handler.Topic             as K
import           HStream.Kafka.Server.Types                     (ServerContext (..))
import qualified HStream.Logger                                 as Log
import qualified HStream.Server.HStreamApi                      as A
import qualified HStream.Utils                                  as Utils
import qualified Kafka.Protocol.Encoding                        as K
import qualified Kafka.Protocol.Error                           as K
import qualified Kafka.Protocol.Message                         as K
import qualified Kafka.Protocol.Service                         as K
import qualified Kafka.Storage                                  as S

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

apiVersionV0To :: K.ApiVersionV0 -> K.ApiVersion
apiVersionV0To K.ApiVersionV0{..} =
  let taggedFields = K.EmptyTaggedFields in K.ApiVersion{..}

--------------------
--  3: Metadata
--------------------
handleMetadata
  :: ServerContext -> K.RequestContext
  -> K.MetadataRequest -> IO K.MetadataResponse
handleMetadata ctx reqCtx req = do
  -- In version 0,
  -- an empty array indicates "request metadata for all topics."
  -- a null array will cause an error.
  --
  -- In version 1 and higher,
  -- an empty array indicates "request metadata for no topics,", and
  -- a null array is used to indiate "request metadata for all topics."
  let reqTopics = if reqCtx.apiVersion >= 1
                     then req.topics
                     else let K.NonNullKaArray topicVec = req.topics
                           in if V.null topicVec then K.KaArray Nothing
                                                 else K.NonNullKaArray topicVec
      -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
      -- causing a potential overflow.
      ctlId = fromIntegral ctx.serverID
  respBrokers <- getBrokers
  case reqTopics of
    K.KaArray Nothing -> returnAllTopics respBrokers ctlId
    K.KaArray (Just v)
      | V.null v  -> return $ K.MetadataResponse
          { throttleTimeMs = 0
          , clusterId = Nothing
          , controllerId = ctlId
          , topics = K.NonNullKaArray V.empty
          , brokers = K.NonNullKaArray respBrokers
          }
      | otherwise -> do
          -- Note: authorize **DESCRIBE** for existed topics;
          --       authorize **DESCRIBE** and **CREATE** for
          --       unexisted topics.
          let topicNames = Set.fromList . V.toList $
                V.map (\K.MetadataRequestTopic{..} -> name) v
          allStreamNames <- S.findStreams ctx.scLDClient S.StreamTypeTopic <&> Set.fromList . L.map (Utils.cBytesToText . S.streamName)
          let needCreate = Set.toList $ topicNames Set.\\ allStreamNames
              alreadyExist = V.fromList . Set.toList $ topicNames `Set.intersection` allStreamNames
              kafkaBrokerConfigs = ctx.kafkaBrokerConfigs

          createResp <-
            if kafkaBrokerConfigs.autoCreateTopicsEnable._value && req.allowAutoTopicCreation
              then do
                let defaultReplicas = kafkaBrokerConfigs.defaultReplicationFactor._value
                    defaultNumPartitions = kafkaBrokerConfigs.numPartitions._value
                resp <- forM needCreate $ \topic -> do
                  ((K.simpleAuthorize (K.toAuthorizableReqCtx reqCtx) ctx.authorizer K.Res_TOPIC topic K.AclOp_DESCRIBE) &&^
                   (K.simpleAuthorize (K.toAuthorizableReqCtx reqCtx) ctx.authorizer K.Res_TOPIC topic K.AclOp_CREATE)) >>= \case
                    False -> return $ makeErrorTopicResp topic K.TOPIC_AUTHORIZATION_FAILED
                    True  -> case K.validateTopicName topic of
                      Left (code, msg) -> do
                        Log.warning $ "Auto create topic " <> Log.build topic <> " failed, error: " <> Log.build (show msg)
                        return $ makeErrorTopicResp topic code
                      Right _ -> do
                        ((code, _), shards) <- createTopic ctx topic (fromIntegral defaultReplicas) (fromIntegral defaultNumPartitions) Map.empty
                        if code /= K.NONE
                          then return $ makeErrorTopicResp topic code
                          else mkResponse topic (V.fromList shards)
                return $ V.fromList resp
              else do
                let f topic acc = makeErrorTopicResp topic K.UNKNOWN_TOPIC_OR_PARTITION : acc
                return . V.fromList $ FD.foldr' f [] needCreate
          unless (V.null createResp) $ Log.info $ "auto create topic response: " <> Log.build (show createResp)

          -- Note: We checked [DESCRIBE TOPIC] in 'getRespTopic'
          respTopics <- forM alreadyExist getRespTopic
          let respTopics' = respTopics <> createResp
          -- return $ K.MetadataResponseV4 (K.KaArray $ Just respBrokers) ctlId (K.KaArray $ Just respTopics)
          -- TODO: implement read cluster id
          return $ K.MetadataResponse
            { clusterId = Nothing
            , controllerId = ctlId
            , throttleTimeMs = 0
            , topics = K.NonNullKaArray respTopics'
            , brokers = K.NonNullKaArray respBrokers
            }
  where
    returnAllTopics :: V.Vector K.MetadataResponseBroker
                    -> Int32
                    -> IO K.MetadataResponse
    returnAllTopics respBrokers_ ctlId_ = do
      -- FIXME: `serverID` is a `Word32` but kafka expects an `Int32`,
      -- causing a potential overflow.
      allStreamNames <- S.findStreams ctx.scLDClient S.StreamTypeTopic <&> (fmap (Utils.cBytesToText . S.streamName))
      respTopics <- forM allStreamNames getRespTopic <&> V.fromList
      -- [ACL] Note: no topic info should be disclosed when requesting all
      --             topics. So just **omit** unauthzed topics from the
      --             response rather than return an error.
      let authzedRespTopics =
            V.filter (\topic -> topic.errorCode /= K.TOPIC_AUTHORIZATION_FAILED
                     ) respTopics
      -- return $ K.MetadataResponseV1 (K.KaArray $ Just respBrokers_) ctlId_ (K.KaArray $ Just respTopics)
      return $ K.MetadataResponse
        { clusterId = Nothing
        , controllerId = ctlId_
        , throttleTimeMs = 0
        , topics = K.NonNullKaArray authzedRespTopics
        , brokers = K.NonNullKaArray respBrokers_
        }

    getBrokers :: IO (V.Vector K.MetadataResponseBroker)
    getBrokers = do
      (nodes, _) <- Gossip.describeCluster ctx.gossipContext ctx.scAdvertisedListenersKey
      let brokers = V.map (\A.ServerNode{..} ->
                              K.MetadataResponseBroker
                              { nodeId = fromIntegral serverNodeId
                              , host   = serverNodeHost
                              , port   = fromIntegral serverNodePort
                              , rack   = Nothing
                              }
                          ) nodes
      return brokers

    getRespTopic :: Text -> IO K.MetadataResponseTopic
    getRespTopic topicName = do
      let streamId = S.transToTopicStreamName topicName
      -- [ACL] authorize [DESCRIBE TOPIC]
      K.simpleAuthorize (K.toAuthorizableReqCtx reqCtx) ctx.authorizer K.Res_TOPIC topicName K.AclOp_DESCRIBE >>= \case
        False -> return $ makeErrorTopicResp topicName K.TOPIC_AUTHORIZATION_FAILED
        True  -> do
          -- FIXME: block is nested too deeply
          shards_e <- try ((V.map snd) <$> S.listStreamPartitionsOrderedByName ctx.scLDClient streamId)
          case shards_e of
            -- FIXME: Are the following error codes proper?
            -- FIXME: We passed `Nothing` as partitions when an error occurs. Is this proper?
            Left (e :: SomeException)
              | Just (_ :: S.NOTFOUND) <- fromException e ->
                  return $ makeErrorTopicResp topicName K.UNKNOWN_TOPIC_OR_PARTITION
              | otherwise ->
                  return $ makeErrorTopicResp topicName K.UNKNOWN_SERVER_ERROR
            Right shards
              | V.null shards ->
                  return $ makeErrorTopicResp topicName K.INVALID_TOPIC_EXCEPTION
              | V.length shards > fromIntegral (maxBound :: Int32) ->
                  return $ makeErrorTopicResp topicName K.INVALID_PARTITIONS
              | otherwise -> mkResponse topicName shards

    mkResponse topicName shards = do
      respPartitions <-
        V.iforM shards $ \idx shardId -> do
          theNode <- lookupKafkaPersist ctx.metaHandle ctx.gossipContext
                       ctx.loadBalanceHashRing ctx.scAdvertisedListenersKey
                       (KafkaResTopic $ Text.pack $ show shardId)
          -- FIXME: Convert from `Word32` to `Int32`, possible overflow!
          when ((A.serverNodeId theNode) > fromIntegral (maxBound :: Int32)) $
            Log.warning $ "ServerID " <> Log.build (A.serverNodeId theNode)
                       <> " is too large, it should be less than "
                       <> Log.build (maxBound :: Int32)
          let (theNodeId :: Int32) = fromIntegral (A.serverNodeId theNode)
              streamId = S.transToTopicStreamName topicName
          -- The logReplicationFactor should not be Nothing, so we use fromJust here.
          repfac <- fromJust . S.attrValue . S.logReplicationFactor <$> S.getStreamLogAttrs ctx.scLDClient streamId
          pure $ K.MetadataResponsePartition
                   { errorCode       = K.NONE
                   , partitionIndex  = (fromIntegral idx)
                   , leaderId        = theNodeId
                   , replicaNodes    = K.NonNullKaArray $ (V.replicate repfac theNodeId) -- FIXME: what should it be?
                   , isrNodes        = K.KaArray $ Just (V.singleton theNodeId) -- FIXME: what should it be?
                   , offlineReplicas = K.KaArray $ Just V.empty -- TODO
                   }
      return $
        K.MetadataResponseTopic
          { errorCode = K.NONE
          , name = topicName
          , partitions = (K.KaArray $ Just respPartitions)
          , isInternal = False
          }

    makeErrorTopicResp :: Text -> K.ErrorCode -> K.MetadataResponseTopic
    makeErrorTopicResp topicName code = K.MetadataResponseTopic
             { errorCode = code
             , name = topicName
             , partitions = K.emptyKaArray
             , isInternal = False
             }

---------------------------------------------------------------------------
--  32: DescribeConfigs
---------------------------------------------------------------------------
-- FIXME: This function does not catch any Kafka ErrorCodeException.
--        Is this proper?
-- FIXME: Recheck if returned error codes and messages are proper.
--        See kafka.server.ConfigHelper#handleDescribeConfigsRequest
handleDescribeConfigs
  :: ServerContext
  -> K.RequestContext
  -> K.DescribeConfigsRequest
  -> IO K.DescribeConfigsResponse
handleDescribeConfigs serverCtx reqCtx req = do
  manager <- KCM.mkKafkaConfigManager serverCtx.scLDClient serverCtx.kafkaBrokerConfigs
  results <- V.forM (Utils.kaArrayToVector req.resources) $ \resource -> do
    case toEnum (fromIntegral resource.resourceType) of
      KC.TOPIC -> do
        -- [ACL] check [DESCRIBE_CONFIGS TOPIC]
        K.simpleAuthorize (K.toAuthorizableReqCtx reqCtx) serverCtx.authorizer K.Res_TOPIC resource.resourceName K.AclOp_DESCRIBE_CONFIGS >>= \case
          False -> return $ KCM.getErrorResponse KC.TOPIC
                                                 resource.resourceName
                                                 K.TOPIC_AUTHORIZATION_FAILED
                                                 "Topic authorization failed."
          True  ->
            KCM.listTopicConfigs manager resource.resourceName resource.configurationKeys
      KC.BROKER -> do
        -- FIXME: authorize [DESCRIBE_CONFIGS CLUSTER] first
        if T.pack (show serverCtx.serverID) == resource.resourceName
        then KCM.listBrokerConfigs manager resource.resourceName resource.configurationKeys
        else return $ KCM.getErrorResponse KC.BROKER
                                           resource.resourceName
                                           K.INVALID_REQUEST
                                           ("Unexpected broker id, expected " <> (T.pack (show serverCtx.serverID)) <> " but received " <> resource.resourceName)
      rt -> return $ KCM.getErrorResponse rt
                                          resource.resourceName
                                          K.INVALID_REQUEST
                                          ("Unexpected resource type " <> T.pack (show rt) <> " for resouce" <> resource.resourceName)

  return $ K.DescribeConfigsResponse {
    results        = K.NonNullKaArray results
  , throttleTimeMs = 0
  }

---------------------------------------------------------------------------
--  32: FindCoordinator
---------------------------------------------------------------------------
data CoordinatorType
  = GROUP
  | TRANSACTION
  deriving (Enum, Eq)

-- FIXME: This function does not catch any Kafka ErrorCodeException.
--        Is this proper?
handleFindCoordinator :: ServerContext -> K.RequestContext -> K.FindCoordinatorRequest -> IO K.FindCoordinatorResponse
handleFindCoordinator ServerContext{..} reqCtx req = do
  case toEnum (fromIntegral req.keyType) of
    GROUP -> do
      -- [ACL] check [DESCRIBE GROUP]
      K.simpleAuthorize (K.toAuthorizableReqCtx reqCtx) authorizer K.Res_GROUP req.key K.AclOp_DESCRIBE >>= \case
        True  -> do
          A.ServerNode{..} <- lookupKafkaPersist metaHandle gossipContext loadBalanceHashRing scAdvertisedListenersKey (KafkaResGroup req.key)
          Log.info $ "findCoordinator for group:" <> Log.buildString' req.key <> ", assign to node " <> Log.buildString' serverNodeId
          return $ K.FindCoordinatorResponse {
              errorMessage=Nothing
            , nodeId=fromIntegral serverNodeId
            , errorCode=0
            , throttleTimeMs=0
            , port=fromIntegral serverNodePort
            , host=serverNodeHost
            }
        -- Note: About kafka's error message, see org.apache.kafka.common.protocol.Errors
        False -> return (makeErrorResponse K.GROUP_AUTHORIZATION_FAILED "Group authorization failed.")
    _ -> do
      -- TODO: authz [DESCRIBE TRANSACTION_ID] when this is supported
      -- FIXME: Is the error code/message
      return $ makeErrorResponse K.COORDINATOR_NOT_AVAILABLE "KeyType Must be 0(GROUP)"
  where
    -- FIXME: hard-coded constants
    -- Note: Kafka returns `nodeId = -1`, `port = -1` and `host=""` on error.
    --       See kafka.server.KafkaApis#getCoordinator and
    --           org.apache.kafka.common.Node#NO_NODE
    makeErrorResponse :: K.ErrorCode -> Text -> K.FindCoordinatorResponse
    makeErrorResponse code errMsg = K.FindCoordinatorResponse {
        errorMessage   = Just errMsg
      , nodeId         = -1
      , errorCode      = code
      , throttleTimeMs = 0
      , port           = -1
      , host           = ""
      }
