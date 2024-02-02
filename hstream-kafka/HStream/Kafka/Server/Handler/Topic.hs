{-# LANGUAGE CPP                 #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ViewPatterns        #-}

module HStream.Kafka.Server.Handler.Topic
  ( -- 19: CreateTopics
    handleCreateTopics
    -- 20: DeleteTopics
  , handleDeleteTopics
    -- 37: CreatePartitions
  , handleCreatePartitions
  ) where

import           Control.Exception
import           Control.Monad
import qualified Data.List                          as L
import qualified Data.Map                           as Map
import           Data.Maybe                         (isNothing)
import qualified Data.Text                          as T
import qualified Data.Vector                        as V
import           HStream.Kafka.Common.OffsetManager (cleanOffsetCache)
import qualified HStream.Kafka.Common.Utils         as Utils
import           HStream.Kafka.Server.Core.Store    (listTopicPartitionsOrdered)
import           HStream.Kafka.Server.Core.Topic    (createPartitions)
import qualified HStream.Kafka.Server.Core.Topic    as Core
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import qualified HStream.Store                      as S
import           Kafka.Protocol                     (NullableString)
import qualified Kafka.Protocol.Encoding            as K
import           Kafka.Protocol.Error               (ErrorCode)
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K

--------------------
-- 19: CreateTopics
--------------------
-- FIXME: The `timeoutMs` field of request is omitted.
handleCreateTopics
  :: ServerContext -> K.RequestContext -> K.CreateTopicsRequest -> IO K.CreateTopicsResponse
handleCreateTopics ctx@ServerContext{scLDClient} _ K.CreateTopicsRequest{..} =
  case topics of
    K.KaArray Nothing ->
      -- FIXME: We return `[]` when topics is `Nothing`.
      --        Is this proper?
      return $ K.CreateTopicsResponse {topics = K.KaArray $ Just V.empty, throttleTimeMs = 0}
    K.KaArray (Just topics_)
      | V.null topics_ -> return $ K.CreateTopicsResponse {topics = K.KaArray $ Just V.empty, throttleTimeMs = 0}
      | otherwise      -> do
          let (errRes, topics') = V.partitionWith (\tp -> mapErr tp.name . validateTopic $ tp) topics_
          if | null topics' ->
                -- all topics validate failed, return directly
                return $ K.CreateTopicsResponse {topics = K.KaArray $ Just errRes, throttleTimeMs = 0}
             | validateOnly -> do
                res <- V.forM topics' $ \K.CreatableTopic{..} -> do
                  let streamId = S.transToTopicStreamName name
                  exist <- S.doesStreamExist scLDClient streamId
                  if exist
                    then do
                      Log.info $ "Topic " <> Log.build name <> " already exist."
                      return K.CreatableTopicResult
                         { errorMessage=Just $ "Topic " <> name <> " already exist."
                         , errorCode=K.TOPIC_ALREADY_EXISTS
                         , name=name
                         }
                    else return K.CreatableTopicResult {errorMessage=Nothing, errorCode=K.NONE, name=name}

                return $ K.CreateTopicsResponse {topics = K.KaArray . Just $ res <> errRes, throttleTimeMs = 0}
             | otherwise -> do
                respTopics <- forM topics' $ createTopic
                return $ K.CreateTopicsResponse {topics = K.KaArray . Just $ respTopics <> errRes, throttleTimeMs = 0}
  where
    mapErr name (Left (errorCode, msg)) = Left $ K.CreatableTopicResult name errorCode msg
    mapErr _ (Right tp) = Right tp

    createTopic :: K.CreatableTopic -> IO K.CreatableTopicResult
    createTopic K.CreatableTopic{..} = do
      let configMap = Map.fromList . map (\c -> (c.name, c.value)) . Utils.kaArrayToList $ configs
      ((errorCode, msg), _) <- Core.createTopic ctx name replicationFactor numPartitions configMap
      return $ K.CreatableTopicResult name errorCode (Just msg)

validateTopic :: K.CreatableTopic -> Either (ErrorCode, NullableString) K.CreatableTopic
validateTopic topic@K.CreatableTopic{..} = do
  validateNullConfig configs
  *> validateAssignments assignments
  *> validateReplica replicationFactor
  *> validateNumPartitions numPartitions
 where
   invalidReplicaMsg = Just . T.pack $ "Replication factor must be larger than 0, or -1 to use the default value."
   invalidNumPartitionsMsg = Just . T.pack $ "Number of partitions must be larger than 0, or -1 to use the default value."
   unsuportedPartitionAssignments = Just . T.pack $ "Partition assignments is not supported now."

   validateNullConfig (K.unKaArray -> Just configs') =
     let nullConfigs = V.filter (\K.CreateableTopicConfig{value} -> isNothing value) configs'
      in if V.null nullConfigs
           then Right topic
           else Left (K.INVALID_CONFIG, Just $ T.pack ("Null value not supported for topic configs: " <> show nullConfigs))
   validateNullConfig _ = Right topic

   validateAssignments (K.unKaArray -> Nothing) = Right topic
   validateAssignments (K.unKaArray -> Just as)
     | V.null as = Right topic
   validateAssignments _ = Left (K.INVALID_REQUEST, unsuportedPartitionAssignments)

   validateReplica replica
     | replica < -1 || replica == 0 = Left (K.INVALID_REPLICATION_FACTOR, invalidReplicaMsg)
     | otherwise                    = Right topic

   validateNumPartitions partitions
     | partitions < -1 || partitions == 0 = Left (K.INVALID_PARTITIONS, invalidNumPartitionsMsg)
     | otherwise                          = Right topic

--------------------
-- 20: DeleteTopics
--------------------
-- FIXME: The `timeoutMs` field of request is omitted.
handleDeleteTopics
  :: ServerContext -> K.RequestContext -> K.DeleteTopicsRequest -> IO K.DeleteTopicsResponse
handleDeleteTopics ServerContext{..} _ K.DeleteTopicsRequest{..} =
  case topicNames of
    K.KaArray Nothing ->
      -- FIXME: We return `[]` when topics is `Nothing`.
      --        Is this proper?
      return $ K.DeleteTopicsResponse {responses = K.KaArray $ Just V.empty, throttleTimeMs = 0}
    K.KaArray (Just topicNames_)
      | V.null topicNames_ -> return $ K.DeleteTopicsResponse {responses = K.KaArray $ Just V.empty, throttleTimeMs = 0}
      | otherwise          -> do
          respTopics <- forM topicNames_ $ \topicName -> do
            try (deleteTopic topicName) >>= \case
              Left (e :: SomeException)
                | Just _ <- fromException @S.NOTFOUND e -> do
                   Log.warning $ "Delete topic failed, topic " <> Log.build topicName <> " does not exist"
                   return $ K.DeletableTopicResult topicName K.UNKNOWN_TOPIC_OR_PARTITION
                | otherwise -> do
                    Log.warning $ "Exception occurs when deleting topic " <> Log.build topicName <> ": " <> Log.build (show e)
                    return $ K.DeletableTopicResult topicName K.UNKNOWN_SERVER_ERROR
              Right res -> return res
          return $ K.DeleteTopicsResponse {responses = K.KaArray $ Just respTopics, throttleTimeMs = 0}
  where
    -- FIXME: There can be some potential exceptions which are difficult to
    --        classify using Kafka's error code. So this function may throw
    --        exceptions.
    -- WARNING: This function may throw exceptions!
    --
    -- TODO: Handle topic that has subscription (i.e. cannot be deleted)
    deleteTopic :: T.Text -> IO K.DeletableTopicResult
    deleteTopic topicName = do
      let streamId = S.transToTopicStreamName topicName
      -- delete offset caches.
      --
      -- XXX: Normally we do not need to delete this because the logid is a
      -- random number and will unlikely be reused.
      logIds <- listTopicPartitionsOrdered scLDClient streamId
      V.forM_ logIds $ \logid ->
        cleanOffsetCache scOffsetManager logid
      S.removeStream scLDClient streamId
      return $ K.DeletableTopicResult topicName K.NONE

--------------------
-- 37: CreatePartitions
--------------------
handleCreatePartitions
  :: ServerContext -> K.RequestContext -> K.CreatePartitionsRequest -> IO K.CreatePartitionsResponse
handleCreatePartitions ctx _ req@K.CreatePartitionsRequest{..} = do
  let topics' = Utils.kaArrayToVector req.topics
      groups = L.groupBy (\a b -> a.name == b.name) . L.sortBy (\a b -> compare a.name b.name) $ V.toList topics'
      dups = concat $ L.filter (\l -> length l > 1) groups
      validReques = concat $ L.filter (\l -> length l == 1) groups

  succResults <- forM validReques $ \K.CreatePartitionsTopic{..} -> do
    res <- createPartitions ctx name count assignments timeoutMs validateOnly
    return $ mkResults name res
  let dupsResults = map (\t -> mkResults t.name (K.INVALID_REQUEST, "Duplicate topic in request.")) dups
  return K.CreatePartitionsResponse
        { results = K.KaArray . Just . V.fromList $ succResults <> dupsResults
        , throttleTimeMs = 0
        }
 where
   mkResults topic (code, msg) =
     K.CreatePartitionsTopicResult
       { name = topic
       , errorCode = code
       , errorMessage = Just msg
       }
