{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

module HStream.Kafka.Server.Handler.Topic
  ( -- 19: CreateTopics
    handleCreateTopicsV0
    -- 20: DeleteTopics
  , handleDeleteTopics
  ) where

import           Control.Exception
import           Control.Monad
import qualified Data.Text                          as T
import qualified Data.Vector                        as V

import qualified Data.Map                           as Map
import           HStream.Kafka.Common.OffsetManager (cleanOffsetCache)
import qualified HStream.Kafka.Common.Utils         as Utils
import qualified HStream.Kafka.Server.Core.Topic    as Core
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import qualified HStream.Store                      as S
import qualified Kafka.Protocol.Encoding            as K
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K

--------------------
-- 19: CreateTopics
--------------------
-- FIXME: The `timeoutMs` field of request is omitted.
handleCreateTopicsV0
  :: ServerContext -> K.RequestContext -> K.CreateTopicsRequestV0 -> IO K.CreateTopicsResponseV0
handleCreateTopicsV0 ctx _ K.CreateTopicsRequestV0{..} =
  case topics of
    K.KaArray Nothing ->
      -- FIXME: We return `[]` when topics is `Nothing`.
      --        Is this proper?
      return $ K.CreateTopicsResponseV0 (K.KaArray $ Just V.empty)
    K.KaArray (Just topics_)
      | V.null topics_ -> return $ K.CreateTopicsResponseV0 (K.KaArray $ Just V.empty)
      | otherwise     -> do
          respTopics <- forM topics_ $ createTopic
          return $ K.CreateTopicsResponseV0 (K.KaArray $ Just respTopics)
  where
    createTopic :: K.CreatableTopicV0 -> IO K.CreatableTopicResultV0
    createTopic K.CreatableTopicV0{..}
      | replicationFactor < -1 || replicationFactor == 0 = do
          Log.warning $ "Expect a positive replicationFactor but got " <> Log.build replicationFactor
          return $ K.CreatableTopicResultV0 name K.INVALID_REPLICATION_FACTOR
      | numPartitions < -1 || numPartitions == 0 = do
          Log.warning $ "Expect a positive numPartitions but got " <> Log.build numPartitions
          return $ K.CreatableTopicResultV0 name K.INVALID_PARTITIONS
      | otherwise = do
          let configMap = Map.fromList . map (\c -> (c.name, c.value)) . Utils.kaArrayToList $ configs
          (errorCode, _) <- Core.createTopic ctx name replicationFactor numPartitions configMap
          return $ K.CreatableTopicResultV0 name errorCode

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
      partitions <- S.listStreamPartitionsOrdered scLDClient streamId
      V.forM_ partitions $ \(_, logid) ->
        cleanOffsetCache scOffsetManager logid
      S.removeStream scLDClient streamId
      return $ K.DeletableTopicResult topicName K.NONE
