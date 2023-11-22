{-# LANGUAGE CPP                 #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}

module HStream.Kafka.Server.Handler.Topic
  ( -- 19: CreateTopics
    handleCreateTopicsV0

    -- 20: DeleteTopics
  , handleDeleteTopicsV0
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
import qualified HStream.Stats                      as Stats
import qualified HStream.Store                      as S
import qualified HStream.Utils                      as Utils
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
handleDeleteTopicsV0
  :: ServerContext -> K.RequestContext -> K.DeleteTopicsRequestV0 -> IO K.DeleteTopicsResponseV0
handleDeleteTopicsV0 ServerContext{..} _ K.DeleteTopicsRequestV0{..} =
  case topicNames of
    K.KaArray Nothing ->
      -- FIXME: We return `[]` when topics is `Nothing`.
      --        Is this proper?
      return $ K.DeleteTopicsResponseV0 (K.KaArray $ Just V.empty)
    K.KaArray (Just topicNames_)
      | V.null topicNames_ -> return $ K.DeleteTopicsResponseV0 (K.KaArray $ Just V.empty)
      | otherwise     -> do
          respTopics <- forM topicNames_ $ \topicName -> do
            try (deleteTopic topicName) >>= \case
              Left (e :: SomeException) -> do
                Log.warning $ "Exception occurs when deleting topic " <> Log.build topicName <> ": " <> Log.build (show e)
                return $ K.DeletableTopicResultV0 topicName K.UNKNOWN_SERVER_ERROR
              Right res -> return res
          return $ K.DeleteTopicsResponseV0 (K.KaArray $ Just respTopics)
  where
    -- FIXME: There can be some potential exceptions which are difficult to
    --        classify using Kafka's error code. So this function may throw
    --        exceptions.
    -- WARNING: This function may throw exceptions!
    --
    -- TODO: Handle topic that has subscription (i.e. cannot be deleted)
    deleteTopic :: T.Text -> IO K.DeletableTopicResultV0
    deleteTopic topicName = do
      let streamId = S.transToTopicStreamName topicName
      S.doesStreamExist scLDClient streamId >>= \case
        True  -> do
          -- delete offset caches.
          --
          -- XXX: Normally we do not need to delete this because the logid is a
          -- random number and will unlikely be reused.
          partitions <- S.listStreamPartitionsOrdered scLDClient streamId
          V.forM_ partitions $ \(_, logid) ->
            cleanOffsetCache scOffsetManager logid
          S.removeStream scLDClient streamId
          Stats.stream_stat_erase scStatsHolder (Utils.textToCBytes topicName)
          return $ K.DeletableTopicResultV0 topicName K.NONE
        False -> do
          Log.warning $ "Stream " <> Log.build (show streamId) <> " does not exist"
          return $ K.DeletableTopicResultV0 topicName K.UNKNOWN_TOPIC_OR_PARTITION
