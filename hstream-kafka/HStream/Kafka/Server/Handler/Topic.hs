{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Kafka.Server.Handler.Topic
  ( -- 19: CreateTopics
    handleCreateTopicsV0

    -- 20: DeleteTopics
  , handleDeleteTopicsV0
  ) where

import           Control.Exception
import           Control.Monad
import qualified Data.Map.Strict             as M
import           Data.Maybe
import qualified Data.Text                   as T
import qualified Data.Vector                 as V

import qualified HStream.Base.Time           as BaseTime
import qualified HStream.Common.Server.Shard as Shard
import qualified HStream.Common.Types        as CommonTypes
import           HStream.Kafka.Server.Types  (ServerContext (..))
import qualified HStream.Logger              as Log
import qualified HStream.Stats               as Stats
import qualified HStream.Store               as S
import qualified HStream.Utils               as Utils
import qualified Kafka.Protocol.Encoding     as K
import qualified Kafka.Protocol.Error        as K
import qualified Kafka.Protocol.Message      as K
import qualified Kafka.Protocol.Service      as K

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
          respTopics <- forM topics_ createTopic
          return $ K.CreateTopicsResponseV0 (K.KaArray $ Just respTopics)
  where
    createTopic :: K.CreatableTopicV0 -> IO K.CreatableTopicResultV0
    createTopic K.CreatableTopicV0{..}
      | replicationFactor <= 0 = do
          Log.warning $ "Expect a positive replicationFactor but got " <> Log.build replicationFactor
          return $ K.CreatableTopicResultV0 name K.INVALID_REPLICATION_FACTOR
      | numPartitions <= 0     = do
          Log.warning $ "Expect a positive numPartitions but got " <> Log.build numPartitions
          return $ K.CreatableTopicResultV0 name K.INVALID_PARTITIONS
      | otherwise = do
          let streamId = Utils.transToStreamName name
          timeStamp <- BaseTime.getSystemNsTimestamp
          -- FIXME: Is there any other attrs to store?
          -- FIXME: Should we parse any other attr from `confs` of `CreateableTopicV0`?
          let extraAttr = M.fromList [("createTime", (Utils.textToCBytes . T.pack) $ show timeStamp)]
              attrs = S.def { S.logReplicationFactor = S.defAttr1 (fromIntegral replicationFactor)
                            , S.logAttrsExtras       = extraAttr
                            }
          try (S.createStream (scLDClient ctx) streamId attrs) >>= \case
            Left (e :: SomeException)
              | isJust (fromException @S.EXISTS e) -> do
                Log.warning $ "Stream already exists: " <> Log.build (show streamId)
                return $ K.CreatableTopicResultV0 name K.TOPIC_ALREADY_EXISTS
              | otherwise -> do
                  Log.warning $ "Exception occurs when creating stream " <> Log.build (show streamId) <> ": " <> Log.build (show e)
                  return $ K.CreatableTopicResultV0 name K.UNKNOWN_SERVER_ERROR
            Right _ -> do
              let keyTups = CommonTypes.devideKeySpace (fromIntegral numPartitions)
              shards_e <-
                try $ forM (keyTups `zip` [0..]) $ \((startKey, endKey), i) -> do
                  let shard = Shard.mkShard i streamId startKey endKey (fromIntegral numPartitions)
                  Shard.createShard (scLDClient ctx) shard
              case shards_e of
                Left (e :: SomeException) -> do
                  Log.warning $ "Exception occurs when creating shards of stream " <> Log.build (show streamId) <> ": " <> Log.build (show e)
                  return $ K.CreatableTopicResultV0 name K.INVALID_PARTITIONS
                Right shards -> do
                  Log.debug $ "Created " <> Log.build (show (length shards)) <> " shards for stream " <> Log.build name <> ": " <> Log.build (show (Shard.shardId <$> shards))
                  return $ K.CreatableTopicResultV0 name K.NONE

--------------------
-- 20: DeleteTopics
--------------------
-- FIXME: The `timeoutMs` field of request is omitted.
handleDeleteTopicsV0
  :: ServerContext -> K.RequestContext -> K.DeleteTopicsRequestV0 -> IO K.DeleteTopicsResponseV0
handleDeleteTopicsV0 ctx _ K.DeleteTopicsRequestV0{..} =
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
      let streamId = Utils.transToStreamName topicName
      S.doesStreamExist (scLDClient ctx) streamId >>= \case
        True  -> do
          S.removeStream (scLDClient ctx) streamId
          Stats.stream_stat_erase (scStatsHolder ctx) (Utils.textToCBytes topicName)
          return $ K.DeletableTopicResultV0 topicName K.NONE
        False -> do
          Log.warning $ "Stream " <> Log.build (show streamId) <> " does not exist"
          return $ K.DeletableTopicResultV0 topicName K.UNKNOWN_TOPIC_OR_PARTITION
