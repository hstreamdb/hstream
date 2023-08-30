{-# LANGUAGE OverloadedStrings #-}

module HStream.Server.KafkaHandler.Topic
  ( -- 19: CreateTopics
    handleCreateTopicsV0
  ) where

import           Control.Exception
import           Control.Monad
import qualified Data.Map.Strict               as M
import           Data.Maybe
import qualified Data.Text                     as T
import qualified Data.Vector                   as V

import qualified HStream.Base.Time             as BaseTime
import qualified HStream.Common.Types          as CommonTypes
import qualified HStream.Logger                as Log
import qualified HStream.Server.Shard          as Shard
import qualified HStream.Server.Types          as HsTypes
import qualified HStream.Store                 as S
import qualified HStream.Utils                 as Utils

import qualified Kafka.Protocol.Error          as K
import qualified Kafka.Protocol.Message.Struct as K
import qualified Kafka.Protocol.Service        as K

--------------------
-- 19: CreateTopics
--------------------
-- FIXME: The `timeoutMs` field of request is omitted.
handleCreateTopicsV0
  :: HsTypes.ServerContext -> K.RequestContext -> K.CreateTopicsRequestV0 -> IO K.CreateTopicsResponseV0
handleCreateTopicsV0 ctx _ K.CreateTopicsRequestV0{..} =
  case topics of
    Nothing     ->
      -- FIXME: We return `[]` when topics is `Nothing`.
      --        Is this proper?
      return $ K.CreateTopicsResponseV0 (Just V.empty)
    Just topics_
      | V.null topics_ -> return $ K.CreateTopicsResponseV0 (Just V.empty)
      | otherwise     -> do
          respTopics <- forM topics_ createTopic
          return $ K.CreateTopicsResponseV0 (Just respTopics)
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
          let streamId = HsTypes.transToStreamName name
          timeStamp <- BaseTime.getSystemNsTimestamp
          -- FIXME: Is there any other attrs to store?
          -- FIXME: Should we parse any other attr from `confs` of `CreateableTopicV0`?
          let extraAttr = M.fromList [("createTime", (Utils.textToCBytes . T.pack) $ show timeStamp)]
              attrs = S.def { S.logReplicationFactor = S.defAttr1 (fromIntegral replicationFactor)
                            , S.logAttrsExtras       = extraAttr
                            }
          try (S.createStream (HsTypes.scLDClient ctx) streamId attrs) >>= \case
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
                  Shard.createShard (HsTypes.scLDClient ctx) shard
              case shards_e of
                Left (e :: SomeException) -> do
                  Log.warning $ "Exception occurs when creating shards of stream " <> Log.build (show streamId) <> ": " <> Log.build (show e)
                  return $ K.CreatableTopicResultV0 name K.INVALID_PARTITIONS
                Right shards -> do
                  Log.debug $ "Created " <> Log.build (show (length shards)) <> " shards for stream " <> Log.build name <> ": " <> Log.build (show (Shard.shardId <$> shards))
                  return $ K.CreatableTopicResultV0 name K.NONE
