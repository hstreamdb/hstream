{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Server.Core.Topic
 ( createTopic
 )
 where

import           Control.Exception                       (Exception (displayException, fromException),
                                                          SomeException, try)
import           Control.Monad                           (forM)
import qualified Data.Aeson                              as J
import           Data.Bifunctor                          (Bifunctor (bimap))
import           Data.Char                               (chr, ord)
import           Data.Int                                (Int16, Int32)
import qualified Data.Map                                as Map
import qualified Data.Map.Strict                         as M
import           Data.Maybe                              (isJust)
import           Data.Text                               (Text)
import qualified Data.Text                               as T
import qualified HStream.Base.Time                       as BaseTime
import qualified HStream.Kafka.Server.Config.KafkaConfig as KC
import           HStream.Kafka.Server.Types              (ServerContext (..))
import qualified HStream.Logger                          as Log
import qualified HStream.Store                           as S
import qualified HStream.Utils                           as Utils
import qualified Kafka.Protocol.Error                    as K
import qualified Z.Data.CBytes                           as CB
import           Z.Data.CBytes                           (CBytes)

createTopic
  :: ServerContext
  -> Text
  -> Int16
  -> Int32
  -> Map.Map T.Text (Maybe T.Text)
  -> IO ((K.ErrorCode, T.Text), [S.C_LogID])
createTopic ServerContext{..} name replicationFactor numPartitions configs = do
  let streamId = S.transToTopicStreamName name
  timeStamp <- BaseTime.getSystemNsTimestamp
  let replica = if replicationFactor == -1
                  then kafkaBrokerConfigs.defaultReplicationFactor._value
                  else fromIntegral replicationFactor
  case KC.mkKafkaTopicConfigs configs of
    Left msg -> do
      Log.info $ "create topic failed, invalid config:" <> Log.build msg
      return ((K.INVALID_CONFIG, "Create topic with invalid config: " <> T.pack (show msg)), [])
    Right topicConfigs -> do
      -- FIXME: Is there any other attrs to store?
      -- FIXME: Should we parse any other attr from `confs` of `CreateableTopicV0`?
      let configs' = M.fromList . map (bimap Utils.textToCBytes (Utils.lazyByteStringToCBytes . J.encode)) . M.toList $ configs
          extraAttr = Map.union configs' $ M.fromList [ ("createTime", (Utils.textToCBytes . T.pack) $ show timeStamp) ]
          attrs = S.def { S.logReplicationFactor = S.defAttr1 replica
                        , S.logAttrsExtras       = extraAttr
                        , S.logBacklogDuration   = S.defAttr1 (getBacklogDuration topicConfigs)
                        }
      try (S.createStream scLDClient streamId attrs) >>= \case
        Left (e :: SomeException)
          | isJust (fromException @S.EXISTS e) -> do
            Log.warning $ "Topic already exists: " <> Log.build name
            return ((K.TOPIC_ALREADY_EXISTS, "Topic " <> name <> " already exists"), [])
          | otherwise -> do
              Log.warning $ "Exception occurs when creating stream " <> Log.build (show streamId) <> ": " <> Log.build (show e)
              return ((K.UNKNOWN_SERVER_ERROR, "Unexpected Server error"), [])
        Right _ -> do
          let partitions = if numPartitions == -1
                              then fromIntegral kafkaBrokerConfigs.numPartitions._value
                              else numPartitions
          shards_e <- try $ createTopicPartitions scLDClient streamId partitions
          case shards_e of
            Left (e :: SomeException) -> do
              Log.warning $ "Exception occurs when creating shards of topic " <> Log.build name <> ": " <> Log.build (show e)
              return ((K.INVALID_PARTITIONS, "Create shard for topic " <> name <> " error: " <> T.pack (displayException e)), [])
            Right shards -> do
              Log.info $ "Created " <> Log.build (show (length shards)) <> " shards for topic " <> Log.build name <> ": " <> Log.build (show shards)
              return ((K.NONE, T.empty), shards)
  where
    getBacklogDuration KC.KafkaTopicConfigs{cleanupPolicy=cleanupPolicy, retentionMs=KC.RetentionMs retentionMs}
      | cleanupPolicy == KC.CleanupPolicyCompact = Nothing
      | retentionMs `div` 1000 > 0 = Just (fromIntegral retentionMs `div` 1000)
      | otherwise = Nothing

createTopicPartitions :: S.LDClient -> S.StreamId -> Int32 -> IO [S.C_LogID]
createTopicPartitions client streamId partitions = do
  totalCnt <- getTotalPartitionCount client streamId
  forM [0..partitions-1] $ \i -> do
    let partitionId = encodePartitionId (totalCnt + i)
    S.createStreamPartition client streamId (Just partitionId) Map.empty

-- Get the total number of partitions of a topic
getTotalPartitionCount :: S.LDClient -> S.StreamId -> IO Int32
getTotalPartitionCount client streamId = do
  fromIntegral . M.size <$> S.listStreamPartitions client streamId

-- Encode numbers into strings following these rules:
-- the length of the number is mapped to the letters a-z, and then the number is appended.
-- For example, 1 becomes a1, 2 becomes a2, 10 becomes b10, 99 becomes b99,
-- 100 becomes c100, etc. This is done to facilitate sorting by alphabetical order.
encodePartitionId :: Int32 -> CBytes
encodePartitionId n =
  let strN = show n
      prefix = chr (ord 'a' + length strN - 1)
   in CB.pack (prefix : strN)

