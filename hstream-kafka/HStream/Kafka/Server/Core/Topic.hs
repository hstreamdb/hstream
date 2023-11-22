module HStream.Kafka.Server.Core.Topic
 ( createTopic
 )
 where

import           Control.Exception                       (Exception (fromException),
                                                          SomeException, try)
import           Control.Monad                           (forM)
import           Data.Int                                (Int16, Int32)
import qualified Data.Map.Strict                         as M
import           Data.Maybe                              (isJust)
import           Data.Text                               (Text)
import qualified Data.Text                               as T

import qualified Data.Aeson                              as J
import           Data.Bifunctor                          (Bifunctor (bimap))
import qualified Data.Map                                as Map
import qualified HStream.Base.Time                       as BaseTime
import qualified HStream.Common.Server.Shard             as Shard
import qualified HStream.Common.Types                    as CommonTypes
import qualified HStream.Kafka.Server.Config.KafkaConfig as KC
import           HStream.Kafka.Server.Types              (ServerContext (..))
import qualified HStream.Logger                          as Log
import qualified HStream.Store                           as S
import qualified HStream.Utils                           as Utils
import qualified Kafka.Protocol.Error                    as K

createTopic :: ServerContext -> Text -> Int16 -> Int32 -> Map.Map T.Text (Maybe T.Text) -> IO (K.ErrorCode, [Shard.Shard])
createTopic ServerContext{..} name replicationFactor numPartitions configs = do
  let streamId = S.transToTopicStreamName name
  timeStamp <- BaseTime.getSystemNsTimestamp
  let replica = if replicationFactor == -1 then scDefaultTopicRepFactor else fromIntegral replicationFactor
  case KC.mkKafkaTopicConfigs configs of
    Left msg -> do
      Log.info $ "create topic failed, invaid config:" <> Log.build msg
      return (K.INVALID_CONFIG, [])
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
            Log.warning $ "Stream already exists: " <> Log.build (show streamId)
            return (K.TOPIC_ALREADY_EXISTS, [])
          | otherwise -> do
              Log.warning $ "Exception occurs when creating stream " <> Log.build (show streamId) <> ": " <> Log.build (show e)
              return (K.UNKNOWN_SERVER_ERROR, [])
        Right _ -> do
          let partitions = if numPartitions == -1 then scDefaultPartitionNum else fromIntegral numPartitions
          let keyTups = CommonTypes.devideKeySpace partitions
          shards_e <-
            try $ forM (keyTups `zip` [0..]) $ \((startKey, endKey), i) -> do
              let shard = Shard.mkShard i streamId startKey endKey (fromIntegral numPartitions)
              Shard.createShard scLDClient shard
          case shards_e of
            Left (e :: SomeException) -> do
              Log.warning $ "Exception occurs when creating shards of stream " <> Log.build (show streamId) <> ": " <> Log.build (show e)
              return (K.INVALID_PARTITIONS, [])
            Right shards -> do
              Log.info $ "Created " <> Log.build (show (length shards)) <> " shards for stream " <> Log.build name <> ": " <> Log.build (show (Shard.shardId <$> shards))
              return (K.NONE, shards)
  where
    getBacklogDuration KC.KafkaTopicConfigs{cleanupPolicy=cleanupPolicy, retentionMs=KC.RetentionMs retentionMs}
      | cleanupPolicy == KC.CleanupPolicyCompact = Nothing
      | retentionMs `div` 1000 > 0 = Just (fromIntegral retentionMs `div` 1000)
      | otherwise = Nothing

