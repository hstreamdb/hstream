{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Server.Core.Topic
 ( createTopic
 , createPartitions
 )
 where

import           Control.Exception                       (Exception (displayException, fromException),
                                                          SomeException, try)
import qualified Data.Aeson                              as J
import           Data.Bifunctor                          (Bifunctor (bimap))
import           Data.Int                                (Int16, Int32)
import qualified Data.Map                                as Map
import qualified Data.Map.Strict                         as M
import           Data.Maybe                              (isJust)
import           Data.Text                               (Text)
import qualified Data.Text                               as T
import qualified HStream.Base.Time                       as BaseTime
import qualified HStream.Kafka.Server.Config.KafkaConfig as KC
import           HStream.Kafka.Server.Core.Store         (createTopicPartitions,
                                                          getTotalPartitionCount)
import           HStream.Kafka.Server.Types              (ServerContext (..))
import qualified HStream.Logger                          as Log
import qualified HStream.Store                           as S
import qualified HStream.Utils                           as Utils
import qualified Kafka.Protocol                          as K
import qualified Kafka.Protocol.Error                    as K

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

createPartitions
  :: ServerContext
  -> T.Text
  -> Int32
  -> K.KaArray K.CreatePartitionsAssignment
  -> Int32
  -> Bool
  -> IO (K.ErrorCode, T.Text)
createPartitions ServerContext{..} topicName newPartitionCnt ass timeoutMs validateOnly
  | isJust $ K.unKaArray ass = do 
      Log.info $ "createPartitions for topic " <> Log.build topicName <> " failed because assignment is not supported."
      return (K.INVALID_REQUEST, "Partition assignments is not supported now.")
  | otherwise = do
     let streamId = S.transToStreamName topicName
     inc <- getIncreamentPartitions streamId
     case inc of
       Right cnt -> doCreate streamId cnt
       Left e    -> return e
 where
  getIncreamentPartitions streamId = do
    res <- try $ getTotalPartitionCount scLDClient streamId
    case res of
       Left (e :: SomeException)
         | Just (_ :: S.NOTFOUND) <- fromException e ->
             return . Left $ (K.UNKNOWN_TOPIC_OR_PARTITION, "The topic " <> topicName <> " does not exist")
         | otherwise -> do
             Log.fatal $ "getTotalPartitionCount for topic " <> Log.build topicName <> " failed: " <> Log.build (displayException e)
             return . Left $ (K.UNKNOWN_SERVER_ERROR, T.pack $ displayException e)
       Right oldPartitions
         | newPartitionCnt - oldPartitions < 0 -> do
             let msg = "Topic currently has " <> show oldPartitions <> " partitions, which is higher than the requested " <> show newPartitionCnt
             return . Left $ (K.INVALID_PARTITIONS, T.pack msg)
         | newPartitionCnt == oldPartitions ->
             return . Left $ (K.INVALID_PARTITIONS, "Topic already has " <> T.pack (show oldPartitions) <> " partitions")
         | otherwise -> return . Right $ newPartitionCnt - oldPartitions

  doCreate streamId cnt
    | timeoutMs <= 0 = return (K.REQUEST_TIMED_OUT, T.empty)
    | validateOnly = return (K.NONE, T.empty)
    | otherwise = do
       _ <- createTopicPartitions scLDClient streamId cnt
       return (K.NONE, T.empty)
