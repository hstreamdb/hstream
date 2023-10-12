module HStream.Kafka.Group.GroupMetadataManager
  ( GroupMetadataManager
  , mkGroupMetadataManager
  , storeOffsets
  , fetchOffsets
  ) where

import           Control.Concurrent                  (MVar, modifyMVar_,
                                                      newMVar, withMVar)
import           Control.Exception                   (throw)
import qualified Control.Monad                       as M
import           Data.Hashable
import qualified Data.HashTable.IO                   as H
import           Data.Int                            (Int32, Int64)
import qualified Data.Map.Strict                     as Map
import           Data.Maybe                          (fromMaybe)
import qualified Data.Text                           as T
import qualified Data.Vector                         as V
import           Data.Word                           (Word64)
import           GHC.Generics                        (Generic)
import           HStream.Kafka.Common.KafkaException (ErrorCodeException (ErrorCodeException))
import qualified HStream.Kafka.Common.Utils          as Utils
import           HStream.Kafka.Group.OffsetsStore    (OffsetStorage (..),
                                                      mkCkpOffsetStorage)
import qualified HStream.Logger                      as Log
import qualified HStream.Store                       as S
import qualified Kafka.Protocol                      as K
import           Kafka.Protocol.Encoding             (KaArray (KaArray, unKaArray))
import qualified Kafka.Protocol.Error                as K
import           Kafka.Protocol.Message              (OffsetCommitRequestPartitionV0 (..),
                                                      OffsetCommitResponsePartitionV0 (..),
                                                      OffsetFetchResponsePartitionV0 (..))

data GroupMetadataManager = forall os. OffsetStorage os => GroupMetadataManager
  { serverId      :: Int32
  , ldClient      :: S.LDClient
  , groupName     :: T.Text
  , offsetStorage :: os
  , offsetsCache  :: MVar (Map.Map TopicPartition Int64)
  , partitionsMap :: Utils.HashTable TopicPartition Word64
    -- ^ partitionsMap maps TopicPartition to the underlying logID
  }

mkGroupMetadataManager :: S.LDClient -> Int32 -> T.Text -> IO GroupMetadataManager
mkGroupMetadataManager ldClient serverId groupName = do
  offsetsCache  <- newMVar Map.empty
  partitionsMap <- H.new
  offsetStorage <- mkCkpOffsetStorage ldClient groupName

  return GroupMetadataManager{..}
 where
   rebuildCache = do
     undefined

storeOffsets
  :: GroupMetadataManager
  -> T.Text
  -> KaArray OffsetCommitRequestPartitionV0
  -> IO (KaArray OffsetCommitResponsePartitionV0)
storeOffsets gmm@GroupMetadataManager{..} topicName arrayOffsets = do
  let offsets = fromMaybe V.empty (unKaArray arrayOffsets)

  -- check if a TopicPartition that has an offset to be committed is contained in current
  -- consumer group's partitionsMap. If not, server will return a UNKNOWN_TOPIC_OR_PARTITION
  -- error, and that error will be convert to COORDINATOR_NOT_AVAILABLE error finally
  offsets' <- computeCheckpointOffsets gmm topicName offsets

  -- write checkpoints
  let checkPoints = V.foldl' (\acc (_, logId, offset) -> Map.insert logId offset acc) Map.empty offsets'
  commitOffsets offsetStorage topicName checkPoints
  Log.debug $ "consumer group " <> Log.build groupName <> " commit offsets {" <> Log.build (show checkPoints)
           <> "} to topic " <> Log.build topicName

  -- update cache
  modifyMVar_ offsetsCache $ \cache -> do
    let updates = V.foldl' (\acc (key, _, offset) -> Map.insert key (fromIntegral offset) acc) Map.empty offsets'
    return $ Map.union updates cache

  let suc = V.map (\(TopicPartition{topicPartitionIdx}, _, _) -> (topicPartitionIdx, K.NONE)) offsets'
      res = V.map (\(partitionIndex, errorCode) -> OffsetCommitResponsePartitionV0{..}) suc
  return KaArray {unKaArray = Just res}

computeCheckpointOffsets :: GroupMetadataManager -> T.Text -> V.Vector K.OffsetCommitRequestPartitionV0
  -> IO (V.Vector (TopicPartition, Word64, Word64))
computeCheckpointOffsets GroupMetadataManager{..} topicName requestOffsets = do
  V.forM requestOffsets $ \OffsetCommitRequestPartitionV0{..} -> do
    let tp = mkTopicPartition topicName partitionIndex
    H.lookup partitionsMap tp >>= \case
      Nothing -> do
        -- read partitions and build partitionsMap
        partitions <- S.listStreamPartitionsOrdered ldClient (S.transToTopicStreamName topicName)
        M.forM_ (zip [0..] (V.toList partitions)) $ \(idx, (_, logId)) -> do
          H.insert partitionsMap (TopicPartition topicName idx) logId
        case partitions V.!? (fromIntegral partitionIndex) of
          Nothing -> do
            Log.info $ "consumer group " <> Log.build groupName <> " receive OffsetCommitRequestPartition with unknown topic or partion"
                    <> ", topic name: " <> Log.build topicName
                    <> ", partition: " <> Log.build partitionIndex
            throw (ErrorCodeException K.UNKNOWN_TOPIC_OR_PARTITION)
          -- ^ TODO: better response(and exception)
          Just (_, logId) -> return (tp, logId, fromIntegral committedOffset)
      Just logId -> return (tp, logId, fromIntegral committedOffset)

fetchOffsets
  :: GroupMetadataManager
  -> T.Text
  -> KaArray Int32
  -> IO (KaArray OffsetFetchResponsePartitionV0)
fetchOffsets GroupMetadataManager{..} topicName partitions = do
  let partitions' = fromMaybe V.empty (unKaArray partitions)
  res <- withMVar offsetsCache $ \cache -> do
    traverse
      (
       \ partitionIdx -> do
           let key = mkTopicPartition topicName partitionIdx
            in case Map.lookup key cache of
                 Just offset -> return $ OffsetFetchResponsePartitionV0
                   { committedOffset = offset
                   , metadata = Nothing
                   , partitionIndex= partitionIdx
                   , errorCode = K.NONE
                   }
                 Nothing -> return $ OffsetFetchResponsePartitionV0
                   { committedOffset = -1
                   , metadata = Nothing
                   , partitionIndex= partitionIdx
                   -- TODO: check the error code here
                   , errorCode = K.NONE
                   }
      ) partitions'

  return $ KaArray {unKaArray = Just res}

-------------------------------------------------------------------------------------------------
-- helper

data TopicPartition = TopicPartition
 { topicName         :: T.Text
 , topicPartitionIdx :: Int32
   -- ^ logId of each partition, a.k.a partitionIndex in Kafka
 } deriving(Eq, Generic, Ord)

instance Hashable TopicPartition

instance Show TopicPartition where
  show TopicPartition{..} = show topicName <> "-" <> show topicPartitionIdx

mkTopicPartition :: T.Text -> Int32 -> TopicPartition
mkTopicPartition topicName topicPartitionIdx = TopicPartition{..}
