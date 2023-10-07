module HStream.Kafka.Group.GroupMetadataManager
  ( GroupMetadataManager
  , mkGroupMetadataManager
  , storeOffsets
  , fetchOffsets
  ) where

import           Control.Concurrent               (MVar, modifyMVar_, newMVar,
                                                   withMVar)
import           Control.Concurrent.MVar          (readMVar)
import           Control.Monad                    (unless)
import           Data.Hashable
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromMaybe)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word64)
import           GHC.Generics                     (Generic)
import           HStream.Kafka.Group.OffsetsStore (OffsetStorage (..))
import qualified HStream.Logger                   as Log
import           Kafka.Protocol.Encoding          (KaArray (KaArray, unKaArray))
import qualified Kafka.Protocol.Error             as K
import           Kafka.Protocol.Message           (OffsetCommitRequestPartitionV0 (..),
                                                   OffsetCommitResponsePartitionV0 (..),
                                                   OffsetFetchResponsePartitionV0 (..))

data GroupMetadataManager = forall os. OffsetStorage os => GroupMetadataManager
  { serverId      :: Int32
  , groupName     :: T.Text
  , offsetStorage :: os
  , offsetsCache  :: MVar (Map.Map TopicPartition Int64)
  , partitionsMap :: MVar (HM.HashMap TopicPartition Word64)
    -- ^ partitionsMap maps TopicPartition to the underlying logID
  }

mkGroupMetadataManager :: OffsetStorage os => os -> Int32 -> T.Text -> IO GroupMetadataManager
mkGroupMetadataManager offsetStorage serverId groupName = do
  offsetsCache  <- newMVar Map.empty
  partitionsMap <- newMVar HM.empty

  return GroupMetadataManager{..}
 where
   rebuildCache = do
     undefined

storeOffsets
  :: GroupMetadataManager
  -> T.Text
  -> KaArray OffsetCommitRequestPartitionV0
  -> IO (KaArray OffsetCommitResponsePartitionV0)
storeOffsets GroupMetadataManager{..} topicName arrayOffsets = do
  let offsets = fromMaybe V.empty (unKaArray arrayOffsets)

  -- check if a TopicPartition that has an offset to be committed is contained in current
  -- consumer group's partitionsMap. If not, server will return a UNKNOWN_TOPIC_OR_PARTITION
  -- error, and that error will be convert to COORDINATOR_NOT_AVAILABLE error finally
  partitionsInfo <- readMVar partitionsMap
  let (notFoundErrs, offsets') = V.partitionWith
       ( \OffsetCommitRequestPartitionV0{..} ->
            let key = mkTopicPartition topicName partitionIndex
             in case HM.lookup key partitionsInfo of
                  Just logId -> Right $ (key, logId, fromIntegral committedOffset)
                  Nothing    -> Left $ (partitionIndex, K.COORDINATOR_NOT_AVAILABLE)
       ) offsets
  unless (V.null notFoundErrs) $ do
    Log.info $ "consumer group " <> Log.build groupName <> " receive OffsetCommitRequestPartition with unknown topic or partion"
            <> ", topic name: " <> Log.build topicName
            <> ", partitions: " <> Log.build (show $ V.map fst notFoundErrs)

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
      res = V.map (\(partitionIndex, errorCode) -> OffsetCommitResponsePartitionV0{..}) (suc <> notFoundErrs)
  return KaArray {unKaArray = Just res}

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
