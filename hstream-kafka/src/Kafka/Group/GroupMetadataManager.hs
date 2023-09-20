module Kafka.Group.GroupMetadataManager 
  ( mkGroupMetadataManager
  , storeOffsets
  ) where

import qualified Data.Text as T
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict                as Map
import Kafka.Protocol.Encoding (KaArray (unKaArray))
import Kafka.Protocol.Message.Struct (OffsetCommitRequestPartitionV0 (..))
import qualified Data.Vector as V
import Data.Hashable 
import Data.Maybe (fromMaybe)
import Kafka.Protocol.Error (ErrorCode)
import qualified Kafka.Protocol.Error as K
import Data.Int (Int32)
import Data.Word (Word64)
import qualified HStream.Store as S
import HStream.Utils (textToCBytes)
import qualified HStream.Logger                as Log
import Control.Concurrent (MVar, modifyMVar_, newMVar)
import Control.Concurrent.MVar (readMVar)
import GHC.Generics (Generic)

data GroupMetadataManager = GroupMetadataManager
  { serverId          :: Int
  , groupName         :: T.Text
  , offsetsCache      :: MVar (Map.Map TopicPartition Int)
  , offsetTopicId     :: Word64
    -- ^ __consumer_offsets logID
  , offsetCkpStore    :: S.LDCheckpointStore
  , partitionsMap     :: MVar (HM.HashMap TopicPartition S.C_LogID)
    -- ^ partitionsMap maps TopicPartition to the underlying logID
  }

mkGroupMetadataManager :: S.LDClient -> Int -> T.Text -> IO GroupMetadataManager
mkGroupMetadataManager ldClient serverId groupName = do
  offsetsCache  <- newMVar Map.empty
  partitionsMap <- newMVar HM.empty

  let cbGroupName = textToCBytes groupName
  -- FIXME: need to get log attributes from somewhere
  S.initOffsetCheckpointDir ldClient S.def
  offsetTopicId <- S.allocOffsetCheckpointId ldClient cbGroupName
  Log.info $ "allocate offset checkpoint store id " <> Log.build offsetTopicId <> " for group " <> Log.build groupName
  offsetCkpStore <- S.newRSMBasedCheckpointStore ldClient offsetTopicId 5000

  return GroupMetadataManager{..}
 where
   rebuildCache = do
     undefined

storeOffsets 
  :: GroupMetadataManager 
  -> T.Text
  -> KaArray OffsetCommitRequestPartitionV0
  -> IO (HM.HashMap Int32 ErrorCode)
storeOffsets GroupMetadataManager{..} topicName arrayOffsets = do
  let offsets = fromMaybe V.empty (unKaArray arrayOffsets)

  -- TODO: 检查 offsetsCache 中是否包含待提交 offset 的 TopicPartition
  -- 在 kafka 中，分区加入 consumer group 需要执行额外的操作，因此如果某个 TopicPartition
  -- 没有包含在当前的缓存中，则需要返回 UNKNOWN_TOPIC_OR_PARTITION

  partitionsInfo <- readMVar partitionsMap
  let (notFoundErrs, offsets') = V.partitionWith
       ( \OffsetCommitRequestPartitionV0{..} -> 
            let key = mkTopicPartition topicName partitionIndex 
             in case HM.lookup key partitionsInfo of
                  Just logId -> Right $ (key, logId, fromIntegral committedOffset)
                  Nothing    -> Left $ (partitionIndex, K.COORDINATOR_NOT_AVAILABLE)
       ) offsets

  -- write checkpoints
  let checkPoints = V.foldl' (\acc (_, logId, offset) -> Map.insert logId offset acc) Map.empty offsets'
  S.ckpStoreUpdateMultiLSNSync offsetCkpStore (textToCBytes groupName) checkPoints

  -- update cache
  modifyMVar_ offsetsCache $ \cache -> do
    let updates = V.foldl' (\acc (key, _, offset) -> Map.insert key (fromIntegral offset) acc) Map.empty offsets'
    return $ Map.union updates cache

  let res = V.foldl' (\acc (TopicPartition{topicPartitionIdx}, _, _) -> HM.insert topicPartitionIdx K.NONE acc) HM.empty offsets'
  return $ HM.union res (HM.fromList . V.toList $ notFoundErrs) 

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
