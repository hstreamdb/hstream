{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Group.GroupOffsetManager
  ( GroupOffsetManager
  , mkGroupOffsetManager
  , storeOffsets
  , fetchOffsets
  , fetchAllOffsets
  ) where

import           Control.Concurrent                  (MVar, getNumCapabilities,
                                                      modifyMVar, modifyMVar_,
                                                      newMVar, readMVar,
                                                      withMVar)
import           Control.Exception                   (throw)
import           Control.Monad                       (void)
import           Data.Hashable
import qualified Data.HashMap.Strict                 as HM
import           Data.Int                            (Int32, Int64)
import qualified Data.Map.Strict                     as Map
import           Data.Maybe                          (fromMaybe)
import qualified Data.Text                           as T
import qualified Data.Vector                         as V
import           Data.Word                           (Word64)
import           GHC.Generics                        (Generic)
import           HStream.Kafka.Common.KafkaException (ErrorCodeException (ErrorCodeException))
import           HStream.Kafka.Group.OffsetsStore    (OffsetStorage (..),
                                                      mkCkpOffsetStorage)
import qualified HStream.Logger                      as Log
import qualified HStream.Store                       as S
import           HStream.Utils                       (limitedMapConcurrently)
import qualified Kafka.Protocol                      as K
import           Kafka.Protocol.Encoding             (KaArray (KaArray, unKaArray))
import qualified Kafka.Protocol.Error                as K
import           Kafka.Protocol.Message              (OffsetCommitRequestPartitionV0 (..),
                                                      OffsetCommitResponsePartitionV0 (..),
                                                      OffsetFetchResponsePartitionV0 (..))

data GroupOffsetManager = forall os. OffsetStorage os => GroupOffsetManager
  { serverId      :: Int32
  , ldClient      :: S.LDClient
  , groupName     :: T.Text
  , offsetStorage :: os
  , offsetsCache  :: MVar (Map.Map TopicPartition Int64)
  , partitionsMap :: MVar (HM.HashMap TopicPartition Word64)
  }

-- FIXME: if we create a consumer group with groupName haven been used, call
-- mkCkpOffsetStorage with groupName may lead us to a un-clean ckp-store
mkGroupOffsetManager :: S.LDClient -> Int32 -> T.Text -> IO GroupOffsetManager
mkGroupOffsetManager ldClient serverId groupName = do
  offsetsCache  <- newMVar Map.empty
  partitionsMap <- newMVar HM.empty
  offsetStorage <- mkCkpOffsetStorage ldClient groupName
  return GroupOffsetManager{..}

loadOffsetsFromStorage :: GroupOffsetManager -> V.Vector T.Text -> IO ()
loadOffsetsFromStorage GroupOffsetManager{..} topicNames = do
  concurrentCap <- getNumCapabilities
  void $ limitedMapConcurrently (min 8 concurrentCap) load $ V.toList topicNames
 where
   load topicName = do
     partitions <- S.listStreamPartitionsOrdered ldClient (S.transToTopicStreamName topicName)
     tpOffsets <- loadOffsets offsetStorage topicName

     let tpWithLogId = V.zipWith (\(_, logId) idx -> (mkTopicPartition topicName idx, logId)) partitions (V.fromList [0..])
     offsetsTuples <- V.forM tpWithLogId $ \(tp, logId) -> do
       case Map.lookup logId tpOffsets of
         Nothing -> do
           -- FIXME: here we found that a partition is belong to a topic, but it doesn't have any offset commit record in
           -- __offset_commit topic. find a way to handle this situation
           undefined
         Just offset -> return (tp, fromIntegral offset)

     let partitionMap = HM.fromList . V.toList $ tpWithLogId
     let offsetsMap = Map.fromList . V.toList $ offsetsTuples
     Log.info $ "loadOffsets for topic " <> Log.build topicName
             <> ", partitionMap: " <> Log.build (show partitionMap)
             <> ", offsetsMap: " <> Log.build (show offsetsMap)
     -- update offsetsCache
     modifyMVar_ offsetsCache $ return . Map.union offsetsMap
     -- update partitionsMap
     modifyMVar_ partitionsMap $ return . HM.union partitionMap

storeOffsets
  :: GroupOffsetManager
  -> T.Text
  -> KaArray OffsetCommitRequestPartitionV0
  -> IO (KaArray OffsetCommitResponsePartitionV0)
storeOffsets gmm@GroupOffsetManager{..} topicName arrayOffsets = do
  let offsets = fromMaybe V.empty (unKaArray arrayOffsets)

  -- check if a TopicPartition that has an offset to be committed is contained in current
  -- consumer group's partitionsMap. If not, server will return a UNKNOWN_TOPIC_OR_PARTITION
  -- error, and that error will be convert to COORDINATOR_NOT_AVAILABLE error finally
  offsetsInfo <- getOffsetsInfo gmm topicName offsets
  Log.debug $ "get offsetsInfo for topic " <> Log.build topicName <> ": " <> Log.build (show offsetsInfo)

  -- write checkpoints
  let checkPoints = V.foldl' (\acc (_, logId, offset) -> Map.insert logId offset acc) Map.empty offsetsInfo
  commitOffsets offsetStorage topicName checkPoints
  Log.debug $ "consumer group " <> Log.build groupName <> " commit offsets {" <> Log.build (show checkPoints)
           <> "} to topic " <> Log.build topicName

  -- update cache
  modifyMVar_ offsetsCache $ \cache -> do
    let updates = V.foldl' (\acc (key, _, offset) -> Map.insert key (fromIntegral offset) acc) Map.empty offsetsInfo
    return $ Map.union updates cache

  let suc = V.map (\(TopicPartition{topicPartitionIdx}, _, _) -> (topicPartitionIdx, K.NONE)) offsetsInfo
      res = V.map (\(partitionIndex, errorCode) -> OffsetCommitResponsePartitionV0{..}) suc
  return KaArray {unKaArray = Just res}

getOffsetsInfo
  :: GroupOffsetManager
  -> T.Text
  -> V.Vector K.OffsetCommitRequestPartitionV0
  -> IO (V.Vector (TopicPartition, Word64, Word64)) -- return [(topic-partition, logid, offset)]
getOffsetsInfo GroupOffsetManager{..} topicName requestOffsets = do
  V.forM requestOffsets $ \OffsetCommitRequestPartitionV0{..} -> do
    let tp = mkTopicPartition topicName partitionIndex
    modifyMVar partitionsMap $ \mp -> do
      case HM.lookup tp mp of
        Nothing -> do
          Log.info $ "can't find topic-partition " <> Log.build (show tp) <> " in partitionsMap: " <> Log.build (show mp)
          -- read partitions and build partitionsMap
          partitions <- S.listStreamPartitionsOrdered ldClient (S.transToTopicStreamName topicName)
          Log.info $ "list all partitions for topic " <> Log.build topicName <> ": " <> Log.build (show partitions)
          case partitions V.!? (fromIntegral partitionIndex) of
            Nothing -> do
              Log.info $ "consumer group " <> Log.build groupName <> " receive OffsetCommitRequestPartition with unknown topic or partion"
                      <> ", topic name: " <> Log.build topicName
                      <> ", partition: " <> Log.build partitionIndex
              throw (ErrorCodeException K.UNKNOWN_TOPIC_OR_PARTITION)
              -- ^ TODO: better response(and exception)
            Just (_, logId) -> do
              let partitionMap = HM.fromList . V.toList $ V.zipWith (\idx (_, lgId) -> ((mkTopicPartition topicName idx), lgId)) (V.fromList [0..]) partitions
                  mp' = HM.union partitionMap mp
              return (mp', (tp, logId, fromIntegral committedOffset))
        Just logId -> return (mp, (tp, logId, fromIntegral committedOffset))

fetchOffsets
  :: GroupOffsetManager
  -> T.Text
  -> KaArray Int32
  -> IO (KaArray OffsetFetchResponsePartitionV0)
fetchOffsets GroupOffsetManager{..} topicName partitions = do
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

fetchAllOffsets :: GroupOffsetManager -> IO (KaArray K.OffsetFetchResponseTopicV0)
fetchAllOffsets GroupOffsetManager{..} = do
  -- group offsets by TopicName
  cachedOffset <- Map.foldrWithKey foldF Map.empty <$> readMVar offsetsCache
  return . KaArray . Just . V.map makeTopic . V.fromList . Map.toList $ cachedOffset
  where makePartition partition offset = OffsetFetchResponsePartitionV0
                   { committedOffset = offset
                   , metadata = Nothing
                   , partitionIndex=partition
                   , errorCode = K.NONE
                   }
        foldF tp offset = Map.insertWith (V.++) tp.topicName (V.singleton (makePartition tp.topicPartitionIdx offset))
        makeTopic (name, partitions) = K.OffsetFetchResponseTopicV0 {partitions=KaArray (Just partitions), name=name}

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
