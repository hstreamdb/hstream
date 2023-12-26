{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Group.GroupOffsetManager
  ( GroupOffsetManager
  , mkGroupOffsetManager
  , storeOffsets
  , fetchOffsets
  , fetchAllOffsets
  , nullOffsets
  , loadOffsetsFromStorage
  ) where

import           Control.Exception                   (throw)
import           Data.Hashable
import           Data.Int                            (Int32, Int64)
import           Data.IORef                          (IORef, modifyIORef',
                                                      newIORef, readIORef,
                                                      writeIORef)
import qualified Data.Map.Strict                     as Map
import           Data.Maybe                          (fromMaybe)
import           Data.Set                            (Set)
import qualified Data.Set                            as S
import qualified Data.Text                           as T
import qualified Data.Vector                         as V
import           Data.Word                           (Word64)
import           GHC.Generics                        (Generic)
import           System.Clock

import           HStream.Kafka.Common.KafkaException (ErrorCodeException (ErrorCodeException))
import qualified HStream.Kafka.Common.Metrics        as M
import           HStream.Kafka.Group.OffsetsStore    (OffsetStorage (..),
                                                      mkCkpOffsetStorage)
import qualified HStream.Logger                      as Log
import qualified HStream.Store                       as LD
import qualified HStream.Store                       as S
import qualified Kafka.Protocol                      as K
import           Kafka.Protocol.Encoding             (KaArray (KaArray, unKaArray))
import qualified Kafka.Protocol.Error                as K
import           Kafka.Protocol.Message              (OffsetCommitRequestPartition (..),
                                                      OffsetCommitResponsePartition (..),
                                                      OffsetFetchResponsePartition (..))

-- NOTE: All operations on the GroupMetadataManager are not concurrency-safe,
-- and the caller needs to ensure concurrency-safety on its own.
data GroupOffsetManager = forall os. OffsetStorage os => GroupOffsetManager
  { serverId      :: Int32
  , ldClient      :: S.LDClient
  , groupName     :: T.Text
  , offsetStorage :: os
  , offsetsCache  :: IORef (Map.Map TopicPartition Int64)
  , partitionsMap :: IORef (Map.Map TopicPartition Word64)
  }

-- FIXME: if we create a consumer group with groupName haven been used, call
-- mkCkpOffsetStorage with groupName may lead us to a un-clean ckp-store
mkGroupOffsetManager :: S.LDClient -> Int32 -> T.Text -> IO GroupOffsetManager
mkGroupOffsetManager ldClient serverId groupName = do
  offsetsCache  <- newIORef Map.empty
  partitionsMap <- newIORef Map.empty
  offsetStorage <- mkCkpOffsetStorage ldClient groupName
  return GroupOffsetManager{..}

loadOffsetsFromStorage :: GroupOffsetManager -> IO ()
loadOffsetsFromStorage GroupOffsetManager{..} = do
  Log.info $ "Consumer group " <> Log.build groupName <> " start load offsets from storage"
  start <- getTime Monotonic
  tpOffsets <- Map.map fromIntegral <$> loadOffsets offsetStorage groupName
  let totalPartitions = length tpOffsets
      logIds = S.fromList $ Map.keys tpOffsets
  topicNumRef <- newIORef 0
  tps <- getTopicPartitions logIds [] topicNumRef
  totalTopicNum <- readIORef topicNumRef
  let partitionMap = Map.fromList tps
      offsetsMap = Map.compose tpOffsets partitionMap
  Log.info $ "loadOffsets for group " <> Log.build groupName
          <> ", partitionMap: " <> Log.build (show partitionMap)
          <> ", offsetsMap: " <> Log.build (show offsetsMap)
  -- update offsetsCache
  modifyIORef' offsetsCache $ return offsetsMap
  -- update partitionsMap
  modifyIORef' partitionsMap $ return partitionMap
  end <- getTime Monotonic
  let msDuration = toNanoSecs (end `diffTimeSpec` start) `div` 1000000
  Log.info $ "Finish load offsets for consumer group " <> Log.build groupName
          <> ", total time " <> Log.build msDuration <> "ms"
          <> ", total nums of topics " <> Log.build totalTopicNum
          <> ", total nums of partitions " <> Log.build totalPartitions
 where
   getTopicPartitions :: Set Word64 -> [[(TopicPartition, S.C_LogID)]] -> IORef Int -> IO ([(TopicPartition, S.C_LogID)])
   getTopicPartitions lgs res topicNum
     | S.null lgs = return $ concat res
     | otherwise = do
         let lgId = S.elemAt 0 lgs
         LD.logIdHasGroup ldClient lgId >>= \case
          True -> do
             (streamId, _) <- S.getStreamIdFromLogId ldClient lgId
             modifyIORef' topicNum (+1)
             partitions <- V.toList <$> S.listStreamPartitionsOrdered ldClient streamId
             let topicName = T.pack $ S.showStreamName streamId
                 tpWithLogId = zipWith (\(_, logId) idx -> (mkTopicPartition topicName idx, logId)) partitions ([0..])
                 res' = tpWithLogId : res
                 -- remove partition ids from lgs because they all have same streamId
                 lgs' = lgs S.\\ S.fromList (map snd partitions)
             getTopicPartitions lgs' res' topicNum
          False -> do
            Log.warning $ "get log group from log id failed, skip this log id:" <> Log.build lgId
            getTopicPartitions (S.delete lgId lgs) res topicNum

storeOffsets
  :: GroupOffsetManager
  -> T.Text
  -> KaArray OffsetCommitRequestPartition
  -> IO (KaArray OffsetCommitResponsePartition)
storeOffsets gmm@GroupOffsetManager{..} topicName arrayOffsets = do
  let offsets = fromMaybe V.empty (unKaArray arrayOffsets)

  -- check if a TopicPartition that has an offset to be committed is contained in current
  -- consumer group's partitionsMap. If not, server will return a UNKNOWN_TOPIC_OR_PARTITION
  -- error, and that error will be convert to COORDINATOR_NOT_AVAILABLE error finally
  offsetsInfo <- getOffsetsInfo gmm topicName offsets
  Log.debug $ "get offsetsInfo for topic " <> Log.build topicName <> ": " <> Log.build (show offsetsInfo)

  -- write checkpoints
  let checkPoints = V.foldl' (\acc (_, logId, offset) -> Map.insert logId offset acc) Map.empty offsetsInfo
  commitOffsets offsetStorage groupName checkPoints
  Log.debug $ "consumer group " <> Log.build groupName <> " commit offsets {" <> Log.build (show checkPoints)
           <> "} for topic " <> Log.build topicName

  V.forM_ offsetsInfo $ \(tp, _, offset) -> do
    M.withLabel M.consumerGroupCommittedOffsets (groupName, topicName, T.pack . show $ tp.topicPartitionIdx) $
      flip M.setGauge (fromIntegral offset)

  -- update cache
  modifyIORef' offsetsCache $ \cache -> do
    let updates = V.foldl' (\acc (key, _, offset) -> Map.insert key (fromIntegral offset) acc) Map.empty offsetsInfo
    Map.union updates cache

  let suc = V.map (\(TopicPartition{topicPartitionIdx}, _, _) -> (topicPartitionIdx, K.NONE)) offsetsInfo
      res = V.map (\(partitionIndex, errorCode) -> OffsetCommitResponsePartition{..}) suc
  return KaArray {unKaArray = Just res}

getOffsetsInfo
  :: GroupOffsetManager
  -> T.Text
  -> V.Vector K.OffsetCommitRequestPartition
  -> IO (V.Vector (TopicPartition, Word64, Word64)) -- return [(topic-partition, logid, offset)]
getOffsetsInfo GroupOffsetManager{..} topicName requestOffsets = do
  V.forM requestOffsets $ \OffsetCommitRequestPartition{..} -> do
    let tp = mkTopicPartition topicName partitionIndex
    mp <- readIORef partitionsMap
    case Map.lookup tp mp of
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
            let partitionMap = Map.fromList . V.toList $ V.zipWith (\idx (_, lgId) -> ((mkTopicPartition topicName idx), lgId)) (V.fromList [0..]) partitions
                mp' = Map.union partitionMap mp
            writeIORef partitionsMap mp'
            return (tp, logId, fromIntegral committedOffset)
      Just logId -> return (tp, logId, fromIntegral committedOffset)

fetchOffsets
  :: GroupOffsetManager
  -> T.Text
  -> KaArray Int32
  -> IO (KaArray OffsetFetchResponsePartition)
fetchOffsets GroupOffsetManager{..} topicName partitions = do
  let partitions' = fromMaybe V.empty (unKaArray partitions)
  cache <- readIORef offsetsCache
  res <- traverse (getOffset cache) partitions'
  return $ KaArray {unKaArray = Just res}
 where
   getOffset cache partitionIdx = do
     let key = mkTopicPartition topicName partitionIdx
      in case Map.lookup key cache of
           Just offset -> return $ OffsetFetchResponsePartition
             { committedOffset = offset
             , metadata = Nothing
             , partitionIndex= partitionIdx
             , errorCode = K.NONE
             }
           Nothing -> return $ OffsetFetchResponsePartition
             { committedOffset = -1
             , metadata = Nothing
             , partitionIndex= partitionIdx
             -- TODO: check the error code here
             , errorCode = K.NONE
             }

fetchAllOffsets :: GroupOffsetManager -> IO (KaArray K.OffsetFetchResponseTopic)
fetchAllOffsets GroupOffsetManager{..} = do
  -- group offsets by TopicName
  cachedOffset <- Map.foldrWithKey foldF Map.empty <$> readIORef offsetsCache
  return . KaArray . Just . V.map makeTopic . V.fromList . Map.toList $ cachedOffset
  where makePartition partition offset = OffsetFetchResponsePartition
                   { committedOffset = offset
                   , metadata = Nothing
                   , partitionIndex=partition
                   , errorCode = K.NONE
                   }
        foldF tp offset = Map.insertWith (V.++) tp.topicName (V.singleton (makePartition tp.topicPartitionIdx offset))
        makeTopic (name, partitions) = K.OffsetFetchResponseTopic {partitions=KaArray (Just partitions), name=name}

nullOffsets :: GroupOffsetManager -> IO Bool
nullOffsets GroupOffsetManager{..} = do
  Map.null <$> readIORef offsetsCache

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
