module HStream.Kafka.Server.Handler.Offset
 ( handleOffsetCommitV0
 , handleOffsetFetchV0
 , handleListOffsetsV0
 )
where

import           Data.Int                             (Int64)
import           Data.Text                            (Text)
import           Data.Vector                          (Vector)
import qualified Data.Vector                          as V
import           HStream.Kafka.Common.OffsetManager   (getLatestOffset,
                                                       getOffsetByTimestamp,
                                                       getOldestOffset)
import qualified HStream.Kafka.Group.GroupCoordinator as GC
import           HStream.Kafka.Server.Types           (ServerContext (..))
import qualified HStream.Store                        as S
import qualified Kafka.Protocol                       as K
import qualified Kafka.Protocol.Error                 as K
import qualified Kafka.Protocol.Service               as K

--------------------
-- 2: ListOffsets
--------------------
handleListOffsetsV0
  :: ServerContext -> K.RequestContext -> K.ListOffsetsRequestV0 -> IO K.ListOffsetsResponseV0
handleListOffsetsV0 sc _ K.ListOffsetsRequestV0{..} = do
  case K.unKaArray topics of
    Nothing      -> undefined
    Just topics' -> do
      res <- V.forM topics' $ \K.ListOffsetsTopicV0 {..} -> do
               listOffsetTopicPartitions sc name (K.unKaArray partitions)
      return $ K.ListOffsetsResponseV0 {topics = K.KaArray {unKaArray = Just res}}

latestTimestamp :: Int64
latestTimestamp = -1

earliestTimestamp :: Int64
earliestTimestamp = -2

listOffsetTopicPartitions :: ServerContext -> Text -> Maybe (Vector K.ListOffsetsPartitionV0) -> IO K.ListOffsetsTopicResponseV0
listOffsetTopicPartitions _ topicName Nothing = do
  return $ K.ListOffsetsTopicResponseV0 {partitions = K.KaArray {unKaArray = Nothing}, name = topicName}
listOffsetTopicPartitions ServerContext{..} topicName (Just offsetsPartitions) = do
  orderedParts <- S.listStreamPartitionsOrdered scLDClient (S.transToTopicStreamName topicName)
  res <- V.forM offsetsPartitions $ \K.ListOffsetsPartitionV0{..} -> do
    let logid = orderedParts V.!? (fromIntegral partitionIndex)
    offset <- getOffset logid timestamp
    return $ K.ListOffsetsPartitionResponseV0
              { oldStyleOffsets = K.KaArray {unKaArray = Just $ maybe V.empty V.singleton offset}
              -- { oldStyleOffsets = K.KaArray {unKaArray = V.singleton <$> offset}
              , partitionIndex = partitionIndex
              , errorCode = K.NONE
              }
  return $ K.ListOffsetsTopicResponseV0 {partitions = K.KaArray {unKaArray = Just res}, name = topicName}
 where
   getOffset Nothing _ = undefined
   getOffset (Just (_, logid)) timestamp
     | timestamp == latestTimestamp   = getLatestOffset scOffsetManager logid
     | timestamp == earliestTimestamp = getOldestOffset scOffsetManager logid
     | otherwise                      = getOffsetByTimestamp scOffsetManager logid timestamp

--------------------
-- 8: OffsetCommit
--------------------
handleOffsetCommitV0
  :: ServerContext -> K.RequestContext -> K.OffsetCommitRequestV0 -> IO K.OffsetCommitResponseV0
handleOffsetCommitV0 ServerContext{..} _ req = do
  GC.commitOffsets scGroupCoordinator req

--------------------
-- 9: OffsetFetch
--------------------
handleOffsetFetchV0
  :: ServerContext -> K.RequestContext -> K.OffsetFetchRequestV0 -> IO K.OffsetFetchResponseV0
handleOffsetFetchV0 ServerContext{..} _ req = do
  GC.fetchOffsets scGroupCoordinator req
