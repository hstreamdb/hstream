{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE PatternSynonyms     #-}

module HStream.Kafka.Server.Handler.Offset
 ( handleOffsetCommit
 , handleOffsetFetch
 , handleListOffsets
 )
where

import           Data.Int                             (Int64)
import           Data.Maybe                           (fromMaybe)
import           Data.Text                            (Text)
import           Data.Vector                          (Vector)
import qualified Data.Vector                          as V

import           HStream.Kafka.Common.OffsetManager   (getLatestOffset,
                                                       getOffsetByTimestamp,
                                                       getOldestOffset)
import           HStream.Kafka.Common.Utils           (mapKaArray)
import qualified HStream.Kafka.Group.GroupCoordinator as GC
import           HStream.Kafka.Server.Types           (ServerContext (..))
import qualified HStream.Logger                       as Log
import qualified HStream.Store                        as S
import qualified Kafka.Protocol                       as K
import qualified Kafka.Protocol.Error                 as K
import qualified Kafka.Protocol.Service               as K

--------------------
-- 2: ListOffsets
--------------------
pattern LatestTimestamp :: Int64
pattern LatestTimestamp = (-1)

pattern EarliestTimestamp :: Int64
pattern EarliestTimestamp = (-2)

handleListOffsets
  :: ServerContext -> K.RequestContext -> K.ListOffsetsRequest -> IO K.ListOffsetsResponse
handleListOffsets sc reqCtx req
  | reqCtx.apiVersion >= 2 && req.isolationLevel /= 0 = do
    Log.warning $ "currently only support READ_UNCOMMITED(isolationLevel = 0) request."
    return $ mkErrResponse req
  | otherwise = listOffsets sc reqCtx req
 where
   mkErrResponse K.ListOffsetsRequest{..} =
     let topicsRsp = mapKaArray (\K.ListOffsetsTopic{..} ->
          let partitionsRsp = mapKaArray (\K.ListOffsetsPartition{..} ->
               K.ListOffsetsPartitionResponse
                  { offset = -1
                  , timestamp = -1
                  -- ^ FIXME: read record timestamp ?
                  , partitionIndex = partitionIndex
                  , errorCode = K.INVALID_REQUEST
                  , oldStyleOffsets = K.KaArray Nothing
                  }) partitions
           in K.ListOffsetsTopicResponse {partitions=partitionsRsp, name=name}
          ) topics
     in K.ListOffsetsResponse {topics=topicsRsp, throttleTimeMs=0}

listOffsets
  :: ServerContext -> K.RequestContext -> K.ListOffsetsRequest -> IO K.ListOffsetsResponse
listOffsets sc _ K.ListOffsetsRequest{..} = do
  case K.unKaArray topics of
    Nothing      -> undefined
    -- ^ check kafka
    Just topics' -> do
      res <- V.forM topics' $ \K.ListOffsetsTopic {..} -> do
               listOffsetTopicPartitions sc name (K.unKaArray partitions)
      return $ K.ListOffsetsResponse {topics = K.KaArray {unKaArray = Just res}, throttleTimeMs = 0}

listOffsetTopicPartitions
  :: ServerContext
  -> Text
  -> Maybe (Vector K.ListOffsetsPartition)
  -> IO K.ListOffsetsTopicResponse
listOffsetTopicPartitions _ topicName Nothing = do
  return $ K.ListOffsetsTopicResponse {partitions = K.KaArray {unKaArray = Nothing}, name = topicName}
listOffsetTopicPartitions ServerContext{..} topicName (Just offsetsPartitions) = do
  orderedParts <- S.listStreamPartitionsOrdered scLDClient (S.transToTopicStreamName topicName)
  res <- V.forM offsetsPartitions $ \K.ListOffsetsPartition{..} -> do
    -- TODO: handle Nothing
    let partition = orderedParts V.! (fromIntegral partitionIndex)
    offset <- getOffset (snd partition) timestamp
    return $ K.ListOffsetsPartitionResponse
              { offset = offset
              , timestamp = timestamp
              -- ^ FIXME: read record timestamp ?
              , partitionIndex = partitionIndex
              , errorCode = K.NONE
              , oldStyleOffsets = K.NonNullKaArray (V.singleton offset)
              }
  return $ K.ListOffsetsTopicResponse {partitions = K.KaArray {unKaArray = Just res}, name = topicName}
 where
   -- NOTE: The last offset of a partition is the offset of the upcoming
   -- message, i.e. the offset of the last available message + 1.
   getOffset logid LatestTimestamp =
     maybe 0 (+ 1) <$> getLatestOffset scOffsetManager logid
   getOffset logid EarliestTimestamp =
     fromMaybe 0 <$> getOldestOffset scOffsetManager logid
   -- Return the earliest offset whose timestamp is greater than or equal to
   -- the given timestamp.
   --
   -- TODO: actually, this is not supported currently.
   getOffset logid timestamp =
     fromMaybe (-1) <$> getOffsetByTimestamp scOffsetManager logid timestamp

--------------------
-- 8: OffsetCommit
--------------------
handleOffsetCommit
  :: ServerContext -> K.RequestContext -> K.OffsetCommitRequest -> IO K.OffsetCommitResponse
handleOffsetCommit ServerContext{..} _ req = do
  GC.commitOffsets scGroupCoordinator req

--------------------
-- 9: OffsetFetch
--------------------
handleOffsetFetch
  :: ServerContext -> K.RequestContext -> K.OffsetFetchRequest -> IO K.OffsetFetchResponse
handleOffsetFetch ServerContext{..} _ req = do
  GC.fetchOffsets scGroupCoordinator req
