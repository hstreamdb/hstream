{-# LANGUAGE BangPatterns #-}

module HStream.Kafka.Server.Handler.Consume
  ( handleFetchV2
  ) where

import           Control.Monad
import qualified Data.ByteString                    as BS
import           Data.Int
import qualified Data.List                          as L
import           Data.Maybe
import qualified Data.Vector                        as V

import qualified HStream.Kafka.Common.OffsetManager as K
import qualified HStream.Kafka.Common.RecordFormat  as K
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import qualified HStream.Store                      as S
import qualified HStream.Utils                      as U
import qualified Kafka.Protocol.Encoding            as K
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K

--------------------
-- 1: Fetch
--------------------
handleFetchV2
  :: ServerContext -> K.RequestContext -> K.FetchRequestV2 -> IO K.FetchResponseV2
handleFetchV2 ServerContext{..} _ K.FetchRequestV2{..} = case topics of
  K.KaArray Nothing -> undefined
  K.KaArray (Just topicReqs_) -> do
    (_,_,_,resps) <-
      foldM (\( acc_isFirstTopic
              , acc_totalMaxBytes_m
              , acc_timeLeft
              , acc_resps
              ) topicReq ->
               -- Timeout, do not fetch any new data
               if acc_timeLeft < 0 then
                 return (acc_isFirstTopic, acc_totalMaxBytes_m, acc_timeLeft, acc_resps)
               else do
                 -- Read one topic, then update total bytes left and time left
                 -- Note: it is important to know if this is the first topic!
                 (totalMaxBytes_m', timeLeftMs', resp) <-
                   readSingleTopic scLDClient scOffsetManager topicReq
                                   acc_totalMaxBytes_m acc_timeLeft
                                   acc_isFirstTopic
                 return ( -- `isJust totalMaxBytes_m' && totalMaxBytes_m' == acc_totalMaxBytes_m` means
                          -- there is nothing read from this topic.
                          if isJust totalMaxBytes_m' && totalMaxBytes_m' == acc_totalMaxBytes_m
                             then acc_isFirstTopic else False
                        , totalMaxBytes_m'
                        , timeLeftMs'
                        , acc_resps ++ [resp]
                        )
            ) (True, Nothing, maxWaitMs, []) topicReqs_
    return $ K.FetchResponseV2 0{- TODO: throttleTimeMs -} (K.KaArray $ Just $ V.fromList resps)

-------------------------------------------------------------------------------

readSingleTopic
  :: S.LDClient
  -> K.OffsetManager
  -> K.FetchTopicV2
  -> Maybe Int32 -- limit: total bytes left now
  -> Int32       -- limit: time left now
  -> Bool        -- is this the first topic? (if so, omit the bytes limit of this )
  -> IO (Maybe Int32, Int32, K.FetchableTopicResponseV2) -- (total bytes left, time left, response of this topic)
readSingleTopic ldclient om K.FetchTopicV2{..} totalMaxBytes_m timeLeftMs isFirstTopic = case partitions of
  K.KaArray Nothing    -> return (totalMaxBytes_m, timeLeftMs, K.FetchableTopicResponseV2 topic (K.KaArray Nothing))
  K.KaArray (Just parts) -> do
    orderedParts <- S.listStreamPartitionsOrdered ldclient (S.transToTopicStreamName topic)
    -- FIXME: is it proper to use one reader for all partitions of a topic?
    reader <- S.newLDReader ldclient 1 (Just 1)
    (_,totalMaxBytes_m', timeLeftMs', resps) <-
      foldM (\( acc_isFirstPartition
              , acc_totalMaxBytes_m
              , acc_timeLeft
              , acc_resps
              ) K.FetchPartitionV2{..} ->
               if acc_timeLeft <= 0 then
                 return (acc_isFirstPartition, acc_totalMaxBytes_m, acc_timeLeft, acc_resps)
               else do
                 let (_, logId) = orderedParts V.! fromIntegral partition
                 elsn <- getPartitionLsn ldclient om logId partition fetchOffset
                 case elsn of
                   Left resp ->
                     pure (acc_isFirstPartition, acc_totalMaxBytes_m, acc_timeLeft, acc_resps ++ [resp])
                   Right (startlsn, endlsn, hioffset) -> do
                     (len, timeLeftMs', resp) <-
                       readSinglePartition reader
                                           logId (startlsn, endlsn)
                                           partition
                                           fetchOffset
                                           hioffset
                                           acc_totalMaxBytes_m
                                           partitionMaxBytes
                                           acc_timeLeft
                                           acc_isFirstPartition
                                           isFirstTopic
                     return ( if len > 0 then False else acc_isFirstPartition
                            , fmap (\x -> x - len) acc_totalMaxBytes_m
                            , timeLeftMs'
                            , acc_resps ++ [resp]
                            )
            ) (True, totalMaxBytes_m, timeLeftMs, []) parts -- !!! FIXME: update time left!!!
    return ( totalMaxBytes_m'
           , timeLeftMs'
           , K.FetchableTopicResponseV2 topic (K.KaArray $ Just $ V.fromList resps)
           )

-- Return tuple of (startLsn, endLsn, highwaterOffset)
getPartitionLsn
  :: S.LDClient
  -> K.OffsetManager
  -> S.C_LogID -> Int32
  -> Int64        -- ^ kafka start offset
  -> IO (Either K.PartitionDataV2 (S.LSN, S.LSN, Int64))
getPartitionLsn ldclient om logid partition offset = do
  m <- K.getLatestOffsetWithLsn om logid
  case m of
    Just (latestOffset, endLsn) -> do
      if | offset < latestOffset -> do
             let key = U.int2cbytes offset
             (_, startLsn) <- S.findKey ldclient logid key S.FindKeyStrict
             pure $ Right (startLsn, endLsn, latestOffset + 1)
         | offset == latestOffset ->
             pure $ Right (endLsn, endLsn, latestOffset + 1)
         | offset == (latestOffset + 1) ->
             pure $ Left $ K.PartitionDataV2 partition K.NONE (latestOffset + 1) (Just "")
         | offset > (latestOffset + 1) ->
             pure $ Left $ errorPartitionResponseV2 partition K.OFFSET_OUT_OF_RANGE
         -- ghc is not smart enough to detact my partten matching is complete
         | otherwise -> error "This should not be reached (getPartitionLsn)"
    Nothing -> do
      Log.debug $ "Partition " <> Log.build logid <> " is empty"
      if offset == 0
         then pure $ Left $ K.PartitionDataV2 partition K.NONE 0 (Just "")
         else pure $ Left $ errorPartitionResponseV2 partition K.OFFSET_OUT_OF_RANGE

readSinglePartition
  :: S.LDReader   -- the logdevice reader of this **topic**, but only one logId is read at the same time
  -> S.C_LogID    -- logId of this partition
  -> (S.LSN, S.LSN)  -- ^ (start_lsn, end_lsn)
  -> Int32        -- partition index: 0, 1, ...
  -> Int64        -- ^ kafka read start offset
  -> Int64        -- ^ kafka highwater offset
  -> Maybe Int32  -- limit: total bytes left now, `Nothing` means no limit
  -> Int32        -- limit: bytes left of this partition now
  -> Int32        -- limit: time left now
  -> Bool         -- is this the first partition? (if so, return the data even if it exceeds the limit)
  -> Bool         -- is this the first topic? (if so and this is also the first partition, return the data even if it exceeds the limit)
  -> IO (Int32, Int32, K.PartitionDataV2) -- (the number of bytes read, time left, response of this partition)
readSinglePartition reader logId (startLSN, endLSN) partitionIndex
                    offset highwaterOffset totalMaxBytes_m partitionMaxBytes
                    timeLeftMs isFirstPartition isFirstTopic = do
  S.readerSetTimeout reader timeLeftMs
  S.readerSetWaitOnlyWhenNoData reader
  S.readerStartReading reader logId startLSN endLSN -- FIXME: what should the end be? Is tailLSN proper?
  (timeLeftMs', acc) <- go [] timeLeftMs partitionMaxBytes totalMaxBytes_m -- !!! FIXME: update time left!!!
  isReading <- S.readerIsReading reader logId
  when isReading $
    S.readerStopReading reader logId -- FIXME: does `readerStopReading` actually stop the reading of the logId?
  let returnBytes = BS.concat acc    -- FIXME: we just concat the payload bytes of each record, is this proper?
      returnBytesLen = BS.length returnBytes -- FIXME: is the length correct?
  let resp = K.PartitionDataV2 partitionIndex K.NONE highwaterOffset (Just returnBytes) -- FIXME: exceptions?
  return (fromIntegral returnBytesLen, timeLeftMs', resp) -- !!! FIXME: update time left!!!
  where
    -- Note: `go` reads records from a logId **one by one** until the time limit or bytes limit is reached.
    go :: [BS.ByteString] -- accumulated bytes list
       -> Int32           -- time left now
       -> Int32           -- bytes left of this partition now
       -> Maybe Int32     -- total bytes left now, `Nothing` means no limit
       -> IO (Int32, [BS.ByteString]) -- (time left, accumulated bytes list)
    -- !!! FIXME: update time left!!!
    -- !!! FIXME: stats!!!
    go accBytesList timeLeft partitionBytesLeft totalBytesLeft_m
      | timeLeft <= 0 = return (timeLeft, accBytesList)
      | otherwise = do
          records <- S.readerRead reader 1
          case L.null records of
            True  -> return (timeLeft, accBytesList)
            False -> do
              let record = L.head records
              K.RecordFormat{ recordBytes = K.CompactBytes bytesOnDisk
                            , offset = absEndOffset
                            , batchLength = batchLength
                            } <- K.runGet (S.recordPayload record)
              let absStartOffset = absEndOffset + 1 - fromIntegral batchLength
              recordBytes <-
                if (absStartOffset < offset)
                   -- TODO improvements: actually only the first bathch need to to this seek
                   then K.seekBatch (fromIntegral $ offset - absStartOffset) bytesOnDisk
                   else pure bytesOnDisk
              let recordBytesLen = BS.length recordBytes

              -- Note: Record size > global max bytes, omit the limit
              --       if this is the first (non-empty) partition among
              --       all topics.
              if isJust totalBytesLeft_m && recordBytesLen > fromIntegral (fromJust totalBytesLeft_m) then
                if isFirstTopic && isFirstPartition && L.null accBytesList then
                  return (timeLeft, [recordBytes]) else
                  return (timeLeft, accBytesList)
              -- Note: Record size > partition max bytes, omit the limit
              --       if this is the first partition of this topic.
              else if recordBytesLen > fromIntegral partitionBytesLeft then
                if isFirstPartition && L.null accBytesList then
                  return (timeLeft, [recordBytes]) else
                  return (timeLeft, accBytesList)
              -- Note: update time limit, partition bytes limit and total bytes limit
              else go (accBytesList ++ [recordBytes])
                      timeLeft
                      (partitionBytesLeft - fromIntegral recordBytesLen)
                      (fmap (\x -> x - fromIntegral recordBytesLen) totalBytesLeft_m)

-------------------------------------------------------------------------------

errorPartitionResponseV2 :: Int32 -> K.ErrorCode -> K.PartitionDataV2
errorPartitionResponseV2 partitionIndex ec =
  K.PartitionDataV2 partitionIndex ec (-1) (Just "")
{-# INLINE errorPartitionResponseV2 #-}
