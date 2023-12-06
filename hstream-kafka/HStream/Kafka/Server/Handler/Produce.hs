module HStream.Kafka.Server.Handler.Produce
  ( handleProduce
  ) where

import qualified Control.Concurrent.Async           as Async
import           Control.Monad
import           Data.ByteString                    (ByteString)
import qualified Data.ByteString                    as BS
import           Data.Int
import           Data.Maybe                         (fromMaybe)
import           Data.Text                          (Text)
import qualified Data.Text                          as T
import qualified Data.Vector                        as V
import           Data.Word

import qualified HStream.Kafka.Common.OffsetManager as K
import qualified HStream.Kafka.Common.RecordFormat  as K
import           HStream.Kafka.Common.Utils         (observeWithLabel)
import           HStream.Kafka.Metrics.ProduceStats (appendLatencySnd,
                                                     topicTotalAppendBytes,
                                                     topicTotalAppendMessages,
                                                     totalProduceRequest)
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import qualified HStream.Store                      as S
import qualified HStream.Utils                      as U
import qualified Kafka.Protocol.Encoding            as K
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K
import qualified Prometheus                         as P

-- acks: (FIXME: Currently we only support -1)
--   0: The server will not send any response(this is the only case where the
--      server will not reply to a request).
--
--   1: The server will wait the data is written to the local log before
--      sending a response. Broker will respond without awaiting full
--      acknowledgement from all followers. In this case should the leader fail
--      immediately after acknowledging the record but before the followers
--      have replicated it then the record will be lost.
--
--   -1: Wait for the full set of in-sync replicas to write the record. This
--       guarantees that the record will not be lost as long as at least one
--       in-sync replica remains alive. This is the strongest available
--       guarantee.
handleProduce
  :: ServerContext
  -> K.RequestContext
  -> K.ProduceRequest
  -> IO K.ProduceResponse
handleProduce ServerContext{..} _ req = do
  -- TODO: handle request args: acks, timeoutMs
  let topicData = fromMaybe V.empty (K.unKaArray req.topicData)

  responses <- V.forM topicData $ \topic{- TopicProduceData -} -> do
    -- A topic is a stream. Here we donot need to check the topic existence,
    -- because the metadata api does(?)
    partitions <- S.listStreamPartitionsOrdered
                    scLDClient (S.transToTopicStreamName topic.name)
    let partitionData = fromMaybe V.empty (K.unKaArray topic.partitionData)
    -- TODO: limit total concurrencies ?
    let loopPart = if V.length partitionData > 1
                      then Async.forConcurrently
                      else V.forM
    partitionResponses <- loopPart partitionData $ \partition -> do
      let Just (_, logid) = partitions V.!? (fromIntegral partition.index) -- TODO: handle Nothing
      P.withLabel totalProduceRequest (topic.name, T.pack . show $ partition.index) $ \counter -> void $ P.addCounter counter 1
      let Just recordBytes = partition.recordBytes -- TODO: handle Nothing
      Log.debug1 $ "Try to append to logid " <> Log.build logid
                <> "(" <> Log.build partition.index <> ")"

      -- Wirte appends
      (S.AppendCompletion{..}, offset) <-
        appendRecords True scLDClient scOffsetManager (topic.name, partition.index) logid recordBytes

      Log.debug1 $ "Append done " <> Log.build appendCompLogID
                <> ", lsn: " <> Log.build appendCompLSN
                <> ", start offset: " <> Log.build offset

      -- TODO: logAppendTimeMs, only support LogAppendTime now
      pure $ K.PartitionProduceResponse partition.index K.NONE offset appendCompTimestamp

    pure $ K.TopicProduceResponse topic.name (K.KaArray $ Just partitionResponses)

  pure $ K.ProduceResponse (K.KaArray $ Just responses) 0{- TODO: throttleTimeMs -}

-------------------------------------------------------------------------------

appendRecords
  :: Bool
  -> S.LDClient
  -> K.OffsetManager
  -> (Text, Int32)
  -> Word64
  -> ByteString
  -> IO (S.AppendCompletion, Int64)
appendRecords shouldValidateCrc ldclient om (streamName, partition) logid bs = do
  (records, batchLength) <- K.decodeBatchRecords' shouldValidateCrc bs
  when (batchLength < 1) $ error "Invalid batch length"

  -- Offset wroten into storage is the max key in the batch, but return the min
  -- key to the client. This is because the usage of findKey.
  --
  -- Suppose we have three batched records:
  --
  -- record0, record1, record2
  -- [0,      1-5,     6-10]
  --
  -- if write with key [0, 1, 6]
  --
  -- findKey 0 -> (lo: None, hi: record0); Expect: record0
  -- findKey 1 -> (lo: record0, hi: record1); Expect: record1
  -- findKey 2 -> (lo: record1, hi: record2); Expect: record1
  -- ...
  -- findKey 6 -> (lo: record1, hi: record2); Expect: record2
  --
  -- if write with key [0, 5, 10]
  --
  -- we can always get the correct result with the hi.
  K.withOffsetN om logid (fromIntegral batchLength) $ \o -> do
    let startOffset = o + 1 - fromIntegral batchLength
        records' = K.modifyBatchRecordsOffset (+ startOffset) records
    let appendKey = U.intToCBytesWithPadding o
        appendAttrs = Just [(S.KeyTypeFindKey, appendKey)]
        storedBs = K.encodeBatchRecords records'
        -- FIXME unlikely overflow: convert batchLength from Int to Int32
        storedRecord = K.runPut $ K.RecordFormat 0{- version -}
                                                 o (fromIntegral batchLength)
                                                 (K.CompactBytes storedBs)
    Log.debug1 $ "Append key " <> Log.buildString' appendKey
    r <- observeWithLabel appendLatencySnd streamName $
           S.appendCompressedBS ldclient logid storedRecord S.CompressionNone
                                appendAttrs
    let !partLabel = (streamName, T.pack . show $ partition)
    P.withLabel topicTotalAppendBytes partLabel $ \counter ->
      void $ P.addCounter counter (fromIntegral $ BS.length storedRecord)
    P.withLabel topicTotalAppendMessages partLabel $ \counter ->
      void $ P.addCounter counter (fromIntegral batchLength)
    pure (r, startOffset)
