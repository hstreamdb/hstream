module HStream.Kafka.Server.Handler.Produce
  ( handleProduceV2
  ) where

import           Control.Monad
import           Data.ByteString                    (ByteString)
import           Data.Int
import           Data.Maybe                         (fromMaybe)
import qualified Data.Vector                        as V
import           Data.Word

import qualified HStream.Kafka.Common.OffsetManager as K
import qualified HStream.Kafka.Common.RecordFormat  as K
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Store                      as S
import qualified HStream.Utils                      as U
import qualified Kafka.Protocol.Encoding            as K
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K

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
handleProduceV2
  :: ServerContext
  -> K.RequestContext
  -> K.ProduceRequestV2
  -> IO K.ProduceResponseV2
handleProduceV2 ServerContext{..} _ K.ProduceRequestV0{..} = do
  -- TODO: handle request args: acks, timeoutMs

  let topicData' = fromMaybe V.empty (K.unKaArray topicData)
  responses <- V.forM topicData' $ \K.TopicProduceDataV0{..} -> do
    -- A topic is a stream. Here we donot need to check the topic existence,
    -- because the metadata api does(?)
    let topic = S.transToTopicStreamName name
    partitions <- S.listStreamPartitionsOrdered scLDClient topic
    let partitionData' = fromMaybe V.empty (K.unKaArray partitionData)
    partitionResponses <- V.forM partitionData' $ \K.PartitionProduceDataV0{..} -> do
      let Just (_, logid) = partitions V.!? (fromIntegral index) -- TODO: handle Nothing
      let Just recordBytes' = recordBytes -- TODO: handle Nothing

      -- Wirte appends
      (S.AppendCompletion{..}, offset) <-
        appendRecords True scLDClient scOffsetManager logid recordBytes'

      -- TODO: logAppendTimeMs, only support LogAppendTime now
      pure $ K.PartitionProduceResponseV2 index K.NONE offset appendCompTimestamp

    pure $ K.TopicProduceResponseV2 name (K.KaArray $ Just partitionResponses)

  pure $ K.ProduceResponseV2 (K.KaArray $ Just responses) 0{- TODO: throttleTimeMs -}

-------------------------------------------------------------------------------

-- TODO
appendRecords
  :: Bool
  -> S.LDClient
  -> K.OffsetManager
  -> Word64
  -> ByteString
  -> IO (S.AppendCompletion, Int64)
appendRecords shouldValidateCrc ldclient om logid bs = do
  records <- K.decodeBatchRecords shouldValidateCrc bs
  let batchLength = V.length records
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
    let appendKey = U.int2cbytes o
        appendAttrs = Just [(S.KeyTypeFindKey, appendKey)]
        -- FIXME unlikely overflow: convert batchLength from Int to Int32
        storedRecord = K.RecordFormat o (fromIntegral batchLength) (K.CompactBytes bs)
    r <- S.appendCompressedBS ldclient
                              logid (K.runPut storedRecord)
                              S.CompressionNone appendAttrs
    pure (r, o - fromIntegral batchLength + 1)
