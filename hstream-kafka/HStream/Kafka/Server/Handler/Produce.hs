module HStream.Kafka.Server.Handler.Produce
  ( handleProduce
  , handleInitProducerId
  ) where

import qualified Control.Concurrent.Async              as Async
import           Control.Exception
import           Control.Monad
import           Data.ByteString                       (ByteString)
import qualified Data.ByteString                       as BS
import           Data.Int
import           Data.Maybe                            (fromMaybe)
import           Data.Text                             (Text)
import qualified Data.Text                             as T
import qualified Data.Vector                           as V
import           Data.Word

import           HStream.Kafka.Common.Acl
import           HStream.Kafka.Common.Authorizer.Class
import qualified HStream.Kafka.Common.Metrics          as M
import qualified HStream.Kafka.Common.OffsetManager    as K
import qualified HStream.Kafka.Common.RecordFormat     as K
import           HStream.Kafka.Common.Resource
import           HStream.Kafka.Server.Types            (ServerContext (..))
import qualified HStream.Logger                        as Log
import qualified HStream.Store                         as S
import qualified HStream.Utils                         as U
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Error                  as K
import qualified Kafka.Protocol.Message                as K
import qualified Kafka.Protocol.Service                as K

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
handleProduce ServerContext{..} _reqCtx req = do
  -- TODO: handle request args: acks, timeoutMs
  let topicData = fromMaybe V.empty (K.unKaArray req.topicData)
  responses <- V.forM topicData $ \topic{- TopicProduceData -} -> do
    -- [ACL] authorization result for the **topic**
    isTopicAuthzed <-
      simpleAuthorize (toAuthorizableReqCtx _reqCtx) authorizer Res_TOPIC topic.name AclOp_WRITE

    -- A topic is a stream. Here we donot need to check the topic existence,
    -- because the metadata api already does(?)
    partitions <- S.listStreamPartitionsOrderedByName
                      scLDClient (S.transToTopicStreamName topic.name)
    let partitionData = fromMaybe V.empty (K.unKaArray topic.partitionData)
    -- TODO: limit total concurrencies ?
    let loopPart = if V.length partitionData > 1
                      then Async.forConcurrently
                      else V.forM
    partitionResponses <- loopPart partitionData $ \partition -> do
      let Just (_, logid) = partitions V.!? (fromIntegral partition.index) -- TODO: handle Nothing
      M.withLabel
        M.totalProduceRequest
        (topic.name, T.pack . show $ partition.index) $ \counter ->
          void $ M.addCounter counter 1
      let Just recordBytes = partition.recordBytes -- TODO: handle Nothing
      Log.debug1 $ "Try to append to logid " <> Log.build logid
                <> "(" <> Log.build partition.index <> ")"

      -- [ACL] Generate response by the authorization result of the **topic**
      case isTopicAuthzed of
        False -> pure $ K.PartitionProduceResponse
                  { index           = partition.index
                  , errorCode       = K.TOPIC_AUTHORIZATION_FAILED
                  , baseOffset      = -1
                  , logAppendTimeMs = -1
                  , logStartOffset  = -1
                  }
        True -> do
          -- TODO: PartitionProduceResponse.logAppendTimeMs
          --
          -- The timestamp returned by broker after appending the messages. If
          -- CreateTime is used for the topic, the timestamp will be -1.  If
          -- LogAppendTime is used for the topic, the timestamp will be the broker
          -- local time when the messages are appended.
          --
          -- Currently, only support LogAppendTime
          catches (do
            (appendCompTimestamp, offset) <-
              appendRecords True scLDClient scOffsetManager
                                 (topic.name, partition.index) logid recordBytes
            pure $ K.PartitionProduceResponse
              { index           = partition.index
              , errorCode       = K.NONE
              , baseOffset      = offset
              , logAppendTimeMs = appendCompTimestamp
              , logStartOffset  = (-1) -- TODO: getLogStartOffset
              })
            [ Handler (\(K.DecodeError (ec, msg))-> do
                Log.debug1 $ "Append DecodeError " <> Log.buildString' ec
                          <> ", " <> Log.buildString' msg
                pure $ K.PartitionProduceResponse
                  { index           = partition.index
                  , errorCode       = ec
                  , baseOffset      = (-1)
                  , logAppendTimeMs = (-1)
                  , logStartOffset  = (-1)
                  })
            ]

    pure $ K.TopicProduceResponse topic.name (K.KaArray $ Just partitionResponses)

  pure $ K.ProduceResponse (K.KaArray $ Just responses) 0{- TODO: throttleTimeMs -}

-- TODO
handleInitProducerId
  :: ServerContext
  -> K.RequestContext
  -> K.InitProducerIdRequest
  -> IO K.InitProducerIdResponse
handleInitProducerId _ _ _ = do
  Log.warning "InitProducerId is not implemented"
  pure $ K.InitProducerIdResponse
    { throttleTimeMs = 0
    , errorCode = K.NONE
    , producerId = 0
    , producerEpoch = 0
    }

-------------------------------------------------------------------------------

appendRecords
  :: Bool
  -> S.LDClient
  -> K.OffsetManager
  -> (Text, Int32)
  -> Word64
  -> ByteString
  -> IO (Int64, Int64)  -- ^ Return (logAppendTimeMs, baseOffset)
appendRecords shouldValidateCrc ldclient om (streamName, partition) logid bs = do
  batch <- K.decodeRecordBatch shouldValidateCrc bs
  let batchLength = batch.recordsCount
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
        appendKey = U.intToCBytesWithPadding o
        appendAttrs = Just [(S.KeyTypeFindKey, appendKey)]

    K.unsafeUpdateRecordBatchBaseOffset bs (+ startOffset)

    -- FIXME unlikely overflow: convert batchLength from Int to Int32
    let storedRecord = K.runPut $ K.RecordFormat 0{- version -}
                                                 o
                                                 (fromIntegral batchLength)
                                                 (K.CompactBytes bs)
    Log.debug1 $ "Append key " <> Log.buildString' appendKey
              <> ", write offset " <> Log.build o
              <> ", batch length " <> Log.build batchLength
    r <- M.observeWithLabel M.topicWriteStoreLatency streamName $
           S.appendCompressedBS ldclient logid storedRecord S.CompressionNone
                                appendAttrs
    let !partLabel = (streamName, T.pack . show $ partition)
    M.withLabel M.topicTotalAppendBytes partLabel $ \counter ->
      void $ M.addCounter counter (fromIntegral $ BS.length storedRecord)
    M.withLabel M.topicTotalAppendMessages partLabel $ \counter ->
      void $ M.addCounter counter (fromIntegral batchLength)
    Log.debug1 $ "Append done " <> Log.build r.appendCompLogID
              <> ", lsn: " <> Log.build r.appendCompLSN
              <> ", start offset: " <> Log.build startOffset
    pure (r.appendCompTimestamp, startOffset)

-- TODO: performance improvements
--
-- For each append request after version 5, we need to read the oldest
-- offset of the log. This will cause critical performance problems.
--
--logStartOffset <-
--  if reqCtx.apiVersion >= 5
--     then do m_logStartOffset <- K.getOldestOffset scOffsetManager logid
--             case m_logStartOffset of
--               Just logStartOffset -> pure logStartOffset
--               Nothing -> do
--                 Log.fatal $ "Cannot get log start offset for logid "
--                          <> Log.build logid
--                 pure (-1)
--     else pure (-1)
getLogStartOffset :: IO Int64
getLogStartOffset = pure (-1)
