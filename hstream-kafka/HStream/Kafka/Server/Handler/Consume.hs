{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Server.Handler.Consume
  ( handleFetch
  ) where

import           Control.Exception
import           Control.Monad
import           Data.ByteString                    (ByteString)
import qualified Data.ByteString                    as BS
import qualified Data.ByteString.Builder            as BB
import           Data.Either                        (isRight)
import           Data.Int
import           Data.Maybe
import qualified Data.Text                          as T
import qualified Data.Vector                        as V
import qualified Data.Vector.Hashtables             as HT
import qualified Data.Vector.Storable               as VS
import           GHC.Stack                          (HasCallStack)
import qualified Prometheus                         as P

import qualified HStream.Base.Growing               as GV
import qualified HStream.Kafka.Common.OffsetManager as K
import qualified HStream.Kafka.Common.RecordFormat  as K
import           HStream.Kafka.Metrics.ConsumeStats (readLatencySnd,
                                                     topicTotalSendBytes,
                                                     topicTotalSendMessages,
                                                     totalConsumeRequest)
import           HStream.Kafka.Server.Config        (ServerOpts (..),
                                                     StorageOptions (..))
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import qualified HStream.Store                      as S
import qualified HStream.Utils                      as U
import qualified Kafka.Protocol.Encoding            as K
import qualified Kafka.Protocol.Error               as K
import qualified Kafka.Protocol.Message             as K
import qualified Kafka.Protocol.Service             as K

-------------------------------------------------------------------------------

type RecordTable =
  HT.Dictionary (HT.PrimState IO)
                VS.MVector
                S.C_LogID
                V.MVector
                (GV.Growing V.Vector GV.RealWorld K.RecordFormat)

-- NOTE: this behaviour is not the same as kafka broker
handleFetch
  :: HasCallStack
  => ServerContext
  -> K.RequestContext -> K.FetchRequest -> IO K.FetchResponse
handleFetch ServerContext{..} _ r = K.catchFetchResponseEx $ do
  -- kafka broker just throw java.lang.RuntimeException if topics is null, here
  -- we do the same.
  let K.NonNullKaArray topicReqs = r.topics
  topics <- V.forM topicReqs $ \topic{- K.FetchTopic -} -> do
    orderedParts <- S.listStreamPartitionsOrdered scLDClient
                      (S.transToTopicStreamName topic.topic)
    let K.NonNullKaArray partitionReqs = topic.partitions
    ps <- V.forM partitionReqs $ \p{- K.FetchPartition -} -> do
      P.withLabel totalConsumeRequest (topic.topic, T.pack . show $ p.partition) $
        \counter -> void $ P.addCounter counter 1
      let m_logid = orderedParts V.!? fromIntegral p.partition
      case m_logid of
        Nothing -> do
          let elsn = errorPartitionResponse p.partition K.UNKNOWN_TOPIC_OR_PARTITION
          -- Actually, the logid should be Nothing but 0, however, we won't
          -- use it, so just set it to 0
          pure (0, Left elsn, p)
        Just (_, logid) -> do
          elsn <- getPartitionLsn scLDClient scOffsetManager logid p.partition
                                  p.fetchOffset
          pure (logid, elsn, p)
    pure (topic.topic, ps)

  let numOfReads = V.sum $
        V.map (V.length . V.filter (\(_, x, _) -> isRight x) . snd) topics
  when (numOfReads < 1) $ do
    respTopics <- V.forM topics $ \(topic, partitions) -> do
      respPartitionDatas <- V.forM partitions $ \(_, elsn, _) -> do
        case elsn of
          Left pd -> pure pd
          Right _ -> error "LogicError: this should not be right"
      pure $ K.FetchableTopicResponse topic (K.NonNullKaArray respPartitionDatas)
    let resp = K.FetchResponse (K.NonNullKaArray respTopics) 0{- TODO: throttleTimeMs -}
    throwIO $ K.FetchResponseEx resp

  -- Start reading
  --
  -- We use a per-connection reader(fetchReader) to read.
  V.forM_ topics $ \(_, partitions) -> do
    V.forM_ partitions $ \(logid, elsn, _) -> do
      case elsn of
        Left _ -> pure ()
        Right (startlsn, _, _) -> do
          Log.debug1 $ "start reading log "
                    <> Log.build logid <> " from " <> Log.build startlsn
          S.readerStartReading fetchReader logid startlsn S.LSN_MAX

  -- Read records from storage
  --
  -- TODO:
  -- - dynamically change reader settings according to the client request
  -- (e.g. maxWaitMs, minBytes...)
  -- - handle maxBytes
  --
  -- FIXME: Do not setWaitOnlyWhenNoData if you are mostly focusing on
  -- throughput
  --
  -- Mode1
  records <- readMode1 fetchReader

  -- Process read records
  -- TODO: what if client send two same topic but with different partitions?
  --
  -- {logid: [RecordFormat]}
  readRecords <- HT.initialize numOfReads :: IO RecordTable
  forM_ records $ \record -> do
    recordFormat <- K.runGet @K.RecordFormat record.recordPayload
    let logid = record.recordAttr.recordAttrLogID
    v <- maybe GV.new pure =<< (HT.lookup readRecords logid)
    v' <- GV.append v recordFormat
    HT.insert readRecords logid v'

  -- Generate response
  respTopics <- V.forM topics $ \(topic, partitions) -> do
    respPartitionDatas <- V.forM partitions $ \(logid, elsn, p) -> do
      case elsn of
        Left pd -> pure pd
        Right (_startlsn, _endlsn, hioffset) -> do
          mgv <- HT.lookup readRecords logid
          case mgv of
            Nothing ->
              pure $ K.PartitionData p.partition K.NONE hioffset (Just "")
                                     (-1){- TODO: lastStableOffset -}
                                     (K.NonNullKaArray V.empty){- TODO: abortedTransactions -}
            Just gv -> do
              v <- GV.unsafeFreeze gv
              bs <- encodePartition p v

              let partLabel = (topic, T.pack . show $ p.partition)
              P.withLabel topicTotalSendBytes partLabel $ \counter -> void $
                P.addCounter counter (fromIntegral $ BS.length bs)
              P.withLabel topicTotalSendMessages partLabel $ \counter -> void $ do
                let totalRecords = V.sum $ V.map (\K.RecordFormat{..} -> batchLength) v
                P.addCounter counter (fromIntegral totalRecords)

              pure $ K.PartitionData p.partition K.NONE hioffset (Just bs)
                                     (-1){- TODO: lastStableOffset -}
                                     (K.NonNullKaArray V.empty){- TODO: abortedTransactions -}
    pure $ K.FetchableTopicResponse topic (K.NonNullKaArray respPartitionDatas)
  pure $ K.FetchResponse (K.NonNullKaArray respTopics) 0{- TODO: throttleTimeMs -}

  where
    readMode0 :: S.LDReader -> IO [S.DataRecord ByteString]
    readMode0 reader = do
      if r.minBytes <= 0 || r.maxWaitMs <= 0
         then S.readerSetTimeout reader 0  -- nonblocking
         else S.readerSetTimeout reader r.maxWaitMs
      S.readerSetWaitOnlyWhenNoData reader
      (_, records) <- foldWhileM (0, []) $ \(size, acc) -> do
        rs <- P.observeDuration readLatencySnd $ S.readerRead reader 100
        if null rs
           then pure ((size, acc), False)
           else do let size' = size + sum (map (K.recordBytesSize . (.recordPayload)) rs)
                       acc' = acc <> rs
                   if size' >= fromIntegral r.minBytes
                      then pure ((size', acc'), False)
                      else pure ((size', acc'), True)
      pure records

    readMode1 :: S.LDReader -> IO [S.DataRecord ByteString]
    readMode1 reader = do
      let storageOpts = serverOpts._storage
          defTimeout = fromIntegral storageOpts.fetchReaderTimeout

      if r.minBytes <= 0 || r.maxWaitMs <= 0 -- respond immediately
         then do S.readerSetTimeout reader 0  -- nonblocking
                 S.readerRead reader storageOpts.fetchMaxLen
         else
           if r.maxWaitMs > defTimeout
              then do
                S.readerSetTimeout reader defTimeout
                rs1 <- P.observeDuration readLatencySnd $ S.readerRead reader storageOpts.fetchMaxLen
                let size = sum (map (K.recordBytesSize . (.recordPayload)) rs1)
                if size >= fromIntegral r.minBytes
                   then pure rs1
                   else do S.readerSetTimeout reader (r.maxWaitMs - defTimeout)
                           rs2 <- S.readerRead reader storageOpts.fetchMaxLen
                           pure $ rs1 <> rs2
              else do
                S.readerSetTimeout reader r.maxWaitMs
                S.readerRead reader storageOpts.fetchMaxLen

    -- Note this function's behaviour is not the same as kafka broker
    --
    -- In kafka broker, regarding the format on disk, the broker will return
    -- the message format according to the fetch api version. Which means
    --
    --   * if the fetch api version is less than 4, the broker will always
    --     return MessageSet even the message format on disk is RecordBatch.
    --   * if the fetch api version is 4+, the broker will always return
    --     RecordBath.
    --
    -- Here, we donot handle the fetch api version, we just return the message
    -- format according to the message format on disk.
    --
    -- However, if you always use RecordBath for appending and reading, it
    -- won't be a problem.
    encodePartition
      :: K.FetchPartition -> V.Vector K.RecordFormat -> IO ByteString
    encodePartition p v = do
      let (rf :: K.RecordFormat, vs) =
            -- This should not be Nothing, because if we found the key
            -- in `readRecords`, it means we have at least one record
            -- in this
            fromMaybe (error "LogicError: got empty vector value")
            (V.uncons v)
          bytesOnDisk = K.unCompactBytes rf.recordBytes
      -- only the first MessageSet need to to this seeking
      magic <- K.decodeRecordMagic bytesOnDisk
      fstRecordBytes <-
        if | magic >= 2 -> pure bytesOnDisk
           | otherwise -> do
             let absStartOffset = rf.offset + 1 - fromIntegral rf.batchLength
                 offset = p.fetchOffset - absStartOffset
             if offset > 0
                then K.seekBatch (fromIntegral offset) bytesOnDisk
                else pure bytesOnDisk

      let v' = V.map (BB.byteString . K.unCompactBytes . (.recordBytes)) vs
          b = V.foldl (<>) (BB.byteString fstRecordBytes) v'
      pure $ BS.toStrict $ BB.toLazyByteString b

-------------------------------------------------------------------------------

-- Return tuple of (startLsn, tailLsn, highwaterOffset)
--
-- NOTE: tailLsn is LSN_INVALID if the partition is empty
getPartitionLsn
  :: S.LDClient
  -> K.OffsetManager
  -> S.C_LogID -> Int32
  -> Int64        -- ^ kafka start offset
  -> IO (Either K.PartitionData (S.LSN, S.LSN, Int64))
getPartitionLsn ldclient om logid partition offset = do
  m <- K.getLatestOffsetWithLsn om logid
  case m of
    Just (latestOffset, tailLsn) -> do
      let highwaterOffset = latestOffset + 1
      if | offset < latestOffset -> do
             let key = U.intToCBytesWithPadding offset
             Log.debug1 $ "Try findKey " <> Log.buildString' key <> " in logid "
                       <> Log.build logid
             (_, startLsn) <- S.findKey ldclient logid key S.FindKeyStrict
             Log.debug1 $ "FindKey result " <> Log.build logid <> ": "
                       <> Log.build startLsn
             pure $ Right (startLsn, tailLsn, highwaterOffset)
         | offset == latestOffset ->
             pure $ Right (tailLsn, tailLsn, highwaterOffset)
         | offset == highwaterOffset ->
             pure $ Right (tailLsn + 1, tailLsn, highwaterOffset)
         | offset > highwaterOffset ->
             pure $ Left $ errorPartitionResponse partition K.OFFSET_OUT_OF_RANGE
         -- ghc is not smart enough to detact my partten matching is complete
         | otherwise -> error "This should not be reached (getPartitionLsn)"
    Nothing -> do
      Log.debug $ "Partition " <> Log.build logid <> " is empty"
      if offset == 0
         then pure $ Right (S.LSN_MIN, S.LSN_INVALID, 0)
         else pure $ Left $ errorPartitionResponse partition K.OFFSET_OUT_OF_RANGE

errorPartitionResponse :: Int32 -> K.ErrorCode -> K.PartitionData
errorPartitionResponse partitionIndex ec =
  K.PartitionData partitionIndex ec (-1) (Just "")
                  (-1){- TODO: lastStableOffset -}
                  (K.NonNullKaArray V.empty){- TODO: abortedTransactions -}
{-# INLINE errorPartitionResponse #-}

foldWhileM :: Monad m => a -> (a -> m (a, Bool)) -> m a
foldWhileM !a f = do
  (a', b) <- f a
  if b then foldWhileM a' f else pure a'
