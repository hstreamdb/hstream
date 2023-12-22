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
import           GHC.Data.FastMutInt
import           GHC.Stack                          (HasCallStack)

import qualified HStream.Base.Growing               as GV
import qualified HStream.Kafka.Common.Metrics       as M
import qualified HStream.Kafka.Common.OffsetManager as K
import qualified HStream.Kafka.Common.RecordFormat  as K
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

-- Tuple of (startLsn, tailLsn, highwaterOffset)
type PartitionOffsetData = Either K.PartitionData (S.LSN, S.LSN, Int64)

data Partition = Partition
  { logid   :: {-# UNPACK #-} !S.C_LogID
  , elsn    :: {-# UNPACK #-} !PartitionOffsetData
  , request :: !K.FetchPartition
  }

-- NOTE: this behaviour is not the same as kafka broker
handleFetch
  :: HasCallStack
  => ServerContext
  -> K.RequestContext -> K.FetchRequest -> IO K.FetchResponse
handleFetch ServerContext{..} _ r = K.catchFetchResponseEx $ do
  ---------------------------------------
  -- * Preprocess request
  ---------------------------------------
  mutNumOfReads <- newFastMutInt 0 -- Total number of real reads
  -- kafka broker just throw java.lang.RuntimeException if topics is null, here
  -- we do the same.
  let K.NonNullKaArray topicReqs = r.topics
  topics <- V.forM topicReqs $ \t{- K.FetchTopic -} -> do
    -- Partition should be non-empty
    let K.NonNullKaArray partitionReqs = t.partitions
    orderedParts <- S.listStreamPartitionsOrdered scLDClient
                      (S.transToTopicStreamName t.topic)
    ps <- V.forM partitionReqs $ \p{- K.FetchPartition -} -> do
      M.withLabel M.totalConsumeRequest (t.topic, T.pack . show $ p.partition) $
        \counter -> void $ M.addCounter counter 1
      let m_logid = orderedParts V.!? fromIntegral p.partition
      case m_logid of
        Nothing -> do
          let elsn = errorPartitionResponse p.partition K.UNKNOWN_TOPIC_OR_PARTITION
          -- Actually, the logid should be Nothing but 0, however, we won't
          -- use it, so just set it to 0
          pure $ Partition 0 (Left elsn) p
        Just (_, logid) -> do
          elsn <- getPartitionLsn scLDClient scOffsetManager logid p.partition
                                  p.fetchOffset
          when (isRight elsn) $ void $ atomicFetchAddFastMut mutNumOfReads 1
          pure $ Partition logid elsn p
    pure (t.topic, ps)

  numOfReads <- readFastMutInt mutNumOfReads
  when (numOfReads < 1) $ do
    respTopics <- V.forM topics $ \(topic, partitions) -> do
      respPartitionDatas <- V.forM partitions $ \partition -> do
        case partition.elsn of
          Left pd -> pure pd
          Right _ -> error "LogicError: this should not be right"
      pure $ K.FetchableTopicResponse topic (K.NonNullKaArray respPartitionDatas)
    let resp = K.FetchResponse (K.NonNullKaArray respTopics) 0{- TODO: throttleTimeMs -}
    throwIO $ K.FetchResponseEx resp

  ---------------------------------------
  -- * Start reading
  --
  -- Currently, we use a per-connection reader(fetchReader) to read.
  ---------------------------------------
  V.forM_ topics $ \(_, partitions) -> do
    V.forM_ partitions $ \partition -> do
      case partition.elsn of
        Left _ -> pure ()
        Right (startlsn, _, _) -> do
          Log.debug1 $ "start reading log "
                    <> Log.build partition.logid
                    <> " from " <> Log.build startlsn
          S.readerStartReading fetchReader partition.logid startlsn S.LSN_MAX

  ---------------------------------------
  -- * Read records from storage
  ---------------------------------------
  -- TODO:
  -- - dynamically change reader settings according to the client request
  -- (e.g. maxWaitMs, minBytes...)
  --
  -- FIXME: Do not setWaitOnlyWhenNoData if you are mostly focusing on
  -- throughput
  -- Mode1
  records <- readMode1 fetchReader

  ---------------------------------------
  -- * Process read records
  --
  -- TODO: what if client send two same topic but with different partitions?
  ---------------------------------------
  -- {logid: [RecordFormat]}
  readRecords <- HT.initialize numOfReads :: IO RecordTable
  forM_ records $ \record -> do
    recordFormat <- K.runGet @K.RecordFormat record.recordPayload
    let logid = record.recordAttr.recordAttrLogID
    v <- maybe GV.new pure =<< (HT.lookup readRecords logid)
    v' <- GV.append v recordFormat
    HT.insert readRecords logid v'

  ---------------------------------------
  -- * Generate response
  ---------------------------------------
  mutMaxBytes <- newFastMutInt $ fromIntegral r.maxBytes
  mutIsFirstPartition <- newFastMutInt 1  -- TODO: improve this
  respTopics <- V.forM topics $ \(topic, partitions) -> do
    respPartitionDatas <- V.forM partitions $ \partition -> do
      let request = partition.request
      case partition.elsn of
        Left pd -> pure pd
        Right (_startlsn, _endlsn, hioffset) -> do
          mgv <- HT.lookup readRecords partition.logid
          case mgv of
            Nothing ->
              pure $ K.PartitionData request.partition K.NONE
                                     hioffset
                                     (Just "")
                                     (-1){- TODO: lastStableOffset -}
                                     (K.NonNullKaArray V.empty){- TODO: abortedTransactions -}
            Just gv -> do
              v <- GV.unsafeFreeze gv
              bs <- encodePartition mutMaxBytes mutIsFirstPartition request v
              -- Stats
              let partLabel = (topic, T.pack . show $ request.partition)
              M.withLabel M.topicTotalSendBytes partLabel $ \counter -> void $
                M.addCounter counter (fromIntegral $ BS.length bs)
              M.withLabel M.topicTotalSendMessages partLabel $ \counter -> void $ do
                let totalRecords = V.sum $ V.map (\K.RecordFormat{..} -> batchLength) v
                M.addCounter counter (fromIntegral totalRecords)
              -- PartitionData
              pure $ K.PartitionData request.partition K.NONE hioffset (Just bs)
                                     (-1){- TODO: lastStableOffset -}
                                     (K.NonNullKaArray V.empty){- TODO: abortedTransactions -}
    pure $ K.FetchableTopicResponse topic (K.NonNullKaArray respPartitionDatas)
  pure $ K.FetchResponse (K.NonNullKaArray respTopics) 0{- TODO: throttleTimeMs -}

  where
    -- Currently unused
    readMode0 :: S.LDReader -> IO [S.DataRecord ByteString]
    readMode0 reader = do
      if r.minBytes <= 0 || r.maxWaitMs <= 0
         then S.readerSetTimeout reader 0  -- nonblocking
         else S.readerSetTimeout reader r.maxWaitMs
      S.readerSetWaitOnlyWhenNoData reader
      (_, records) <- foldWhileM (0, []) $ \(size, acc) -> do
        rs <- M.observeDuration M.readLatencySnd $ S.readerRead reader 100
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
                rs1 <- M.observeDuration M.readLatencySnd $
                          S.readerRead reader storageOpts.fetchMaxLen
                let size = sum (map (K.recordBytesSize . (.recordPayload)) rs1)
                if size >= fromIntegral r.minBytes
                   then pure rs1
                   else do S.readerSetTimeout reader (r.maxWaitMs - defTimeout)
                           rs2 <- S.readerRead reader storageOpts.fetchMaxLen
                           pure $ rs1 <> rs2
              else do
                S.readerSetTimeout reader r.maxWaitMs
                S.readerRead reader storageOpts.fetchMaxLen

-------------------------------------------------------------------------------

-- Return tuple of (startLsn, tailLsn, highwaterOffset)
--
-- NOTE: tailLsn is LSN_INVALID if the partition is empty
getPartitionLsn
  :: S.LDClient
  -> K.OffsetManager
  -> S.C_LogID -> Int32
  -> Int64        -- ^ kafka start offset
  -> IO PartitionOffsetData
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
  :: FastMutInt
  -> FastMutInt
  -> K.FetchPartition
  -> V.Vector K.RecordFormat
  -> IO ByteString
encodePartition mutMaxBytes mutIsFirstPartition p v = do
  maxBytes <- readFastMutInt mutMaxBytes
  if maxBytes > 0 then doEncode maxBytes else pure ""
  where
    doEncode maxBytes = do
      isFristPartition <- readFastMutInt mutIsFirstPartition
      (fstRecordBytes, vs) <- reFormat
      let fstLen = BS.length fstRecordBytes
      if isFristPartition == 1
         -- First partition
         then do
           writeFastMutInt mutIsFirstPartition 0  -- next partition should not be the first
           if fstLen >= maxBytes
              then do writeFastMutInt mutMaxBytes (-1)
                      pure fstRecordBytes
              else if fstLen >= (fromIntegral p.partitionMaxBytes)
                      then do void $ atomicFetchAddFastMut mutMaxBytes (-fstLen)
                              pure fstRecordBytes
                      else doEncodeElse fstRecordBytes vs
         -- Not the first partition
         else do
           if fstLen <= maxBytes
              then doEncodeElse fstRecordBytes vs
              else pure ""

    doEncodeElse fstRecordBytes vs = do
      let fstLen = BS.length fstRecordBytes
      void $ atomicFetchAddFastMut mutMaxBytes (-fstLen)
      mutPartitionMaxBytes <-
        newFastMutInt (fromIntegral p.partitionMaxBytes - fstLen)

      bb <- V.foldM (\b r -> do
        let rbs = K.unCompactBytes r.recordBytes
            rlen = BS.length rbs
        curMaxBytes <- atomicFetchAddFastMut mutMaxBytes (-rlen)
        curPartMaxBytes <- atomicFetchAddFastMut mutPartitionMaxBytes (-rlen)
        -- take a negative number of bytes will return an empty ByteString
        let rbs' = BS.take (min curPartMaxBytes curMaxBytes) rbs
        if BS.null rbs'
           then pure b
           else pure $ b <> BB.byteString rbs'
        ) (BB.byteString fstRecordBytes) vs

      pure $ BS.toStrict $ BB.toLazyByteString bb

    reFormat = do
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
      pure (fstRecordBytes, vs)

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
