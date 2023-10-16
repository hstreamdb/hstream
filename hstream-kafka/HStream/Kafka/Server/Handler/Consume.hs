{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Server.Handler.Consume
  ( handleFetchV2
  ) where

import           Control.Monad
import qualified Data.ByteString                    as BS
import qualified Data.ByteString.Builder            as BB
import           Data.Int
import           Data.Maybe
import qualified Data.Vector                        as V
import qualified Data.Vector.Hashtables             as HT
import qualified Data.Vector.Storable               as VS

import qualified HStream.Base.Growing               as GV
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

type RecordTable =
  HT.Dictionary (HT.PrimState IO)
                VS.MVector
                S.C_LogID
                V.MVector
                (GV.Growing V.Vector GV.RealWorld K.RecordFormat)

-- NOTE: this behaviour is not the same as kafka broker
handleFetchV2
  :: ServerContext -> K.RequestContext
  -> K.FetchRequestV2 -> IO K.FetchResponseV2
handleFetchV2 ServerContext{..} _ r = do
  -- kafka broker just throw java.lang.RuntimeException if topics is null, here
  -- we do the same.
  let K.NonNullKaArray topicReqs = r.topics
  topics <- V.forM topicReqs $ \K.FetchTopicV2{..} -> do
    orderedParts <- S.listStreamPartitionsOrdered scLDClient (S.transToTopicStreamName topic)
    let K.NonNullKaArray partitionReqs = partitions
    ps <- V.forM partitionReqs $ \p -> do
      let (_, logid) = orderedParts V.! fromIntegral p.partition
      elsn <- getPartitionLsn scLDClient scOffsetManager logid p.partition p.fetchOffset
      pure (logid, elsn, p)
    pure (topic, ps)

  -- New reader
  let numOfReads = V.sum $ V.map (V.length . snd) topics
  reader <- S.newLDReader scLDClient (fromIntegral numOfReads) Nothing

  -- Start reading
  V.forM_ topics $ \(_, partitions) -> do
    V.forM_ partitions $ \(logid, elsn, _) -> do
      case elsn of
        Left _ -> pure ()
        Right (startlsn, _, _) -> do
          S.readerStartReading reader logid startlsn S.LSN_MAX

  -- Read records from storage
  S.readerSetTimeout reader r.maxWaitMs
  S.readerSetWaitOnlyWhenNoData reader
  (_, records) <- foldWhileM (0, []) $ \(size, acc) -> do
    rs <- S.readerRead reader 100
    if null rs
       then pure ((size, acc), False)
       else do let size' = size + (sum $ map (K.recordBytesSize . (.recordPayload)) rs)
                   acc' = acc <> rs
               if size' >= fromIntegral r.minBytes
                  then pure ((size', acc'), False)
                  else pure ((size', acc'), True)

  -- Process read records
  -- TODO: what if client send two same topic but with different partitions?
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
            Nothing -> pure $ K.PartitionDataV2 p.partition K.NONE hioffset (Just "")
            Just gv -> do
              v <- GV.unsafeFreeze gv
              -- This should not be Nothing, because if we found the key in
              -- `readRecords`, it means we have at least one record in this
              let (rf :: K.RecordFormat, vs) =
                    fromMaybe (error "LogicError: got empty vector value")
                    (V.uncons v)
              let absStartOffset = rf.offset + 1 - fromIntegral rf.batchLength
                  bytesOnDisk = K.unCompactBytes rf.recordBytes
              fstRecordBytes <-
                if (absStartOffset < p.fetchOffset)
                   -- only the first bathch need to to this seek
                   then K.seekBatch (fromIntegral $ p.fetchOffset - absStartOffset) bytesOnDisk
                   else pure bytesOnDisk
              let b = V.foldl (<>) (BB.byteString fstRecordBytes)
                                   (V.map (BB.byteString . K.unCompactBytes . (.recordBytes)) vs)
                  bs = BS.toStrict $ BB.toLazyByteString b
              pure $ K.PartitionDataV2 p.partition K.NONE hioffset (Just bs)
    pure $ K.FetchableTopicResponseV2 topic (K.NonNullKaArray respPartitionDatas)
  pure $ K.FetchResponseV2 0{- TODO: throttleTimeMs -} (K.NonNullKaArray respTopics)

-------------------------------------------------------------------------------

-- Return tuple of (startLsn, tailLsn, highwaterOffset)
--
-- NOTE: tailLsn is LSN_INVALID if the partition is empty
getPartitionLsn
  :: S.LDClient
  -> K.OffsetManager
  -> S.C_LogID -> Int32
  -> Int64        -- ^ kafka start offset
  -> IO (Either K.PartitionDataV2 (S.LSN, S.LSN, Int64))
getPartitionLsn ldclient om logid partition offset = do
  m <- K.getLatestOffsetWithLsn om logid
  case m of
    Just (latestOffset, tailLsn) -> do
      let highwaterOffset = latestOffset + 1
      if | offset < latestOffset -> do
             let key = U.int2cbytes offset
             (_, startLsn) <- S.findKey ldclient logid key S.FindKeyStrict
             pure $ Right (startLsn, tailLsn, highwaterOffset)
         | offset == latestOffset ->
             pure $ Right (tailLsn, tailLsn, highwaterOffset)
         | offset == highwaterOffset ->
             pure $ Right (tailLsn + 1, tailLsn, highwaterOffset)
         | offset > highwaterOffset ->
             pure $ Left $ errorPartitionResponseV2 partition K.OFFSET_OUT_OF_RANGE
         -- ghc is not smart enough to detact my partten matching is complete
         | otherwise -> error "This should not be reached (getPartitionLsn)"
    Nothing -> do
      Log.debug $ "Partition " <> Log.build logid <> " is empty"
      if offset == 0
         then pure $ Right (S.LSN_MIN, S.LSN_INVALID, 0)
         else pure $ Left $ errorPartitionResponseV2 partition K.OFFSET_OUT_OF_RANGE

errorPartitionResponseV2 :: Int32 -> K.ErrorCode -> K.PartitionDataV2
errorPartitionResponseV2 partitionIndex ec =
  K.PartitionDataV2 partitionIndex ec (-1) (Just "")
{-# INLINE errorPartitionResponseV2 #-}

foldWhileM :: Monad m => a -> (a -> m (a, Bool)) -> m a
foldWhileM !a f = do
  (a', b) <- f a
  if b then foldWhileM a' f else pure a'
