{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Server.Handler.Consume
  ( handleFetch
  ) where

import           Control.Exception
import           Control.Monad
import           Data.ByteString                       (ByteString)
import qualified Data.ByteString                       as BS
import qualified Data.ByteString.Builder               as BB
import           Data.Int
import           Data.Maybe
import           Data.Text                             (Text)
import qualified Data.Text                             as T
import           Data.Vector                           (Vector)
import qualified Data.Vector                           as V
import qualified Data.Vector.Hashtables                as HT
import qualified Data.Vector.Storable                  as VS
import           GHC.Data.FastMutInt
import           GHC.Stack                             (HasCallStack)

import qualified HStream.Base.Growing                  as GV
import qualified HStream.Kafka.Common.Acl              as K
import qualified HStream.Kafka.Common.Authorizer.Class as K
import qualified HStream.Kafka.Common.FetchManager     as K
import qualified HStream.Kafka.Common.Metrics          as M
import qualified HStream.Kafka.Common.OffsetManager    as K
import qualified HStream.Kafka.Common.RecordFormat     as K
import qualified HStream.Kafka.Common.Resource         as K
import           HStream.Kafka.Server.Config           (ServerOpts (..),
                                                        StorageOptions (..))
import           HStream.Kafka.Server.Types            (ServerContext (..))
import qualified HStream.Logger                        as Log
import qualified HStream.Store                         as S
import qualified HStream.Utils                         as U
import qualified Kafka.Protocol.Encoding               as K
import qualified Kafka.Protocol.Error                  as K
import qualified Kafka.Protocol.Message                as K
import qualified Kafka.Protocol.Service                as K

-------------------------------------------------------------------------------

-- {logid: ([RemRecord], [ReadRecord])}
type RecordTable =
  HT.Dictionary (HT.PrimState IO)
                VS.MVector
                S.C_LogID
                V.MVector
                (Vector K.RecordFormat, GV.Growing Vector GV.RealWorld K.RecordFormat)

data LsnData
  = LsnData S.LSN S.LSN Int64
    -- ^ (startLsn, tailLsn, highwaterOffset)
    --
    -- NOTE: tailLsn is LSN_INVALID if the partition is empty
  | ContReading (Vector K.RecordFormat) Int64
    -- ^ Continue reading, do not need to start reading
    --
    -- (remRecords, highwaterOffset)
  | ErrPartitionData K.PartitionData
    -- ^ Error partition response
  deriving (Show)

extractHiOffset :: LsnData -> Either K.PartitionData Int64
extractHiOffset (LsnData _ _ o)      = Right o
extractHiOffset (ContReading _ o)    = Right o
extractHiOffset (ErrPartitionData d) = Left d

isErrPartitionData :: LsnData -> Bool
isErrPartitionData (ErrPartitionData _) = True
isErrPartitionData _                    = False

data Partition = Partition
  { logid   :: {-# UNPACK #-} !S.C_LogID
  , elsn    :: !LsnData
  , request :: !K.FetchPartition
  } deriving (Show)

data ReFetchRequest = ReFetchRequest
  { topics     :: !(Vector (Text, Vector Partition))
    -- Original request
  , minBytes   :: !Int32
  , maxWaitMs  :: !Int32
  , maxBytes   :: !Int32
    -- Helpful attrs
  , contFetch  :: !Bool
  , totalReads :: !Int
  , allError   :: !Bool
  } deriving (Show)

-- NOTE: this behaviour is not the same as kafka broker
--
-- TODO
--
-- 1. What if r.maxBytes is <=0 ?
handleFetch
  :: HasCallStack
  => ServerContext
  -> K.RequestContext -> K.FetchRequest -> IO K.FetchResponse
handleFetch sc@ServerContext{..} reqCtx r_ = K.catchFetchResponseEx $ do
  -- Currently, we use a per-connection reader(fetchReader) to read.
  let fetchReader = fetchCtx.reader

  ---------------------------------------
  -- * Preprocess request
  ---------------------------------------
  r <- preProcessRequest sc reqCtx r_

  -- Fail fast: all error
  when r.allError $ do
    respTopics <- V.forM r.topics $ \(topic, partitions) -> do
      respPartitionDatas <- V.forM partitions $ \partition -> do
        case partition.elsn of
          ErrPartitionData pd -> pure pd
          x -> error $ "LogicError: this should not be " <> show x
      pure $ K.FetchableTopicResponse topic (K.NonNullKaArray respPartitionDatas)
    let resp = K.FetchResponse (K.NonNullKaArray respTopics) 0{- TODO: throttleTimeMs -}
    Log.warning $ "Fetch request return error: " <> Log.build (show resp)
    -- Exit early
    throwIO $ K.FetchResponseEx resp

  -- Client request to new reading
  unless r.contFetch $ do
    -- Clear the context
    K.clearFetchLogCtx fetchCtx
    -- Start reading
    V.forM_ r.topics $ \(_, partitions) -> do
      V.forM_ partitions $ \partition -> do
        case partition.elsn of
          LsnData startlsn _ _ -> do
            Log.debug1 $ "start reading log "
                      <> Log.build partition.logid
                      <> " from " <> Log.build startlsn
            S.readerStartReading fetchReader partition.logid startlsn S.LSN_MAX
          _ -> pure ()

  ---------------------------------------
  -- * Read records from storage
  ---------------------------------------
  -- TODO:
  -- - dynamically change reader settings according to the client request
  -- (e.g. maxWaitMs, minBytes...)
  --
  -- FIXME: Do not setWaitOnlyWhenNoData if you are mostly focusing on
  -- throughput

  -- FIXME: what if client send two same topic but with different partitions?
  -- {logid: ([RemRecord], [ReadRecord])}
  readRecords <- readMode1 r serverOpts._storage fetchReader

  ---------------------------------------
  -- * Generate response
  ---------------------------------------
  mutMaxBytes <- newFastMutInt $ fromIntegral r.maxBytes
  mutIsFirstPartition <- newFastMutInt 1  -- TODO: improve this
  respTopics <- V.forM r.topics $ \(topic, partitions) -> do
    respPartitionDatas <- V.forM partitions $ \partition -> do
      let request = partition.request
      let e_hioffset = extractHiOffset partition.elsn
      case e_hioffset of
        Left pd -> pure pd
        Right hioffset -> do
          mgv <- HT.lookup readRecords partition.logid
          case mgv of
            Nothing ->
              pure $ K.PartitionData
                { partitionIndex      = request.partition
                , errorCode           = K.NONE
                , highWatermark       = hioffset
                , recordBytes         = (Just "")
                , lastStableOffset    = (-1) -- TODO
                , abortedTransactions = K.NonNullKaArray V.empty -- TODO
                  -- TODO: for performance reason, we don't implement
                  -- logStartOffset now
                , logStartOffset      = (-1)
                }
            Just (remv, gv) -> do
              v <- if V.null remv
                      then GV.unsafeFreeze gv
                      -- TODO PERF
                      else (remv <>) <$> GV.unsafeFreeze gv
              (bs, m_offset, tokenIdx) <- encodePartition mutMaxBytes mutIsFirstPartition request v
              K.setFetchLogCtx
                fetchCtx
                partition.logid
                K.FetchLogContext{ nextOffset = fromMaybe (-1) m_offset
                                 , remRecords = V.drop (tokenIdx + 1) v
                                 }
              -- Stats
              let partLabel = (topic, T.pack . show $ request.partition)
              M.withLabel M.topicTotalSendBytes partLabel $ \counter -> void $
                M.addCounter counter (fromIntegral $ BS.length bs)
              M.withLabel M.topicTotalSendMessages partLabel $ \counter -> void $ do
                let totalRecords = V.sum $ V.map (\K.RecordFormat{..} -> batchLength) v
                M.addCounter counter (fromIntegral totalRecords)
              -- PartitionData
              pure $ K.PartitionData
                { partitionIndex      = request.partition
                , errorCode           = K.NONE
                , highWatermark       = hioffset
                , recordBytes         = (Just bs)
                , lastStableOffset    = (-1) -- TODO
                , abortedTransactions = K.NonNullKaArray V.empty -- TODO
                  -- TODO: for performance reason, we don't implement
                  -- logStartOffset now
                , logStartOffset      = (-1)
                }
    pure $ K.FetchableTopicResponse topic (K.NonNullKaArray respPartitionDatas)
  pure $ K.FetchResponse (K.NonNullKaArray respTopics) 0{- TODO: throttleTimeMs -}

-------------------------------------------------------------------------------

preProcessRequest :: ServerContext -> K.RequestContext -> K.FetchRequest -> IO ReFetchRequest
preProcessRequest ServerContext{..} reqCtx r = do
  -- kafka broker just throw java.lang.RuntimeException if topics is null, here
  -- we do the same.
  let K.NonNullKaArray topicReqs = r.topics
  mutContFetch <- newFastMutInt 1   -- Bool
  mutNumOfReads <- newFastMutInt 0  -- Total number of reads
  topics <- V.forM topicReqs $ \t{- K.FetchTopic -} -> do
    -- [ACL] check [READ TOPIC]
    -- TODO: In kafka, check [CLUSTER_ACTION CLUSTER] instead if the request is from follower.
    --       Of course, we do not consider this now.
    isTopicAuthzed <- K.simpleAuthorize (K.toAuthorizableReqCtx reqCtx) authorizer K.Res_TOPIC t.topic K.AclOp_READ
    -- Partition should be non-empty
    let K.NonNullKaArray partitionReqs = t.partitions
    -- FIXME: we can also cache this in FetchContext, however, we need to
    -- consider the following: what if someone delete the topic?
    orderedParts <- S.listStreamPartitionsOrderedByName scLDClient
                      (S.transToTopicStreamName t.topic)
    ps <- V.forM partitionReqs $ \p{- K.FetchPartition -} -> do
      M.withLabel M.totalConsumeRequest (t.topic, T.pack . show $ p.partition) $
        \counter -> void $ M.addCounter counter 1
      -- FIXME: too deep nesting...
      if not isTopicAuthzed then do
        let elsn = ErrPartitionData $
              errorPartitionResponse p.partition K.TOPIC_AUTHORIZATION_FAILED
        pure $ Partition 0 elsn p
        else do
        let m_logid = orderedParts V.!? fromIntegral p.partition
        case m_logid of
          Nothing -> do
            let elsn = ErrPartitionData $
                  errorPartitionResponse p.partition K.UNKNOWN_TOPIC_OR_PARTITION
            -- Actually, the logid should be Nothing but 0, however, we won't
            -- use it, so just set it to 0
            pure $ Partition 0 elsn p
          Just (_, logid) -> do
            void $ atomicFetchAddFastMut mutNumOfReads 1
            contFetch <- readFastMutInt mutContFetch
            elsn <-
              if contFetch == 0
                 then getPartitionLsn scLDClient scOffsetManager logid p.partition
                                      p.fetchOffset
                 else do
                   m_logCtx <- K.getFetchLogCtx fetchCtx logid
                   case m_logCtx of
                     Nothing -> do -- Cache miss
                       writeFastMutInt mutContFetch 0
                       getPartitionLsn scLDClient scOffsetManager
                                       logid p.partition p.fetchOffset
                     Just logCtx ->
                       if (logCtx.nextOffset /= p.fetchOffset)
                          then do
                            writeFastMutInt mutContFetch 0
                            getPartitionLsn scLDClient scOffsetManager logid p.partition
                                            p.fetchOffset
                          else do
                            m <- K.getLatestOffsetWithLsn scOffsetManager logid
                            case m of
                              Just (latestOffset, _tailLsn) -> do
                                let highwaterOffset = latestOffset + 1
                                pure $ ContReading logCtx.remRecords highwaterOffset
                              Nothing -> do
                                Log.debug $ "Continue reading, but logid "
                                         <> Log.build logid <> " is empty"
                                pure $ ErrPartitionData $
                                  errorPartitionResponse p.partition
                                                         K.OFFSET_OUT_OF_RANGE
            pure $ Partition logid elsn p
    pure (t.topic, ps)
  contFetch <- readFastMutInt mutContFetch
  numOfReads <- readFastMutInt mutNumOfReads
  -- TODO PERF: We can bybass loop all topics(using a global mutAllError).
  -- However, this will make the code more complex.
  let doesAllError = all (all (isErrPartitionData . (.elsn)) . snd)
  if contFetch == 0
     then do
       pure $ ReFetchRequest{ topics = topics
                            , minBytes = r.minBytes
                            , maxBytes = r.maxBytes
                            , maxWaitMs = r.maxWaitMs
                            , contFetch = False
                            , totalReads = numOfReads
                            , allError = doesAllError topics
                            }
     else do cacheNumOfReads <- length <$> K.getAllFetchLogs fetchCtx
             if numOfReads == cacheNumOfReads
                then
                  pure $ ReFetchRequest{ topics = topics
                                       , minBytes = r.minBytes
                                       , maxBytes = r.maxBytes
                                       , maxWaitMs = r.maxWaitMs
                                       , contFetch = True
                                       , totalReads = numOfReads
                                       , allError = doesAllError topics
                                       }
                else do
                  ts <- forM topics $ \(tn, ps) -> do
                    ps' <- forM ps $ \p -> do
                      case p.elsn of
                        ContReading _ _ -> do
                          elsn <- getPartitionLsn scLDClient scOffsetManager p.logid
                                                  p.request.partition
                                                  p.request.fetchOffset
                          pure $ p{elsn = elsn}
                        _ -> pure p
                    pure (tn, ps')
                  pure $ ReFetchRequest{ topics = ts
                                       , minBytes = r.minBytes
                                       , maxBytes = r.maxBytes
                                       , maxWaitMs = r.maxWaitMs
                                       , contFetch = False
                                       , totalReads = numOfReads
                                       , allError = doesAllError ts
                                       }

getPartitionLsn
  :: S.LDClient
  -> K.OffsetManager
  -> S.C_LogID -> Int32
  -> Int64        -- ^ kafka start offset
  -> IO LsnData
getPartitionLsn ldclient om logid partition offset = do
  Log.debug $ "Request partition LSN for logid: " <> Log.build logid <> ", partition: " <> Log.build partition <> ", offset: " <> Log.build offset
  m <- K.getLatestOffsetWithLsn om logid
  case m of
    Just (latestOffset, tailLsn) -> do
      let highwaterOffset = latestOffset + 1
      if | offset < latestOffset -> do
             let key = U.intToCBytesWithPadding offset
             Log.debug1 $ "Try findKey " <> Log.buildString' key <> " in logid "
                       <> Log.build logid
             (_, startLsn) <- S.findKey ldclient logid key S.FindKeyStrict
             Log.debug1 $ "FindKey for logid " <> Log.build logid <> " get result " <> Log.build startLsn
             pure $ LsnData startLsn tailLsn highwaterOffset
         | offset == latestOffset -> do
             Log.debug1 $ "offset == latestOffset"
             pure $ LsnData tailLsn tailLsn highwaterOffset
         | offset == highwaterOffset -> do
             Log.debug1 $ "offset == highwaterOffset"
             pure $ LsnData (tailLsn + 1) tailLsn highwaterOffset
         | offset > highwaterOffset -> do
             Log.debug1 $ "offset > highwaterOffset"
             pure $ ErrPartitionData $ errorPartitionResponse partition K.OFFSET_OUT_OF_RANGE
         -- ghc is not smart enough to detact my partten matching is complete
         | otherwise -> error "This should not be reached (getPartitionLsn)"
    Nothing -> do
      Log.debug $ "Partition " <> Log.build logid <> " is empty"
      if offset == 0
         then pure $ LsnData S.LSN_MIN S.LSN_INVALID 0
         else pure $ ErrPartitionData $ errorPartitionResponse partition K.OFFSET_OUT_OF_RANGE

readMode1
  :: ReFetchRequest
  -> StorageOptions
  -> S.LDReader
  -> IO RecordTable
readMode1 r storageOpts reader = do
  recordTable <- HT.initialize r.totalReads :: IO RecordTable
  mutRemSize <- newFastMutInt 0
  when r.contFetch $ do
    forM_ r.topics $ \(_, partitions) ->
      forM_ partitions $ \p -> do
        case p.elsn of
          ContReading remRecords _ -> do
            void $ atomicFetchAddFastMut mutRemSize $ V.sum $
              V.map (BS.length . K.unCompactBytes . (.recordBytes)) remRecords
            -- [TAG_NEV]: Make sure do not insert empty vector to the table,
            -- since we will assume the vector is non-empty in `encodePartition`
            unless (V.null remRecords) $
              insertRemRecords recordTable p.logid remRecords
          x -> Log.fatal $
           "LogicError: this should not be reached, " <> Log.buildString' x
  remsize <- readFastMutInt mutRemSize
  if remsize > fromIntegral r.maxBytes  -- assume r.maxBytes > 0
     then pure recordTable
     else doRead recordTable
  where
    doRead recordTable = do
      let defTimeout = fromIntegral storageOpts.fetchReaderTimeout

      if r.minBytes <= 0 || r.maxWaitMs <= 0 -- respond immediately
         then do
           Log.debug1 $ "Set reader wait only when no data and timeout to 10ms"
           S.readerSetWaitOnlyWhenNoData reader
           S.readerSetTimeout reader 10
           insertRecords recordTable
             -- For non-empty results
             =<< S.readerReadSome reader storageOpts.fetchMaxLen 10{-retries-}
         else
           if r.maxWaitMs > defTimeout
              then do
                Log.debug1 $ "Set reader timeout to " <> Log.build defTimeout
                S.readerSetTimeout reader defTimeout
                rs1 <- M.observeDuration M.topicReadStoreLatency $
                          S.readerRead reader storageOpts.fetchMaxLen
                insertRecords recordTable rs1
                -- FIXME: this size is not accurate because of the CompactBytes
                -- See: K.recordBytesSize
                let size = sum (map (K.recordBytesSize . (.recordPayload)) rs1)
                when (size < fromIntegral r.minBytes) $ do
                  S.readerSetTimeout reader (r.maxWaitMs - defTimeout)
                  rs2 <- M.observeDuration M.topicReadStoreLatency $
                           S.readerRead reader storageOpts.fetchMaxLen
                  insertRecords recordTable rs2
              else do
                Log.debug1 $ "Set reader timeout to " <> Log.build r.maxWaitMs
                S.readerSetTimeout reader r.maxWaitMs
                rs <- M.observeDuration M.topicReadStoreLatency $
                  S.readerRead reader storageOpts.fetchMaxLen
                insertRecords recordTable rs
      pure recordTable

    insertRemRecords :: RecordTable -> S.C_LogID -> Vector K.RecordFormat -> IO ()
    insertRemRecords table logid records = do
      (rv, v) <- maybe ((V.empty, ) <$> GV.new) pure =<< (HT.lookup table logid)
      HT.insert table logid (rv <> records, v)

    insertRecords :: RecordTable -> [S.DataRecord ByteString] -> IO ()
    insertRecords table records =
      forM_ records $ \record -> do
        recordFormat <- K.runGet @K.RecordFormat record.recordPayload
        let logid = record.recordAttr.recordAttrLogID
        (rv, v) <- maybe ((V.empty, ) <$> GV.new) pure =<< (HT.lookup table logid)
        v' <- GV.append v recordFormat
        HT.insert table logid (rv, v')

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
-- However, if you always use RecordBatch for appending and reading, it
-- won't be a problem.
encodePartition
  :: FastMutInt
  -> FastMutInt
  -> K.FetchPartition
  -> Vector K.RecordFormat
  -> IO (ByteString, Maybe Int64, Int)
  -- ^ (encoded bytes, next offset, taken vector index)
  --
  -- taken vector index: -1 means no vector taken, otherwise, the index of
  -- the vector taken
encodePartition mutMaxBytes mutIsFirstPartition p v = do
  maxBytes <- readFastMutInt mutMaxBytes
  if maxBytes > 0 then doEncode maxBytes else pure ("", Nothing, (-1))
  where
    doEncode maxBytes = do
      isFristPartition <- readFastMutInt mutIsFirstPartition
      (fstRecordBytes, vs) <- trySeek
      let fstLen = BS.length fstRecordBytes
      if isFristPartition == 1
         -- First partition
         then do
           writeFastMutInt mutIsFirstPartition 0  -- next partition should not be the first
           if fstLen >= maxBytes
              then do writeFastMutInt mutMaxBytes (-1)
                      mo <- K.decodeNextRecordOffset fstRecordBytes
                      pure (fstRecordBytes, mo, 0)
              else if fstLen >= (fromIntegral p.partitionMaxBytes)
                      then do void $ atomicFetchAddFastMut mutMaxBytes (-fstLen)
                              mo <- K.decodeNextRecordOffset fstRecordBytes
                              pure (fstRecordBytes, mo, 0)
                      else doEncodeElse fstRecordBytes vs
         -- Not the first partition
         else do
           if fstLen <= maxBytes
              then doEncodeElse fstRecordBytes vs
              else pure ("", Nothing, (-1))

    doEncodeElse fstBs vs = do
      let fstLen = BS.length fstBs
      void $ atomicFetchAddFastMut mutMaxBytes (-fstLen)
      mutPartitionMaxBytes <-
        newFastMutInt (fromIntegral p.partitionMaxBytes - fstLen)

      (bb, lastOffset', takenVecIdx) <-
        vecFoldWhileM vs (BB.byteString fstBs, Left fstBs, 0) $ \(b, lb, i) r -> do
          -- FIXME: Does this possible be multiple BatchRecords?
          let rbs = K.unCompactBytes r.recordBytes
              rlen = BS.length rbs
          curMaxBytes <- atomicFetchAddFastMut mutMaxBytes (-rlen)
          curPartMaxBytes <- atomicFetchAddFastMut mutPartitionMaxBytes (-rlen)
          let capLen = min curPartMaxBytes curMaxBytes
              -- take a negative number of bytes will return an empty ByteString
              rbs' = BS.take capLen rbs
              b' = b <> BB.byteString rbs'
          if capLen < rlen
             then do
               mo1 <- K.decodeNextRecordOffset rbs'
               case mo1 of
                 Just _ -> pure ((b', Right mo1, i), False)
                 Nothing -> do
                   mo2 <- K.decodeNextRecordOffset (fromLeft' lb)
                   pure ((b', Right mo2, i), False)
             else pure ((b', Left rbs, i + 1), True)
      lastOffset <- either K.decodeNextRecordOffset pure lastOffset'

      pure (BS.toStrict $ BB.toLazyByteString bb, lastOffset, takenVecIdx)

    -- Try to bypass the records if the fetch offset is not the first record
    -- in the batch.
    trySeek = do
      let (fstRecord :: K.RecordFormat, vs) =
            -- [TAG_NEV]: This should not be Nothing, because if we found the
            -- key in `readRecords`, it means we have at least one record in
            -- this.
            fromMaybe (error "LogicError: got empty vector value")
                      (V.uncons v)
          bytesOnDisk = K.unCompactBytes fstRecord.recordBytes
      -- only the first MessageSet need to to this seeking
      magic <- K.decodeRecordMagic bytesOnDisk
      fstRecordBytes <-
        if | magic >= 2 -> pure bytesOnDisk
           | otherwise -> do
             let absStartOffset = fstRecord.offset + 1 - fromIntegral fstRecord.batchLength
                 offset = p.fetchOffset - absStartOffset
             if offset > 0
                then do
                  Log.debug1 $ "Seek MessageSet " <> Log.build offset
                  K.seekMessageSet (fromIntegral offset) bytesOnDisk
                else pure bytesOnDisk
      pure (fstRecordBytes, vs)

errorPartitionResponse :: Int32 -> K.ErrorCode -> K.PartitionData
errorPartitionResponse partitionIndex ec = K.PartitionData
  { partitionIndex      = partitionIndex
  , errorCode           = ec
  , highWatermark       = (-1)
  , recordBytes         = (Just "")
  , lastStableOffset    = (-1) -- TODO
  , abortedTransactions = K.NonNullKaArray V.empty -- TODO
    -- TODO: for performance reason, we don't implement logStartOffset now
  , logStartOffset      = (-1)
  }
{-# INLINE errorPartitionResponse #-}

-------------------------------------------------------------------------------

-- NOTE: condition is True -> continue; False -> break
vecFoldWhileM :: Monad m => Vector b -> a -> (a -> b -> m (a, Bool)) -> m a
vecFoldWhileM !bs !a !f =
  case V.uncons bs of
    Nothing -> pure a
    Just (b, bs') -> do
      (a', cont) <- f a b
      if cont then vecFoldWhileM bs' a' f else pure a'

fromLeft' :: Either a b -> a
fromLeft' (Left x) = x
fromLeft' _        = error "This should not be reached (fromLeft')"
