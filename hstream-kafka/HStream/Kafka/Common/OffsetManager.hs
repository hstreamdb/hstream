{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE OverloadedRecordDot #-}

module HStream.Kafka.Common.OffsetManager
  ( OffsetManager
  , newOffsetManager
  , initOffsetReader
  , withOffset
  , withOffsetN
  , cleanOffsetCache
  , getOldestOffset
  , getLatestOffset
  , getLatestOffsetWithLsn
  , getOffsetByTimestamp

    -- * SparseOffset
  , getOldestSparseOffset
  , getLatestHeadSparseOffsetWithLsn
  , getLatestHeadSparseOffset
  , getSparseOffsetByTimestamp
  , composeSparseOffset
  , sparseOffsetToLsn
  , calNextSparseOffset
  ) where

import           Control.Concurrent
import           Control.Exception
import           Data.Bits
import qualified Data.HashTable.IO                 as H
import           Data.Int
import           Data.Maybe
import           Data.Word
import           Foreign.ForeignPtr                (newForeignPtr_)
import           Foreign.Ptr                       (nullPtr)
import           GHC.Stack                         (HasCallStack)

import           HStream.Kafka.Common.Read
import           HStream.Kafka.Common.RecordFormat
import qualified HStream.Store                     as S
import qualified HStream.Store.Internal.LogDevice  as S

-------------------------------------------------------------------------------

type HashTable k v = H.BasicHashTable k v

data OffsetManager = OffsetManager
  { offsets     :: !(HashTable Word64{- logid -} (MVar Int64))
    -- ^ Offsets cache
    --
    -- TODO:
    -- * use FastMutInt as value (?)
  , offsetsLock :: !(MVar ())
  , store       :: !S.LDClient
  , reader      :: !S.LDReader
  }

newOffsetManager :: S.LDClient -> IO OffsetManager
newOffsetManager store = do
  offsets <- H.new
  offsetsLock <- newMVar ()
  reader <- newForeignPtr_ nullPtr   -- Must be initialized later
  pure OffsetManager{..}

initOffsetReader :: OffsetManager -> IO OffsetManager
initOffsetReader om = do
  -- set maxLogs to 10 seems enough
  reader <- S.newLDReader om.store 10 Nothing
  -- Always wait. Otherwise, the reader will return empty result when timeout
  -- and we cannot know whether the log is empty or timeout.
  S.readerSetTimeout reader (-1)
  S.readerSetWaitOnlyWhenNoData reader
  pure om{reader = reader}

withOffset :: OffsetManager -> Word64 -> (Int64 -> IO a) -> IO a
withOffset m logid = withOffsetN m logid 0

-- thread safe version
--
-- NOTE: n must >= 1 and < (maxBound :: Int32)
withOffsetN :: OffsetManager -> Word64 -> Int64 -> (Int64 -> IO a) -> IO a
withOffsetN m@OffsetManager{..} logid n f = do
  m_offset <- H.lookup offsets logid
  case m_offset of
    Just offset -> doUpdate offset
    Nothing -> do
      offset <- withMVar offsetsLock $ \_ -> do
        -- There may other threads that have already created the offset, because
        -- we have the 'offsetsLock' after the 'H.lookup' operation. So we need
        -- to check again.
        m_offset' <- H.lookup offsets logid
        maybe (do o <- catch (do mo <- getLatestOffset m logid
                                 pure $ fromMaybe (-1) mo)
                             (\(_ :: S.NOTFOUND) -> pure (-1))
                  ov <- newMVar o
                  H.insert offsets logid ov
                  pure ov)
              pure
              m_offset'
      doUpdate offset
  where
    -- FIXME: currently, any exception happen in f will cause the offset not
    -- updated. This may cause inconsistent between the offset and the actual
    -- stored data.
    doUpdate offset = modifyMVar offset $ \o -> do
      let !o' = o + n
      !a <- f o'
      pure (o', a)

cleanOffsetCache :: OffsetManager -> Word64 -> IO ()
cleanOffsetCache OffsetManager{..} = H.delete offsets

-- | Get the oldest offset of a log
getOldestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getOldestOffset OffsetManager{..} logid =
  -- Actually, we only need the first lsn but there is no easy way to get
  fmap (calOffset . third) <$> readOneRecordBypassGap store reader logid (pure (S.LSN_MIN, S.LSN_MAX))

getLatestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getLatestOffset o logid = (fmap fst) <$> getLatestOffsetWithLsn o logid

getLatestOffsetWithLsn
  :: HasCallStack
  => OffsetManager -> Word64 -> IO (Maybe (Int64, S.LSN))
getLatestOffsetWithLsn OffsetManager{..} logid = do
  m <- readLastOneRecord store reader logid
  pure $ do (lsn, record) <- m
            pure (offset record, lsn)

-- 1. Timestamp less than the first record's timestamp will return the first offset
-- 2. Timestamp greater than the last record's timestamp will retuen Nothing
getOffsetByTimestamp :: HasCallStack => OffsetManager -> Word64 -> Int64 -> IO (Maybe Int64)
getOffsetByTimestamp OffsetManager{..} logid timestamp = do
  let getLsn = do lsn <- S.findTime store logid timestamp S.FindKeyStrict
                  tailLsn <- S.getTailLSN store logid
                  if lsn > tailLsn
                     then pure (S.LSN_INVALID, S.LSN_INVALID)
                     -- here we donot use (lsn, lsn) because this may result in
                     -- a gap or empty record.
                     else pure (lsn, tailLsn)
   in fmap (calOffset . third) <$> readOneRecordBypassGap store reader logid getLsn

-- Suppose we have three batched records:
--
-- record0, record1, record2
-- [0,      1-5,     6-10]
--
-- Offsets stored are: 0, 5, 10
calOffset :: RecordFormat -> Int64
calOffset RecordFormat{..} = offset + 1 - fromIntegral batchLength

-------------------------------------------------------------------------------
-- SparseOffset

-- For performance reason(to bypass reading record), we directly use the
-- trim point as the oldest lsn.
--
-- If the log is nonempty and was never trimmed, the trim point is LSN_INVALID(0)
-- which is different from the actual oldest lsn.
getOldestSparseOffset :: OffsetManager -> Word64 -> IO (Maybe Int64)
getOldestSparseOffset OffsetManager{..} logid = do
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do
       lsn <- S.getLogHeadAttrsTrimPoint =<< S.getLogHeadAttrs store logid
       pure $ Just $ composeSparseOffset lsn 0

-- Get the head of last SparseOffset.
--
-- Note this return the start offset of the latest batch, which is diff from
-- 'getLatestOffsetWithLsn'
getLatestHeadSparseOffsetWithLsn
  :: OffsetManager -> Word64 -> IO (Maybe (Int64, S.LSN))
getLatestHeadSparseOffsetWithLsn OffsetManager{..} logid = do
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do
       tailLsn <- S.getTailLSN store logid
       pure $ Just (composeSparseOffset tailLsn 0, tailLsn)

getLatestHeadSparseOffset :: OffsetManager -> Word64 -> IO (Maybe Int64)
getLatestHeadSparseOffset o logid =
  (fmap fst) <$> getLatestHeadSparseOffsetWithLsn o logid

getSparseOffsetByTimestamp :: HasCallStack => OffsetManager -> Word64 -> Int64 -> IO (Maybe Int64)
getSparseOffsetByTimestamp OffsetManager{..} logid timestamp = do
  lsn <- S.findTime store logid timestamp S.FindKeyStrict
  tailLsn <- S.getTailLSN store logid
  if lsn > tailLsn
     then pure Nothing
     -- FIXME: the lsn here may be a gap, do we need to handle this?
     else pure $ Just $ composeSparseOffset lsn 0

-- epoch: 20bit, esn: 20bit, record_index: 24bit
--
-- Actually, epoch will use 19 bits
composeSparseOffset :: Word64 -> Int32 -> Int64
composeSparseOffset lsn idx =
  let epoch = lsn `shiftR` 32
      esn = lsn .&. 0xffffffff
      -- We use 40 bits for lsn, but it should less than 2^39, since
      -- offset is Int64 and we don't want a negative offset.
   in if lsn > 2^(39 :: Int) - 1 || idx > 2^(24 :: Int) - 1
         then error "SparseOffset overflow!"
         else fromIntegral $ (epoch `shiftL` 44) .|. (esn `shiftL` 24) .|. (fromIntegral idx)

sparseOffsetToLsn :: Int64 -> Word64
sparseOffsetToLsn offset = fromIntegral $ offset `shiftR` 24

calNextSparseOffset :: Int64 -> Int64
calNextSparseOffset offset =
  let lsn = offset `shiftR` 24
   in if lsn < 2^(39 :: Int) - 1
         then (offset `shiftR` 24 + 1) `shiftL` 24
         else error "SparseOffset overflow!"

-------------------------------------------------------------------------------

third :: (a, b, c) -> c
third (_, _, x) = x
{-# INLINE third #-}
