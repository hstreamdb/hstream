{-# LANGUAGE BangPatterns #-}

module HStream.Kafka.Common.OffsetManager
  ( OffsetManager
  , newOffsetManager
  , withOffset
  , withOffsetN
  , cleanOffsetCache
  , getOldestOffset
  , getLatestOffset
  , getLatestOffsetWithLsn
  , getOffsetByTimestamp
  ) where

import           Control.Concurrent
import           Control.Exception
import qualified Data.HashTable.IO                 as H
import           Data.Int
import           Data.Word
import           GHC.Stack                         (HasCallStack)

import           HStream.Kafka.Common.Read         (readOneRecord,
                                                    readOneRecordBypassGap)
import           HStream.Kafka.Common.RecordFormat
import qualified HStream.Store                     as S

-------------------------------------------------------------------------------

type HashTable k v = H.BasicHashTable k v

data OffsetManager = OffsetManager
  { offsets     :: HashTable Word64{- logid -} (MVar Int64)
    -- ^ Offsets cache
    --
    -- TODO:
    -- * use FastMutInt as value (?)
  , offsetsLock :: MVar ()
  , store       :: S.LDClient
  , reader      :: S.LDReader
  }

newOffsetManager :: S.LDClient -> Int -> IO OffsetManager
newOffsetManager store maxLogs = do
  offsets <- H.new
  offsetsLock <- newMVar ()
  reader <- S.newLDReader store (fromIntegral maxLogs) Nothing
  -- Always wait. Otherwise, the reader will return empty result when timeout
  -- and we cannot know whether the log is empty or timeout.
  S.readerSetTimeout reader (-1)
  S.readerSetWaitOnlyWhenNoData reader
  pure OffsetManager{..}

withOffset :: OffsetManager -> Word64 -> (Int64 -> IO a) -> IO a
withOffset m logid = withOffsetN m logid 0

-- thread safe version
--
-- NOTE: n must >= 1 and < (maxBound :: Int32)
withOffsetN :: OffsetManager -> Word64 -> Int64 -> (Int64 -> IO a) -> IO a
withOffsetN m@OffsetManager{..} logid n f = do
  m_offset <- H.lookup offsets logid
  case m_offset of
    Just offset -> modifyMVar offset $ \o -> do
      let !o' = o + n
      !a <- f o'
      pure (o', a)
    Nothing -> withMVar offsetsLock $ \_ -> do
      o' <- catch (do mo <- getLatestOffset m logid
                      pure $ maybe (n - 1) (+ n) mo)
                  (\(_ :: S.NOTFOUND) -> pure $ n - 1)
      H.insert offsets logid =<< newMVar o'
      f o'

cleanOffsetCache :: OffsetManager -> Word64 -> IO ()
cleanOffsetCache OffsetManager{..} = H.delete offsets

-- | Get the oldest offset of a log
getOldestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getOldestOffset OffsetManager{..} logid =
  -- Actually, we only need the first lsn but there is no easy way to get
  (fmap $ calOffset . third) <$> readOneRecordBypassGap store reader logid (pure (S.LSN_MIN, S.LSN_MAX))

getLatestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getLatestOffset o logid = (fmap fst) <$> getLatestOffsetWithLsn o logid

getLatestOffsetWithLsn
  :: HasCallStack
  => OffsetManager -> Word64 -> IO (Maybe (Int64, S.LSN))
getLatestOffsetWithLsn OffsetManager{..} logid =
  let getLsn = do tailLsn <- S.getTailLSN store logid
                  pure (tailLsn, tailLsn)
   in do m <- readOneRecord store reader logid getLsn
         pure $ do (lsn, _, record) <- m
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
   in (fmap $ calOffset . third) <$> readOneRecordBypassGap store reader logid getLsn

-------------------------------------------------------------------------------

-- Suppose we have three batched records:
--
-- record0, record1, record2
-- [0,      1-5,     6-10]
--
-- Offsets stored are: 0, 5, 10
calOffset :: RecordFormat -> Int64
calOffset RecordFormat{..} = offset + 1 - fromIntegral batchLength

third :: (a, b, c) -> c
third (_, _, x) = x
{-# INLINE third #-}
