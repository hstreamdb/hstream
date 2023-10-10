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

getOldestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getOldestOffset OffsetManager{..} logid = do
  -- Actually, we only need the first lsn but there is no easy way to get
  (fmap $ offset . third) <$> readOneRecordBypassGap store reader logid (pure (S.LSN_MIN, S.LSN_MAX))

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

getOffsetByTimestamp :: HasCallStack => OffsetManager -> Word64 -> Int64 -> IO (Maybe Int64)
getOffsetByTimestamp OffsetManager{..} logid timestamp = do
  let getLsn = do lsn <- S.findTime store logid timestamp S.FindKeyStrict
                  pure (lsn, lsn)
   in (fmap $ offset . third) <$> readOneRecord store reader logid getLsn

-------------------------------------------------------------------------------

third :: (a, b, c) -> c
third (_, _, x) = x
{-# INLINE third #-}
