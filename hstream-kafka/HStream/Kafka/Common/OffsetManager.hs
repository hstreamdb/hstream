{-# LANGUAGE BangPatterns #-}

module HStream.Kafka.Common.OffsetManager
  ( OffsetManager
  , newOffsetManager
  , withOffset
  , withOffsetN
  , cleanOffsetCache
  , getOldestOffset
  , getLatestOffset
  , getOffsetByTimestamp
  ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.ByteString                   (ByteString)
import qualified Data.HashTable.IO                 as H
import           Data.Int
import           Data.Word
import           GHC.Stack                         (HasCallStack)

import           HStream.Kafka.Common.RecordFormat
import qualified HStream.Logger                    as Log
import qualified HStream.Store                     as S
import qualified HStream.Store.Internal.LogDevice  as S
import qualified Kafka.Protocol.Encoding           as K

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
getOldestOffset OffsetManager{..} logid =
  -- Actually, we only need the first lsn but there is no easy way to get
  (fmap offset) <$> readOneRecord store reader logid (pure (S.LSN_MIN, S.LSN_MAX))

getLatestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getLatestOffset OffsetManager{..} logid =
  let getLsn = do tailLsn <- S.getTailLSN store logid
                  pure (tailLsn, tailLsn)
   in (fmap offset) <$> readOneRecord store reader logid getLsn

getOffsetByTimestamp :: HasCallStack => OffsetManager -> Word64 -> Int64 -> IO (Maybe Int64)
getOffsetByTimestamp OffsetManager{..} logid timestamp = do
  let getLsn = do lsn <- S.findTime store logid timestamp S.FindKeyStrict
                  pure (lsn, lsn)
   in (fmap offset) <$> readOneRecord store reader logid getLsn

-------------------------------------------------------------------------------

-- Return the first read RecordFormat
readOneRecord
  :: HasCallStack
  => S.LDClient
  -> S.LDReader
  -> Word64
  -> IO (S.LSN, S.LSN)
  -> IO (Maybe RecordFormat)
readOneRecord store reader logid getLsn = do
  -- FIXME: This method is blocking until the state can be determined or an
  -- error occurred. Directly read without check isLogEmpty will also block a
  -- while for the first time since the state can be determined.
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do (start, end) <- getLsn
             finally (acquire start end) release
  where
    acquire start end = do
      S.readerStartReading reader logid start end
      dataRecords <- S.readerReadAllowGap @ByteString reader 1
      case dataRecords of
        Right [S.DataRecord{..}] -> Just <$> K.runGet recordPayload
        _ -> do Log.fatal $ "readOneRecord read " <> Log.build logid
                         <> "with lsn (" <> Log.build start <> " "
                         <> Log.build end <> ") "
                         <> "get unexpected result "
                         <> Log.buildString' dataRecords
                ioError $ userError $ "Invalid reader result " <> show dataRecords
    release = do
      isReading <- S.readerIsReading reader logid
      when isReading $ S.readerStopReading reader logid
