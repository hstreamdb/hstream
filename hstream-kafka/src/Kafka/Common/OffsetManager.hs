{-# LANGUAGE BangPatterns #-}

module Kafka.Common.OffsetManager
  ( OffsetManager
  , newOffsetManager
  , withOffset
  , withOffsetN
  , cleanOffsetCache
  , getOldestOffset
  , getLatestOffset
  ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.ByteString                  (ByteString)
import qualified Data.HashTable.IO                as H
import           Data.Int
import           Data.Word
import           GHC.Stack                        (HasCallStack)

import qualified HStream.Store                    as S
import qualified HStream.Store.Internal.LogDevice as S
import           Kafka.Common.RecordFormat
import qualified Kafka.Protocol.Encoding          as K

-------------------------------------------------------------------------------

type HashTable k v = H.BasicHashTable k v

data OffsetManager = OffsetManager
  { offsets     :: HashTable Word64 (MVar Int64)
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
cleanOffsetCache OffsetManager{..} logid = H.delete offsets logid

getOldestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getOldestOffset OffsetManager{..} logid = do
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do
       -- Actually, we only need the first lsn but there is no easy way to get
       Just . offset <$> readOneRecord reader logid S.LSN_MIN S.LSN_MAX

getLatestOffset :: HasCallStack => OffsetManager -> Word64 -> IO (Maybe Int64)
getLatestOffset OffsetManager{..} logid = do
  -- FIXME: first check is empty log seems blocking.
  isEmpty <- S.isLogEmpty store logid
  if isEmpty
     then pure Nothing
     else do tailLsn <- S.getTailLSN store logid
             Just . offset <$> readOneRecord reader logid tailLsn tailLsn

-- TODO
-- getOffsetByTime :: HasCallStack => OffsetManager -> Word64 -> Int64 -> IO Int64
-- getOffsetByTime OffsetManager{..} logid timestamp = undefined

-------------------------------------------------------------------------------

-- Return the first read RecordFormat
--
-- FIXME: what happens when read an empty log?
readOneRecord
  :: HasCallStack
  => S.LDReader -> Word64 -> S.LSN -> S.LSN -> IO RecordFormat
readOneRecord reader logid start end = finally acquire release
  where
    acquire = do
      S.readerSetTimeout reader 1000
      S.readerStartReading reader logid start end
      dataRecords <- S.readerRead @ByteString reader 1
      case dataRecords of
        [S.DataRecord{..}] -> K.runGet recordPayload
        xs ->   -- TODO
          ioError $ userError $ "Invalid reader result " <> show xs
    release = do
      isReading <- S.readerIsReading reader logid
      when isReading $ S.readerStopReading reader logid
