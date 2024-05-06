module HStream.Kafka.Common.RecordFormat
  ( Record (..)
  , RecordFormat (..)
  , recordBytesSize
    -- * Helpers
  , seekMessageSet
  , trySeekMessageSet
  ) where

import           Control.Monad
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import           Data.Int
import           GHC.Generics            (Generic)

import qualified HStream.Logger          as Log
import qualified HStream.Store           as S
import qualified Kafka.Protocol.Encoding as K

-- | Record is the smallest unit of data in HStream Kafka.
--
-- For Fetch handler
data Record = Record
  { recordFormat :: !RecordFormat
  , recordLsn    :: !S.LSN
  } deriving (Show)

-- on-disk format
data RecordFormat = RecordFormat
  { version     :: {-# UNPACK #-} !Int8
    -- ^ Currently, the version is always 0.
  , offset      :: {-# UNPACK #-} !Int64
    -- ^ Max offset in the batch
  , batchLength :: {-# UNPACK #-} !Int32
    -- ^ Total number of records in the batch.
  , recordBytes :: {-# UNPACK #-} !K.CompactBytes
    -- ^ The BatchRecords data.
  } deriving (Generic, Show)

instance K.Serializable RecordFormat

-- Real size of the recordBytes.
--
-- Actually, the size is not accurate, because the recordBytes is a CompactBytes
recordBytesSize :: ByteString -> Int
recordBytesSize bs = BS.length bs - 13{- 1(version) + 8(offset) + 4(batchLength) -}
{-# INLINE recordBytesSize #-}

-- Only MessageSet need to be seeked.
--
-- https://kafka.apache.org/documentation/#messageset
seekMessageSet :: Int32 -> ByteString -> IO ByteString
seekMessageSet i bs{- MessageSet data -} =
  let parser = replicateM_ (fromIntegral i) $ do
                 void $ K.takeBytes 8{- MessageSet.offset(8) -}
                 len <- K.get @Int32 {- MessageSet.message_size(4) -}
                 void $ K.takeBytes (fromIntegral len)
   in snd <$> K.runParser' parser bs
{-# INLINE seekMessageSet #-}

-- | Try to bypass the records if the fetch offset is not the first record
-- in the batch.
trySeekMessageSet
  :: Record   -- ^ The first record in the batch
  -> Int64    -- ^ The fetch offset
  -> IO (ByteString, S.LSN)
trySeekMessageSet r fetchOffset = do
  let bytesOnDisk = K.unCompactBytes r.recordFormat.recordBytes
  magic <- K.decodeRecordMagic bytesOnDisk
  fstRecordBytes <-
    if magic >= 2
       then pure bytesOnDisk
        else do
          let absStartOffset = r.recordFormat.offset + 1 - fromIntegral r.recordFormat.batchLength
              offset = fetchOffset - absStartOffset
          if offset > 0
             then do Log.debug1 $ "Seek MessageSet " <> Log.build offset
                     seekMessageSet (fromIntegral offset) bytesOnDisk
             else pure bytesOnDisk
  pure (fstRecordBytes, r.recordLsn)
