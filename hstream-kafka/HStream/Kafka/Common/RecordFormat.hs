module HStream.Kafka.Common.RecordFormat
  ( RecordFormat (..)
  , recordBytesSize
    -- * Helpers
  , seekMessageSet
  ) where

import           Control.Monad
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import           Data.Int
import           GHC.Generics            (Generic)

import qualified Kafka.Protocol.Encoding as K

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
