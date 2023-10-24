module HStream.Kafka.Common.RecordFormat
  ( RecordFormat (..)
  , seekBatch
  , recordBytesSize
  ) where

import           Control.Monad
import           Data.ByteString         (ByteString)
import qualified Data.ByteString         as BS
import           Data.Int
import           GHC.Generics            (Generic)

import qualified Kafka.Protocol.Encoding as K

-- on-disk format
data RecordFormat = RecordFormat
  { offset      :: Int64
  , batchLength :: Int32
  , recordBytes :: K.CompactBytes
  } deriving (Generic, Show)

instance K.Serializable RecordFormat

seekBatch :: Int32 -> ByteString -> IO ByteString
seekBatch i bs =
  let parser = replicateM_ (fromIntegral i) $ do
                 void $ K.takeBytes 8{- baseOffset: Int64 -}
                 len <- K.get @Int32
                 void $ K.takeBytes (fromIntegral len)
   in snd <$> K.runParser' parser bs
{-# INLINE seekBatch #-}

recordBytesSize :: ByteString -> Int
recordBytesSize bs = BS.length bs - 12{- 8(baseOffset) + 4(batchLength) -}
{-# INLINE recordBytesSize #-}
