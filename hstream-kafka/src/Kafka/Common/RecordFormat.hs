module Kafka.Common.RecordFormat
  ( RecordFormat (..)
  ) where

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
