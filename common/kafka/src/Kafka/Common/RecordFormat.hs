module Kafka.Common.RecordFormat
  ( RecordFormat (..)
  ) where

import           Data.ByteString         (ByteString)
import           Data.Int
import           GHC.Generics            (Generic)

import qualified Kafka.Protocol.Encoding as K

-- Format to store in logdevice
data RecordFormat = RecordFormat
  { offset      :: Int64
  , batchLength :: Int32
  , recordBytes :: ByteString
  } deriving (Generic, Show)

instance K.Serializable RecordFormat
