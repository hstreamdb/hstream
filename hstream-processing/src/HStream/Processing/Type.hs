{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE NoImplicitPrelude  #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE StrictData         #-}

module HStream.Processing.Type
  ( Timestamp,
    StreamName,
    Offset (..),
    SourceRecord (..),
    SinkRecord (..),
    TimestampedKey (..),
    mkTimestampedKey,
  )
where

import           Data.Aeson
import           RIO
import qualified RIO.ByteString.Lazy as BL
import qualified RIO.Text            as T

type Timestamp = Int64

type StreamName = T.Text

data Offset
  = Earlist
  | Latest
  | Offset Word64

data SourceRecord = SourceRecord
  { srcStream    :: StreamName,
    srcOffset    :: Word64,
    srcTimestamp :: Timestamp,
    srcKey       :: Maybe BL.ByteString,
    srcValue     :: BL.ByteString
  }
  deriving (Show)

data SinkRecord = SinkRecord
  { snkStream    :: StreamName,
    snkKey       :: Maybe BL.ByteString,
    snkValue     :: BL.ByteString,
    snkTimestamp :: Timestamp
  }
  deriving (Show)

data TimestampedKey k = TimestampedKey
  { tkKey       :: k,
    tkTimestamp :: Timestamp
  }
  deriving (Generic)

deriving instance (Eq k) => Eq (TimestampedKey k)
deriving instance (Ord k) => Ord (TimestampedKey k)
deriving instance (ToJSON k) => ToJSON (TimestampedKey k)
deriving instance (ToJSON k) => ToJSONKey (TimestampedKey k)
deriving instance (FromJSON k) => FromJSON (TimestampedKey k)
deriving instance (FromJSON k) => FromJSONKey (TimestampedKey k)

mkTimestampedKey :: k -> Timestamp -> TimestampedKey k
mkTimestampedKey key timestamp =
  TimestampedKey
    { tkKey = key,
      tkTimestamp = timestamp
    }
