{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE StrictData        #-}

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
  } deriving Show

data SinkRecord = SinkRecord
  { snkStream    :: StreamName,
    snkKey       :: Maybe BL.ByteString,
    snkValue     :: BL.ByteString,
    snkTimestamp :: Timestamp
  } deriving Show

data TimestampedKey k = TimestampedKey
  { tkKey       :: k,
    tkTimestamp :: Timestamp
  }

mkTimestampedKey :: k -> Timestamp -> TimestampedKey k
mkTimestampedKey key timestamp =
  TimestampedKey
    { tkKey = key,
      tkTimestamp = timestamp
    }
