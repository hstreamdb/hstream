{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Server.ConnectorTypes
  ( SourceConnector (..),
    SourceConnectorWithoutCkp (..),
    SinkConnector (..),
    Timestamp,
    StreamName,
    Offset (..),
    SourceRecord (..),
    SinkRecord (..),
    TimestampedKey (..),
    mkTimestampedKey,
    TemporalFilter (..),
    getCurrentTimestamp
  )
where

import           Data.Time
import           Data.Time.Clock.POSIX
import qualified HStream.Server.HStreamApi as API
import           RIO
import qualified RIO.ByteString.Lazy       as BL
import qualified RIO.Text                  as T

type Timestamp = Int64 -- ms

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

data TemporalFilter = NoFilter
                    | Tumbling Timestamp
                    | Hopping Timestamp Timestamp
                    | Sliding Timestamp

-- data StreamStoreConnector =
--   StreamStoreConnector {
--     subscribeToStreams :: [StreamName] -> IO (),
--     readRecords :: IO [SourceRecord],
--     commitCheckpoint :: StreamName -> Offset -> IO (),
--     writeRecord :: SinkRecord -> IO ()
--   }

data SourceConnector = SourceConnector
  { subscribeToStream   :: StreamName -> Offset -> IO (),
    unSubscribeToStream :: StreamName -> IO (),
    readRecords         :: IO [SourceRecord],
    commitCheckpoint    :: StreamName -> Offset -> IO ()
  }

data SourceConnectorWithoutCkp = SourceConnectorWithoutCkp
  { subscribeToStreamWithoutCkp :: StreamName -> API.SpecialOffset -> IO (),
    unSubscribeToStreamWithoutCkp :: StreamName -> IO (),
    --readRecordsWithoutCkp :: StreamName -> IO [SourceRecord]
    withReadRecordsWithoutCkp :: StreamName -> ([SourceRecord] -> IO ()) -> IO ()
  }

data SinkConnector = SinkConnector
  { writeRecord :: SinkRecord -> IO ()
  }

posixTimeToMilliSeconds :: POSIXTime -> Timestamp
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Timestamp
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime
