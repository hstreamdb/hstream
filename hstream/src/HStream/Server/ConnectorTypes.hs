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
    getCurrentTimestamp
  )
where

import qualified Data.ByteString.Lazy      as BL
import           Data.Int                  (Int64)
import qualified Data.Text                 as T
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word                 (Word64)
import qualified HStream.Server.HStreamApi as API

import qualified HStream.Server.HStreamApi as API

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
    withReadRecordsWithoutCkp :: StreamName
                              -> (BL.ByteString -> Maybe BL.ByteString)
                              -> (BL.ByteString -> Maybe BL.ByteString)
                              -> ([SourceRecord] -> IO ())
                              -> IO ()
  }

data SinkConnector = SinkConnector
  { writeRecord :: (BL.ByteString -> Maybe BL.ByteString)
                -> (BL.ByteString -> Maybe BL.ByteString)
                -> SinkRecord
                -> IO ()
  }

posixTimeToMilliSeconds :: POSIXTime -> Timestamp
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Timestamp
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime
