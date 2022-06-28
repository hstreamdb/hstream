{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Connector
  ( SourceConnector (..),
    SourceConnectorWithoutCkp (..),
    SinkConnector (..),
  )
where

import           HStream.Processing.Type
import           RIO

-- data StreamStoreConnector =
--   StreamStoreConnector {
--     subscribeToStreams :: [StreamName] -> IO (),
--     readRecords :: IO [SourceRecord],
--     commitCheckpoint :: StreamName -> Offset -> IO (),
--     writeRecord :: SinkRecord -> IO ()
--   }

data SourceConnector = SourceConnector
  { subscribeToStream :: StreamName -> Offset -> IO (),
    unSubscribeToStream :: StreamName -> IO (),
    readRecords :: IO [SourceRecord],
    commitCheckpoint :: StreamName -> Offset -> IO ()
  }

data SourceConnectorWithoutCkp = SourceConnectorWithoutCkp
  { subscribeToStreamWithoutCkp :: StreamName -> IO (),
    unSubscribeToStreamWithoutCkp :: StreamName -> IO (),
    readRecordsWithoutCkp :: StreamName -> IO [SourceRecord]
  }

data SinkConnector = SinkConnector
  { writeRecord :: SinkRecord -> IO ()
  }
