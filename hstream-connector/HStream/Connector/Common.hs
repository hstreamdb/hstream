{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Connector.Common
  ( SourceConnector (..),
    SourceConnectorWithoutCkp (..),
    SinkConnector (..),
  )
where

import qualified HStream.Server.HStreamApi as API
import           HStream.Connector.Type
import           RIO

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
