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

import qualified Data.ByteString.Lazy      as BL
import           HStream.Processing.Type
import qualified HStream.Server.HStreamApi as API
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
    -- readRecordsWithoutCkp :: StreamName -> IO [SourceRecord]
    withReadRecordsWithoutCkp ::
      StreamName ->
      (BL.ByteString -> Maybe BL.ByteString) ->
      (BL.ByteString -> Maybe BL.ByteString) ->
      ([SourceRecord] -> IO (IO (), IO ())) ->
      IO ()
  }

data SinkConnector = SinkConnector
  { writeRecord ::
      (BL.ByteString -> Maybe BL.ByteString) ->
      (BL.ByteString -> Maybe BL.ByteString) ->
      SinkRecord ->
      IO ()
  }
