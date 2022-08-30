{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  , append0Stream
  , FoundSubscription (..)
  , StreamExists (..)
  , RecordTooBig (..)
  ) where

import           Control.Exception                 (Exception (displayException),
                                                    catch, throwIO)
import           Control.Monad                     (unless, when)
import qualified Data.ByteString                   as BS
import           Data.Maybe                        (fromMaybe)
import           Data.Text                         (Text)
import qualified Data.Text                         as Text
import qualified Data.Vector                       as V
import           GHC.Stack                         (HasCallStack)
import           Network.GRPC.HighLevel.Generated

import           HStream.Connector.HStore          (transToStreamName)
import           HStream.Server.Exception          (InvalidArgument (..),
                                                    StreamNotExist (..))
import qualified HStream.Server.HStreamApi         as API
import           HStream.Server.Persistence.Object (getSubscriptionWithStream,
                                                    updateSubscription)
import           HStream.Server.Types              (ServerContext (..))
import qualified HStream.Stats                     as Stats
import qualified HStream.Store                     as S
import           HStream.ThirdParty.Protobuf       as PB
import           HStream.Utils

-------------------------------------------------------------------------------

-- createStream will create a stream with a default partition
-- FIXME: Currently, creating a stream which partially exists will do the job
-- but return failure response
createStream :: HasCallStack => ServerContext -> API.Stream -> IO (ServerResponse 'Normal API.Stream)
createStream ServerContext{..} stream@API.Stream{
  streamBacklogDuration = backlogSec, ..} = do
  when (streamReplicationFactor == 0) $ throwIO (InvalidArgument "Stream replicationFactor cannot be zero")
  let streamId = transToStreamName streamStreamName
      attrs = S.def{ S.logReplicationFactor = S.defAttr1 $ fromIntegral streamReplicationFactor
                   , S.logBacklogDuration   = S.defAttr1 $
                      if backlogSec > 0 then Just $ fromIntegral backlogSec else Nothing}
  catch (S.createStream scLDClient streamId attrs) (\(_ :: S.EXISTS) -> throwIO StreamExists)
  returnResp stream

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO (ServerResponse 'Normal Empty)
deleteStream ServerContext{..} API.DeleteStreamRequest{deleteStreamRequestForce = force,
  deleteStreamRequestStreamName = sName, ..} = do
  storeExists <- S.doesStreamExist scLDClient streamId
  if storeExists then doDelete
    else unless deleteStreamRequestIgnoreNonExist $ throwIO StreamNotExist
  returnResp Empty
  where
    streamId = transToStreamName sName
    doDelete = do
      subs <- getSubscriptionWithStream zkHandle sName
      if null subs
      then S.removeStream scLDClient streamId
      else if force
           then do
             -- TODO: delete the archived stream when the stream is no longer needed
             _archivedStream <- S.archiveStream scLDClient streamId
             updateSubscription zkHandle sName (cBytesToText $ S.getArchivedStreamName _archivedStream)
           else
             throwIO FoundSubscription

listStreams
  :: HasCallStack
  => ServerContext
  -> API.ListStreamsRequest
  -> IO (V.Vector API.Stream)
listStreams ServerContext{..} API.ListStreamsRequest = do
  streams <- S.findStreams scLDClient S.StreamTypeStream
  V.forM (V.fromList streams) $ \stream -> do
    -- FIXME: should the default value be 0?
    r <- fromMaybe 0 . S.attrValue . S.logReplicationFactor <$> S.getStreamLogAttrs scLDClient stream
    b <- fromMaybe 0 . fromMaybe Nothing . S.attrValue . S.logBacklogDuration <$> S.getStreamLogAttrs scLDClient stream
    return $ API.Stream (Text.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b)

appendStream :: ServerContext -> Text -> API.BatchedRecord -> Text -> IO API.AppendResponse
appendStream ServerContext{..} sName record@API.BatchedRecord{..} partitionKey = do
  timestamp <- getProtoTimestamp
  let payload = encodBatchRecord . updateRecordTimestamp timestamp $ record
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  logId <- S.getUnderlyingLogId scLDClient streamID key
  S.AppendCompletion {..} <- S.appendCompressedBS scLDClient logId payload cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder streamName (fromIntegral batchedRecordBatchSize)
  let rids = V.zipWith (API.RecordId logId) (V.replicate (fromIntegral batchedRecordBatchSize) appendCompLSN) (V.fromList [0..])
  return $ API.AppendResponse sName rids
  where
    streamName  = textToCBytes sName
    streamID    = S.mkStreamId S.StreamTypeStream streamName
    key         = textToCBytes <$> (if Text.null partitionKey then Nothing else Just partitionKey)

--------------------------------------------------------------------------------

append0Stream :: ServerContext -> Text -> API.BatchedRecord -> Text -> IO API.AppendResponse
append0Stream ServerContext{..} sName record@API.BatchedRecord{..} partitionKey = do
  timestamp <- getProtoTimestamp
  let payload = encodBatchRecord . updateRecordTimestamp timestamp $ record
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  logId <- S.getUnderlyingLogId scLDClient streamID key
  S.AppendCompletion {..} <- S.appendBatchBS scLDClient logId [payload] cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder streamName (fromIntegral batchedRecordBatchSize)
  let rids = V.zipWith (API.RecordId logId) (V.replicate (fromIntegral batchedRecordBatchSize) appendCompLSN) (V.fromList [0..])
  return $ API.AppendResponse sName rids
 where
   streamName  = textToCBytes sName
   streamID    = S.mkStreamId S.StreamTypeStream streamName
   key         = textToCBytes <$> (if Text.null partitionKey then Nothing else Just partitionKey)

--------------------------------------------------------------------------------

data FoundSubscription = FoundSubscription
  deriving (Show)
instance Exception FoundSubscription

data RecordTooBig = RecordTooBig
  deriving (Show)
instance Exception RecordTooBig

data StreamExists = StreamExists
  deriving (Show)
instance Exception StreamExists where
  displayException StreamExists = "StreamExists: Stream has been created"
