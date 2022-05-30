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
import           Control.Monad                     (unless, when, forM_)
import qualified Data.ByteString                   as BS
import           Data.Maybe                        (fromMaybe)
import           Data.Text                         (Text)
import qualified Data.Text                         as Text
import qualified Data.Vector                       as V
import           GHC.Stack                         (HasCallStack)
import Data.Word (Word32)
import           Network.GRPC.HighLevel.Generated

import           HStream.Connector.HStore          (transToStreamName)
import           HStream.Server.Exception          (InvalidArgument (..),
                                                    StreamNotExist (..))
import qualified HStream.Server.HStreamApi         as API
import           HStream.Server.Persistence.Object (getSubscriptionWithStream,
                                                    updateSubscription)
import           HStream.Server.Types              (ServerContext (..), getShardName, getShard)
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
  streamBacklogDuration = backlogSec, streamShardCount = shardCount, ..} = do
  when (streamReplicationFactor == 0) $ throwIO (InvalidArgument "Stream replicationFactor cannot be zero")
  let streamId = transToStreamName streamStreamName
      attrs = S.def{ S.logReplicationFactor = S.defAttr1 $ fromIntegral streamReplicationFactor
                   , S.logBacklogDuration   = S.defAttr1 $
                      if backlogSec > 0 then Just $ fromIntegral backlogSec else Nothing}
  catch (S.createStream scLDClient streamId attrs) (\(_ :: S.EXISTS) -> throwIO StreamExists)
  let shards = if shardCount <= 0 then 1 else shardCount
  let partions :: [Word32] = [0..shards]
  forM_ partions $ \idx -> do
    S.createStreamPartition scLDClient streamId (Just . getShardName $ fromIntegral idx)
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
    attrs <- S.getStreamLogAttrs scLDClient stream
    -- FIXME: should the default value be 0?
    let r = fromMaybe 0 . S.attrValue . S.logReplicationFactor $ attrs
        b = fromMaybe 0 . fromMaybe Nothing . S.attrValue . S.logBacklogDuration $ attrs
    shardCnt <- length <$> S.listStreamPartitions scLDClient stream
    return $ API.Stream (Text.pack . S.showStreamName $ stream) (fromIntegral r) (fromIntegral b) (fromIntegral $ shardCnt - 1)

appendStream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest {appendRequestStreamName = sName,
  appendRequestRecords = records} partitionKey = do
  timestamp <- getProtoTimestamp
  let payload = encodeBatch . API.HStreamRecordBatch $
        encodeRecord . updateRecordTimestamp timestamp <$> records
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  logId <- getShard scLDClient streamID partitionKey
  S.AppendCompletion {..} <- S.appendCompressedBS scLDClient logId payload cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder streamName (fromIntegral $ length records)
  let rids = V.zipWith (API.RecordId logId) (V.replicate (length records) appendCompLSN) (V.fromList [0..])
  return $ API.AppendResponse sName rids
  where
    streamName  = textToCBytes sName
    streamID    = S.mkStreamId S.StreamTypeStream streamName

--------------------------------------------------------------------------------

append0Stream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
append0Stream ServerContext{..} API.AppendRequest{..} partitionKey = do
  timestamp <- getProtoTimestamp
  let payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . API.hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
      streamID = S.mkStreamId S.StreamTypeStream streamName
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  logId <- getShard scLDClient streamID partitionKey
  S.AppendCompletion {..} <- S.appendBatchBS scLDClient logId (V.toList payloads) cmpStrategy Nothing
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  Stats.stream_time_series_add_append_in_records scStatsHolder streamName (fromIntegral $ length appendRequestRecords)
  let records = V.zipWith (\_ idx -> API.RecordId logId appendCompLSN idx) appendRequestRecords [0..]
  return $ API.AppendResponse appendRequestStreamName records

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
