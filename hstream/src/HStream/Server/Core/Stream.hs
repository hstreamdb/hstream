{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  , append0Stream
  , FoundActiveSubscription (..)
  , StreamExists (..)
  , RecordTooBig (..)
  ) where

import           Control.Exception                (Exception (displayException),
                                                   catch, throwIO)
import           Control.Monad                    (unless, void, when)
import qualified Data.ByteString                  as BS
import           Data.Maybe                       (fromMaybe, isJust)
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)
import           Network.GRPC.HighLevel.Generated
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (zooExists, zooMulti)
import           ZooKeeper.Exception              (ZNODEEXISTS)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Common       (deleteStoreStream)
import           HStream.Server.Exception         (InvalidArgument (..),
                                                   StreamNotExist (..))
import           HStream.Server.Handler.Common    (checkIfSubsOfStreamActive,
                                                   removeStreamRelatedPath)
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Persistence       (streamRootPath)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-------------------------------------------------------------------------------

-- createStream will create a stream with a default partition
-- FIXME: Currently, creating a stream which partially exists will do the job
-- but return failure response
createStream :: HasCallStack => ServerContext -> API.Stream -> IO (ServerResponse 'Normal API.Stream)
createStream ServerContext{..} stream@API.Stream{
  streamBacklogDuration = backlogSec, ..} = do
  when (streamReplicationFactor == 0) $ throwIO (InvalidArgument "Stream replicationFactor cannot be zero")
  let nameCB   = textToCBytes streamStreamName
      streamId = transToStreamName streamStreamName
      attrs = S.def{ S.logReplicationFactor = S.defAttr1 $ fromIntegral streamReplicationFactor
                   , S.logBacklogDuration   = S.defAttr1 $
                      if backlogSec > 0 then Just $ fromIntegral backlogSec else Nothing}
  zNodesExist <- catch (createStreamRelatedPath zkHandle nameCB >> return False)
                       (\(_::ZNODEEXISTS) -> return True)
  storeExists <- catch (S.createStream scLDClient streamId attrs
                        >> return False)
                       (\(_ :: S.EXISTS) -> return True)
  when (storeExists || zNodesExist) $ do
    Log.warning $ "Stream exists in" <> (if zNodesExist then " <zk path>" else "") <> (if storeExists then " <hstore>." else ".")
    throwIO $ StreamExists zNodesExist storeExists
  returnResp stream

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO (ServerResponse 'Normal Empty)
deleteStream sc@ServerContext{..} API.DeleteStreamRequest{..} = do
  zNodeExists <- isJust <$> zooExists zkHandle (streamRootPath <> "/" <> nameCB)
  storeExists <- S.doesStreamExist scLDClient streamId
  case (zNodeExists, storeExists) of
    -- Normal path
    (True, True) -> normalDelete
    -- Deletion of a stream was incomplete, failed to clear zk path,
    -- we will get here when client retry the delete request
    (True, False) -> cleanZkNode
    -- Abnormal Path: Since we always delete stream before clear zk path, get here may
    -- means some unexpected error. Since it is a delete request and we just want to destroy the resource,
    -- so it should be fine to just delete the stream instead of throw an exception
    (False, True) -> storeDelete
    -- Concurrency problem, or we finished delete request but client lose the
    -- response and retry
    (False, False) -> unless deleteStreamRequestIgnoreNonExist $ throwIO StreamNotExist
  returnResp Empty
  where
    normalDelete = do
      isActive <- checkIfActive
      if isActive
        then do
          unless deleteStreamRequestForce $ throwIO FoundActiveSubscription
          S.archiveStream scLDClient streamId
        else storeDelete
      cleanZkNode
    storeDelete  = void $ deleteStoreStream sc streamId deleteStreamRequestIgnoreNonExist

    streamId      = transToStreamName deleteStreamRequestStreamName
    nameCB        = textToCBytes deleteStreamRequestStreamName
    checkIfActive = checkIfSubsOfStreamActive zkHandle nameCB
    cleanZkNode   = removeStreamRelatedPath zkHandle nameCB

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

appendStream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest {appendRequestStreamName = sName,
  appendRequestRecords = records} partitionKey = do
  timestamp <- getProtoTimestamp
  let payload = encodeBatch . API.HStreamRecordBatch $
        encodeRecord . updateRecordTimestamp timestamp <$> records
      payloadSize = BS.length payload
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  logId <- S.getUnderlyingLogId scLDClient streamID key
  -- TODO
  -- S.AppendCompletion {..} <- S.appendCompressedBS scLDClient logId payload cmpStrategy Nothing
  S.AppendCompletion {..} <- S.appendBS scLDClient logId payload Nothing
  let rids = V.zipWith (API.RecordId logId) (V.replicate (length records) appendCompLSN) (V.fromList [0..])
  return $ API.AppendResponse sName rids
  where
    streamName  = textToCBytes sName
    streamID    = S.mkStreamId S.StreamTypeStream streamName
    key         = textToCBytes <$> partitionKey
--------------------------------------------------------------------------------
append0Stream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
append0Stream ServerContext{..} API.AppendRequest{..} partitionKey = do
  timestamp <- getProtoTimestamp
  let payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . API.hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
      streamID = S.mkStreamId S.StreamTypeStream streamName
      key = textToCBytes <$> partitionKey
  when (payloadSize > scMaxRecordSize) $ throwIO RecordTooBig
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)

  logId <- S.getUnderlyingLogId scLDClient streamID key
  S.AppendCompletion {..} <- S.appendBatchBS scLDClient logId (V.toList payloads) cmpStrategy Nothing
  let records = V.zipWith (\_ idx -> API.RecordId logId appendCompLSN idx) appendRequestRecords [0..]
  return $ API.AppendResponse appendRequestStreamName records
--------------------------------------------------------------------------------

createStreamRelatedPath :: ZHandle -> CBytes -> IO ()
createStreamRelatedPath zk streamName = do
  -- rootPath/streams/{streamName}
  -- rootPath/streams/{streamName}/keys
  -- rootPath/streams/{streamName}/subscriptions
  -- rootPath/lock/streams/{streamName}
  -- rootPath/lock/streams/{streamName}/subscriptions
  let streamPath = P.streamRootPath <> "/" <> streamName
      keyPath    = P.mkPartitionKeysPath streamName
      subPath    = P.mkStreamSubsPath streamName
      lockPath   = P.streamLockPath <> "/" <> streamName
      streamSubLockPath = P.mkStreamSubsLockPath streamName
  void $ zooMulti zk $ P.createPathOp <$> [streamPath, keyPath, subPath, lockPath, streamSubLockPath]

data FoundActiveSubscription = FoundActiveSubscription
  deriving (Show)
instance Exception FoundActiveSubscription

data RecordTooBig = RecordTooBig
  deriving (Show)
instance Exception RecordTooBig

data StreamExists = StreamExists Bool Bool
  deriving (Show)
instance Exception StreamExists where
  displayException (StreamExists zkExists storeExists)
    | zkExists && storeExists = "StreamExists: Stream has been created"
    | zkExists    = "StreamExists: Inconsistency found. The stream was created, but not persisted to disk"
    | storeExists = "StreamExists: Inconsistency found. The stream was persisted to disk"
                 <> ", but no record of creating the stream is found."
    | otherwise   = "Impossible happened: Stream does not exist, but some how throw stream exist exception"
