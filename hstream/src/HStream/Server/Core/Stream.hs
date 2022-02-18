{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  ) where

import           Control.Exception                (throwIO)
import           Control.Monad                    (unless, void)
import qualified Data.ByteString                  as BS
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (isJust)
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)
import           Network.GRPC.HighLevel.Generated
import           ZooKeeper                        (zooExists)

import           HStream.Connector.HStore         (transToStreamName)
import           HStream.Server.Core.Common       (deleteStoreStream)
import           HStream.Server.Exception         (FoundActiveSubscription (FoundActiveSubscription),
                                                   StreamNotExist (StreamNotExist),
                                                   ZkNodeExists (ZkNodeExists))
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Handler.Common    (checkIfSubsOfStreamActive,
                                                   createStreamRelatedPath,
                                                   removeStreamRelatedPath)
import           HStream.Server.Persistence       (mkPartitionKeysPath,
                                                   streamRootPath,
                                                   tryGetChildren)
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

-------------------------------------------------------------------------------

-- createStream will create a stream with a default partition
createStream :: HasCallStack => ServerContext -> API.Stream -> IO (ServerResponse 'Normal API.Stream)
createStream ServerContext{..} stream@API.Stream{..} = do
  let name = textToCBytes streamStreamName
  keys <- tryGetChildren zkHandle $ mkPartitionKeysPath name
  -- If there is a previous stream with same name failed to perform a deleted operation and
  -- did not retry, or a client try to create a stream already existed.
  unless (null keys) $ throwIO (ZkNodeExists streamStreamName)
  createStreamRelatedPath zkHandle name
  S.createStream scLDClient (transToStreamName streamStreamName) $
    S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  returnResp stream

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO (ServerResponse 'Normal Empty)
deleteStream sc@ServerContext{..} API.DeleteStreamRequest{..} = do
  zNodeExists <- checkZkPathExist
  storeExists <- checkStreamExist
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
          S.archiveStream scLDClient streamName
        else storeDelete
      cleanZkNode

    storeDelete  = void $ deleteStoreStream sc streamName deleteStreamRequestIgnoreNonExist

    streamName       = transToStreamName deleteStreamRequestStreamName
    nameCB           = textToCBytes deleteStreamRequestStreamName
    checkZkPathExist = isJust <$> zooExists zkHandle (streamRootPath <> "/" <> nameCB)
    checkStreamExist = S.doesStreamExist scLDClient streamName
    checkIfActive    = checkIfSubsOfStreamActive zkHandle nameCB
    cleanZkNode      = removeStreamRelatedPath zkHandle nameCB

listStreams
  :: HasCallStack
  => ServerContext
  -> API.ListStreamsRequest
  -> IO (V.Vector API.Stream)
listStreams ServerContext{..} API.ListStreamsRequest = do
  streams <- S.findStreams scLDClient S.StreamTypeStream
  V.forM (V.fromList streams) $ \stream -> do
    r <- S.getStreamReplicaFactor scLDClient stream
    return $ API.Stream (Text.pack . S.showStreamName $ stream) (fromIntegral r)

appendStream :: ServerContext -> API.AppendRequest -> Maybe Text -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest{..} partitionKey = do
  timestamp <- getProtoTimestamp
  let payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . API.hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
      streamID = S.mkStreamId S.StreamTypeStream streamName
      key = textToCBytes <$> partitionKey
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)

  logId <- S.getUnderlyingLogId scLDClient streamID key
  S.AppendCompletion {..} <- S.appendBatchBS scLDClient logId (V.toList payloads) cmpStrategy Nothing
  let records = V.zipWith (\_ idx -> API.RecordId appendCompLSN idx) appendRequestRecords [0..]
  return $ API.AppendResponse appendRequestStreamName records
