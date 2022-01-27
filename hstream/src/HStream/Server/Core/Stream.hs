{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  ) where

import qualified Data.ByteString                  as BS
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)
import           Network.GRPC.HighLevel.Generated

import           Control.Exception                (throwIO)
import           Control.Monad                    (void)
import           Data.Maybe                       (isJust)
import           HStream.Connector.HStore         (transToStreamName)
import           HStream.Server.Core.Common       (deleteStoreStream)
import           HStream.Server.Exception         (StreamNotExist (StreamNotExist))
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
import           ZooKeeper                        (zooExists)

-------------------------------------------------------------------------------

-- createStream will create a stream with a default partition
createStream :: HasCallStack => ServerContext -> API.Stream -> IO (ServerResponse 'Normal API.Stream)
createStream ServerContext{..} stream@API.Stream{..} = do
  let name = textToCBytes streamStreamName
  keys <- tryGetChildren zkHandle $ mkPartitionKeysPath name
  if null keys
    then do
      createStreamRelatedPath zkHandle name
      S.createStream scLDClient (transToStreamName streamStreamName) $
        S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
      returnResp stream
    else
      -- get here may because there is a previouse stream with same name failed to perform a deleted operation and
      -- did not retry, or a client try to create a stream already existed.
      returnErrResp StatusFailedPrecondition "Create failed because zk key path exists."

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO (ServerResponse 'Normal Empty)
deleteStream sc@ServerContext{..} API.DeleteStreamRequest{..} = do
  zNodeExists <- checkZkPathExist
  storeExists <- checkStreamExist
  case (zNodeExists, storeExists) of
    -- normal path
    (True, True) -> delete deleteStreamRequestForce
    -- if we delete stream but failed to clear zk path, we will get here when client retry the delete request
    (True, False) -> cleanZkNode >> returnResp Empty
    -- actually, it should not be here because we always delete stream before clear zk path, get here may
    -- means some unexpected error. since it is a delete request and we just want to destroy the resouce, so
    -- it could be fine to just delete the stream instead of throw an exception
    (False, True) -> doDelete>> returnResp Empty
    -- get here may because we meet a concurrency problem, or we finished delete request but client lose the
    -- response and retry
    (False, False) -> if deleteStreamRequestIgnoreNonExist then returnResp Empty else throwIO StreamNotExist
  where
    streamID = transToStreamName deleteStreamRequestStreamName
    name = textToCBytes deleteStreamRequestStreamName
    streamName = transToStreamName deleteStreamRequestStreamName
    cleanZkNode = removeStreamRelatedPath zkHandle name
    checkZkPathExist = isJust <$> zooExists zkHandle (streamRootPath <> "/" <> name)
    checkStreamExist = S.doesStreamExist scLDClient streamName
    checkIfActive = checkIfSubsOfStreamActive zkHandle name
    doDelete = deleteStoreStream sc streamID deleteStreamRequestIgnoreNonExist

    delete False = do
      isActive <- checkIfActive
      if isActive
        then returnErrResp StatusFailedPrecondition "Can not delete stream with active subscription."
        else doDelete >> cleanZkNode >> returnResp Empty
    delete True = do
      isActive <- checkIfActive
      if isActive then S.archiveStream scLDClient streamName
                  else void doDelete
      cleanZkNode >> returnResp Empty


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
