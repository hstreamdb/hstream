{-# LANGUAGE OverloadedLists #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  ) where

import qualified Data.ByteString                  as BS
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as Text
import qualified Data.Vector                      as V
import           GHC.Stack                        (HasCallStack)

import           Control.Exception                (Handler (..), catches,
                                                   throwIO)
import           Data.Maybe                       (fromMaybe)
import           HStream.Connector.HStore         (transToStreamName)
import           HStream.Server.Core.Common       (deleteStoreStream)
import           HStream.Server.Exception         (StreamNotExist (..))
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Persistence.Utils (mkPartitionKeysPath,
                                                   tryCreate)
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils
import           ZooKeeper                        (zooExists)

-------------------------------------------------------------------------------

-- createStream will create a stream with a default partition
createStream :: HasCallStack => ServerContext -> API.Stream -> IO ()
createStream ServerContext{..} API.Stream{..} =
  S.createStream scLDClient (transToStreamName streamStreamName) $
    S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)

deleteStream :: ServerContext
             -> API.DeleteStreamRequest
             -> IO PB.Empty
deleteStream sc API.DeleteStreamRequest{..} = do
  let streamID = transToStreamName deleteStreamRequestStreamName
  deleteStoreStream sc streamID deleteStreamRequestIgnoreNonExist

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

appendStream :: ServerContext -> API.AppendRequest -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest{..} = do
  timestamp <- getProtoTimestamp
  let partitionKey = getRecordKey . V.head $ appendRequestRecords
      payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . API.hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)

  let prefixPath = mkPartitionKeysPath $ textToCBytes appendRequestStreamName
  let path = prefixPath <> "/" <> (textToCBytes . fromMaybe "__default__" $ partitionKey)
  let streamID = S.mkStreamId S.StreamTypeStream streamName
  logId <- zooExists zkHandle path >>= \case
    Just _ -> S.getUnderlyingLogId scLDClient streamID (textToCBytes <$> partitionKey)
    Nothing -> do
      logId <- catches (S.createStreamPartition scLDClient streamID (textToCBytes <$> partitionKey))
        [ Handler (\(_ :: S.StoreError) -> throwIO StreamNotExist), -- Stream not exists
          Handler (\(_ :: S.EXISTS) -> S.getUnderlyingLogId scLDClient streamID Nothing) -- both stream and partition are already exist
        ]
      tryCreate zkHandle path
      return logId
  S.AppendCompletion {..} <- S.appendBatchBS scLDClient logId (V.toList payloads) cmpStrategy Nothing
  let records = V.zipWith (\_ idx -> API.RecordId appendCompLSN idx) appendRequestRecords [0..]
  return $ API.AppendResponse appendRequestStreamName records
