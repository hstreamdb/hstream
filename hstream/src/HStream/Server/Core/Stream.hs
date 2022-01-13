{-# LANGUAGE OverloadedLists #-}

module HStream.Server.Core.Stream
  ( createStream
  , deleteStream
  , listStreams
  , appendStream
  ) where

import qualified Data.ByteString             as BS
import qualified Data.Map.Strict             as Map
import qualified Data.Text                   as Text
import qualified Data.Vector                 as V
import           GHC.Stack                   (HasCallStack)
import qualified Z.Data.CBytes               as CB

import           HStream.Connector.HStore    (transToStreamName)
import           HStream.Server.Core.Common  (deleteStoreStream)
import qualified HStream.Server.HStreamApi   as API
import           HStream.Server.Types        (ServerContext (..))
import qualified HStream.Stats               as Stats
import qualified HStream.Store               as S
import           HStream.ThirdParty.Protobuf as PB
import           HStream.Utils

-------------------------------------------------------------------------------

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
  streams <- S.findStreams scLDClient S.StreamTypeStream True
  V.forM (V.fromList streams) $ \stream -> do
    r <- S.getStreamReplicaFactor scLDClient stream
    return $ API.Stream (Text.pack . S.showStreamName $ stream) (fromIntegral r)

appendStream :: ServerContext -> API.AppendRequest -> IO API.AppendResponse
appendStream ServerContext{..} API.AppendRequest{..} = do
  timestamp <- getProtoTimestamp
  let payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . API.hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  S.AppendCompletion {..} <- batchAppend scLDClient streamName payloads cmpStrategy
  let records = V.zipWith (\_ idx -> API.RecordId appendCompLSN idx) appendRequestRecords [0..]
  return $ API.AppendResponse appendRequestStreamName records
  where
    batchAppend :: S.LDClient -> CB.CBytes -> V.Vector BS.ByteString -> S.Compression -> IO S.AppendCompletion
    batchAppend client streamName payloads strategy = do
      logId <- S.getUnderlyingLogId client (S.mkStreamId S.StreamTypeStream streamName) Nothing
      -- TODO: support vector of ByteString
      S.appendBatchBS client logId (V.toList payloads) strategy Nothing
