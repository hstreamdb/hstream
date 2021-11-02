{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Stream
  (
    createStreamHandler,
    deleteStreamHandler,
    listStreamsHandler,
    appendHandler
  )
where

import qualified Data.ByteString                  as BS
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB

import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (dropHelper)
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

--------------------------------------------------------------------------------

createStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal Stream Stream ->
  IO (ServerResponse 'Normal Stream)
createStreamHandler ServerContext {..} (ServerNormalRequest _metadata stream@Stream {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Stream Request: New Stream Name: " <> Log.buildText streamStreamName
  S.createStream scLDClient (transToStreamName streamStreamName) $
    S.LogAttrs (S.HsLogAttrs (fromIntegral streamReplicationFactor) Map.empty)
  returnResp stream

deleteStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal DeleteStreamRequest Empty ->
  IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc (ServerNormalRequest _metadata DeleteStreamRequest {..}) = defaultExceptionHandle $ do
  let streamName = deleteStreamRequestStreamName
  Log.debug $ "Receive Delete Stream Request: Stream to Delete: " <> Log.buildText streamName
  dropHelper sc streamName deleteStreamRequestIgnoreNonExist False

listStreamsHandler ::
  ServerContext ->
  ServerRequest 'Normal ListStreamsRequest ListStreamsResponse ->
  IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler ServerContext {..} (ServerNormalRequest _metadata ListStreamsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive List Stream Request"
  streams <- S.findStreams scLDClient S.StreamTypeStream True
  res <- V.forM (V.fromList streams) $ \stream -> do
    refactor <- S.getStreamReplicaFactor scLDClient stream
    return $ Stream (T.pack . S.showStreamName $ stream) (fromIntegral refactor)
  returnResp $ ListStreamsResponse res

appendHandler ::
  ServerContext ->
  ServerRequest 'Normal AppendRequest AppendResponse ->
  IO (ServerResponse 'Normal AppendResponse)
appendHandler ServerContext {..} (ServerNormalRequest _metadata AppendRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Append Stream Request. Append Data to the Stream: " <> Log.buildText appendRequestStreamName
  timestamp <- getProtoTimestamp
  let payloads = encodeRecord . updateRecordTimestamp timestamp <$> appendRequestRecords
      payloadSize = V.sum $ BS.length . hstreamRecordPayload <$> appendRequestRecords
      streamName = textToCBytes appendRequestStreamName
  -- XXX: Should we add a server option to toggle Stats?
  Stats.stream_time_series_add_append_in_bytes scStatsHolder streamName (fromIntegral payloadSize)
  S.AppendCompletion {..} <- batchAppend scLDClient streamName payloads cmpStrategy
  let records = V.zipWith (\_ idx -> RecordId appendCompLSN idx) appendRequestRecords [0 ..]
  returnResp $ AppendResponse appendRequestStreamName records
  where
    batchAppend :: S.LDClient -> CB.CBytes -> V.Vector BS.ByteString -> S.Compression -> IO S.AppendCompletion
    batchAppend client streamName payloads strategy = do
      logId <- S.getUnderlyingLogId client $ S.mkStreamId S.StreamTypeStream streamName
      -- TODO: support vector of ByteString
      S.appendBatchBS client logId (V.toList payloads) strategy Nothing
