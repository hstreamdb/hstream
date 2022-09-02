{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Stream
  ( createStreamHandler
  , deleteStreamHandler
  , listStreamsHandler
  , listShardsHandler
  , appendHandler
  , createShardReaderHandler
  , deleteShardReaderHandler
  , readShardHandler
  )
where

import           Control.Exception
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Logger                   as Log
import           HStream.Metrics.ShardMetrics     (shardFailedHandleCountV,
                                                   shardTotalHandleCountV)
import           HStream.Metrics.StreamMetrics    (streamFailedHandleCountV,
                                                   streamTotalHandleCountV,
                                                   streamTotalNumGauge)
import qualified HStream.Server.Core.Stream       as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as Store
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils
import qualified Prometheus                       as P

--------------------------------------------------------------------------------

createStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamHandler sc (ServerNormalRequest _metadata stream) = createStreamExceptionHandle inc_failed $ do
  Log.debug $ "Receive Create Stream Request: " <> Log.buildString' stream
  P.withLabel streamTotalHandleCountV "createStream" P.incCounter
  C.createStream sc stream
  P.incGauge streamTotalNumGauge
  returnResp stream
 where
   inc_failed = P.withLabel streamFailedHandleCountV "createStream" P.incCounter

-- DeleteStream have two mod: force delete or normal delete
-- For normal delete, if current stream have active subscription, the delete request will return error.
-- For force delete, if current stream have active subscription, the stream will be archived. After that,
-- old stream is no longer visible, current consumers can continue consume from the old subscription,
-- but new consumers are no longer allowed to join that subscription.
--
-- Note: For foce delete, delivery is only guaranteed as far as possible. Which means: a consumer which
-- terminates its consumption for any reason will have no chance to restart the consumption process.
deleteStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteStreamRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc (ServerNormalRequest _metadata request) = deleteStreamExceptionHandle inc_failed $ do
  Log.debug $ "Receive Delete Stream Request: " <> Log.buildString' request
  P.withLabel streamTotalHandleCountV "deleteStream" P.incCounter
  C.deleteStream sc request
  P.decGauge streamTotalNumGauge
  returnResp Empty
 where
   inc_failed = P.withLabel streamFailedHandleCountV "deleteStream" P.incCounter

listStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListStreamsRequest ListStreamsResponse
  -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler sc (ServerNormalRequest _metadata request) = listStreamsExceptionHandle inc_failed $ do
  Log.debug "Receive List Stream Request"
  P.withLabel streamTotalHandleCountV "listStream" P.incCounter
  C.listStreams sc request >>= returnResp . ListStreamsResponse
 where
   inc_failed = P.withLabel streamFailedHandleCountV "listStream" P.incCounter

appendHandler
  :: ServerContext
  -> ServerRequest 'Normal AppendRequest AppendResponse
  -> IO (ServerResponse 'Normal AppendResponse)
appendHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@AppendRequest{..}) =
  appendStreamExceptionHandle inc_failed $ do
    P.withLabel streamTotalHandleCountV "append" P.incCounter
    returnResp =<< C.append sc request
  where
    inc_failed = do
      Stats.stream_stat_add_append_failed scStatsHolder cStreamName 1
      Stats.stream_time_series_add_append_failed_requests scStatsHolder cStreamName 1
      P.withLabel streamFailedHandleCountV "append" P.incCounter
    cStreamName = textToCBytes appendRequestStreamName

--------------------------------------------------------------------------------

listShardsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListShardsRequest ListShardsResponse
  -> IO (ServerResponse 'Normal ListShardsResponse)
listShardsHandler sc (ServerNormalRequest _metadata request) = listShardsExceptionHandle inc_failed $ do
  Log.debug "Receive List Shards Request"
  P.withLabel shardTotalHandleCountV "listShards" P.incCounter
  C.listShards sc request >>= returnResp . ListShardsResponse
 where
   inc_failed = P.withLabel shardFailedHandleCountV "listShards" P.incCounter

createShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateShardReaderRequest CreateShardReaderResponse
  -> IO (ServerResponse 'Normal CreateShardReaderResponse)
createShardReaderHandler sc (ServerNormalRequest _metadata request) = shardReaderExceptionHandle $ do
  Log.debug $ "Receive Create ShardReader Request" <> Log.buildString' (show request)
  C.createShardReader sc request >>= returnResp

deleteShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteShardReaderRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteShardReaderHandler sc (ServerNormalRequest _metadata request) = shardReaderExceptionHandle $ do
  Log.debug $ "Receive Delete ShardReader Request" <> Log.buildString' (show request)
  C.deleteShardReader sc request >> returnResp Empty

readShardHandler
  :: ServerContext
  -> ServerRequest 'Normal ReadShardRequest ReadShardResponse
  -> IO (ServerResponse 'Normal ReadShardResponse)
readShardHandler sc (ServerNormalRequest _metadata request) = shardReaderExceptionHandle $ do
  Log.debug $ "Receive read shard Request: " <> Log.buildString (show request)
  C.readShard sc request >>= returnResp . ReadShardResponse

--------------------------------------------------------------------------------
-- Exception Handlers

streamExistsHandlers :: Handlers (StatusCode, StatusDetails)
streamExistsHandlers = [
  Handler (\(err :: C.StreamExists) -> do
    Log.warning $ Log.buildString' err
    return (StatusAlreadyExists, mkStatusDetails err))
  ]

appendStreamExceptionHandle :: IO () -> ExceptionHandle (ServerResponse 'Normal a)
appendStreamExceptionHandle f = mkExceptionHandle' whileEx mkHandlers
  where
    whileEx :: forall e. Exception e => e -> IO ()
    whileEx err = Log.warning (Log.buildString' err) >> f
    handlers =
      [ Handler (\(_ :: C.RecordTooBig) ->
          return (StatusFailedPrecondition, "Record size exceeds the maximum size limit" ))
      , Handler (\(err :: WrongServer) ->
          return (StatusFailedPrecondition, mkStatusDetails err))
      , Handler (\(err :: Store.NOTFOUND) ->
          return (StatusUnavailable, mkStatusDetails err))
      , Handler (\(err :: Store.NOTINSERVERCONFIG) ->
          return (StatusUnavailable, mkStatusDetails err))
      , Handler (\(err :: Store.NOSEQUENCER) -> do
          return (StatusUnavailable, mkStatusDetails err))
      , Handler (\(err :: NoRecordHeader) ->
          return (StatusInvalidArgument, mkStatusDetails err))
      , Handler (\(err :: DecodeHStreamRecordErr) -> do
          return (StatusInvalidArgument, mkStatusDetails err))
      , Handler (\(err :: C.InvalidBatchedRecord) -> do
          return (StatusInvalidArgument, mkStatusDetails err))
      ] ++ defaultHandlers
    mkHandlers = setRespType mkServerErrResp handlers

createStreamExceptionHandle :: IO () -> ExceptionHandle (ServerResponse 'Normal a)
createStreamExceptionHandle action = mkExceptionHandleWithAction action . setRespType mkServerErrResp $
  streamExistsHandlers ++ defaultHandlers

deleteStreamExceptionHandle :: IO () -> ExceptionHandle (ServerResponse 'Normal a)
deleteStreamExceptionHandle action = mkExceptionHandleWithAction action . setRespType mkServerErrResp $
  deleteExceptionHandler ++ defaultHandlers
  where
    deleteExceptionHandler = [
      Handler (\(err :: C.FoundSubscription) -> do
       Log.warning $ Log.buildString' err
       return (StatusFailedPrecondition, "Stream still has subscription"))
      ]

listStreamsExceptionHandle :: IO () -> ExceptionHandle (ServerResponse 'Normal a)
listStreamsExceptionHandle action =
  mkExceptionHandleWithAction action . setRespType mkServerErrResp $ defaultHandlers

listShardsExceptionHandle :: IO () -> ExceptionHandle (ServerResponse 'Normal a)
listShardsExceptionHandle action = mkExceptionHandleWithAction action . setRespType mkServerErrResp $
   Handler (\(err :: Store.NOTFOUND) ->
          return (StatusUnavailable, mkStatusDetails err))
  : defaultHandlers

shardReaderExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
shardReaderExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  [ Handler (\(err :: C.ShardReaderExists) ->
      return (StatusAlreadyExists, mkStatusDetails err)),
    Handler (\(err :: C.ShardReaderNotExists) ->
      return (StatusFailedPrecondition, mkStatusDetails err)),
    Handler (\(err :: WrongServer) ->
      return (StatusFailedPrecondition, mkStatusDetails err)),
    Handler (\(err :: C.UnKnownShardOffset) ->
      return (StatusInvalidArgument, mkStatusDetails err))
  ] ++ defaultHandlers
