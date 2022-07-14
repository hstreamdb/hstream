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
  , append0Handler
  , readShardHandler
  )
where

import           Control.Exception
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           Control.Concurrent.STM           (readTVarIO)
import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as Store
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

--------------------------------------------------------------------------------

createStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamHandler sc (ServerNormalRequest _metadata stream) = createStreamExceptionHandle $ do
  Log.debug $ "Receive Create Stream Request: " <> Log.buildString' stream
  C.createStream sc stream
  returnResp stream

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
deleteStreamHandler sc (ServerNormalRequest _metadata request) = deleteStreamExceptionHandle $ do
  Log.debug $ "Receive Delete Stream Request: " <> Log.buildString' request
  C.deleteStream sc request
  returnResp Empty

listStreamsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListStreamsRequest ListStreamsResponse
  -> IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $ do
  Log.debug "Receive List Stream Request"
  C.listStreams sc request >>= returnResp . ListStreamsResponse

appendHandler
  :: ServerContext
  -> ServerRequest 'Normal AppendRequest AppendResponse
  -> IO (ServerResponse 'Normal AppendResponse)
appendHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@AppendRequest{..}) =
  appendStreamExceptionHandle inc_failed $ do
    returnResp =<< C.append sc request
  where
    inc_failed = do
      Stats.stream_stat_add_append_failed scStatsHolder cStreamName 1
      Stats.stream_time_series_add_append_failed_requests scStatsHolder cStreamName 1
    cStreamName = textToCBytes appendRequestStreamName

append0Handler
  :: ServerContext
  -> ServerRequest 'Normal AppendRequest AppendResponse
  -> IO (ServerResponse 'Normal AppendResponse)
append0Handler sc@ServerContext{..} (ServerNormalRequest _metadata request@AppendRequest{..}) =
  appendStreamExceptionHandle inc_failed $ do

  Log.debug $ "Receive Append0 Request: StreamName {"
           <> Log.buildText appendRequestStreamName
           <> "}, nums of records = "
           <> Log.buildInt (V.length appendRequestRecords)
  Stats.stream_stat_add_append_total scStatsHolder cStreamName 1
  Stats.stream_time_series_add_append_in_requests scStatsHolder cStreamName 1
  let partitionKey = getRecordKey . V.head $ appendRequestRecords
  hashRing <- readTVarIO loadBalanceHashRing
  let identifier = appendRequestStreamName <> clientDefaultKey
  if getAllocatedNodeId hashRing identifier == serverID
    then C.append0Stream sc request partitionKey >>= returnResp
    else returnErrResp StatusFailedPrecondition "Send appendRequest to wrong Server."
  where
    inc_failed = Stats.stream_stat_add_append_failed scStatsHolder cStreamName 1
    cStreamName = textToCBytes appendRequestStreamName


listShardsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListShardsRequest ListShardsResponse
  -> IO (ServerResponse 'Normal ListShardsResponse)
listShardsHandler sc (ServerNormalRequest _metadata request) = do
  Log.debug "Receive List Shards Request"
  C.listShards sc request >>= returnResp . ListShardsResponse

readShardHandler
  :: ServerContext
  -> ServerRequest 'Normal ReadShardRequest ReadShardResponse
  -> IO (ServerResponse 'Normal ReadShardResponse)
readShardHandler sc (ServerNormalRequest _metadata request) = readShardExceptionHandle $ do
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
      ] ++ defaultHandlers
    mkHandlers = setRespType mkServerErrResp handlers

createStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
createStreamExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  streamExistsHandlers ++ defaultHandlers

deleteStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
deleteStreamExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  deleteExceptionHandler ++ defaultHandlers
  where
    deleteExceptionHandler = [
      Handler (\(err :: C.FoundSubscription) -> do
       Log.warning $ Log.buildString' err
       return (StatusFailedPrecondition, "Stream still has subscription"))
      ]

readShardExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
readShardExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  [ Handler (\(err :: Store.NOTFOUND) ->
      return (StatusUnavailable, mkStatusDetails err))
  ] ++ defaultHandlers
