{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Stream
  (
    createStreamHandler,
    deleteStreamHandler,
    listStreamsHandler,
    appendHandler,
    append0Handler)
where

import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           Control.Concurrent               (readMVar)
import           Control.Exception
import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as C
import           HStream.Server.Exception
import           HStream.Server.Handler.Common    (clientDefaultKey)
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
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
appendHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@AppendRequest{..}) = appendStreamExceptionHandle $ do
  Log.debug $ "Receive Append Request: StreamName {" <> Log.buildText appendRequestStreamName <> "}, nums of records = " <> Log.buildInt (V.length appendRequestRecords)
  hashRing <- readMVar loadBalanceHashRing
  let partitionKey = getRecordKey . V.head $ appendRequestRecords
  let identifier = case partitionKey of
        Just key -> appendRequestStreamName <> key
        Nothing  -> appendRequestStreamName <> clientDefaultKey
  if getAllocatedNodeId hashRing identifier == serverID
    then C.appendStream sc request partitionKey >>= returnResp
    else returnErrResp StatusFailedPrecondition "Send appendRequest to wrong Server."

append0Handler
  :: ServerContext
  -> ServerRequest 'Normal AppendRequest AppendResponse
  -> IO (ServerResponse 'Normal AppendResponse)
append0Handler sc@ServerContext{..} (ServerNormalRequest _metadata request@AppendRequest{..}) = appendStreamExceptionHandle $ do
  Log.debug $ "Receive Append0 Request: StreamName {" <> Log.buildText appendRequestStreamName <> "}, nums of records = " <> Log.buildInt (V.length appendRequestRecords)
  let partitionKey = getRecordKey . V.head $ appendRequestRecords
  hashRing <- readMVar loadBalanceHashRing
  let identifier = appendRequestStreamName <> clientDefaultKey
  if getAllocatedNodeId hashRing identifier == serverID
    then C.append0Stream sc request partitionKey >>= returnResp
    else returnErrResp StatusFailedPrecondition "Send appendRequest to wrong Server."

--------------------------------------------------------------------------------
-- Exception Handlers

streamExistsHandlers :: Handlers (StatusCode, StatusDetails)
streamExistsHandlers = [
  Handler (\(err :: C.StreamExists) -> do
    Log.warning $ Log.buildString' err
    return (StatusAlreadyExists, mkStatusDetails err))
  ]

appendStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
appendStreamExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  handlers ++ defaultHandlers
  where
    handlers = [
      Handler (\(err :: Store.NOTFOUND) ->
        return (StatusUnavailable, mkStatusDetails err)),
      Handler (\(err :: Store.NOTINSERVERCONFIG) ->
        return (StatusUnavailable, mkStatusDetails err))
      ]

createStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
createStreamExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  streamExistsHandlers ++ defaultHandlers

deleteStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
deleteStreamExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  deleteExceptionHandler ++ defaultHandlers
  where
    deleteExceptionHandler = [
      Handler (\(err :: C.FoundActiveSubscription) -> do
       Log.warning $ Log.buildString' err
       return (StatusFailedPrecondition, "Stream still has active consumers"))
      ]
