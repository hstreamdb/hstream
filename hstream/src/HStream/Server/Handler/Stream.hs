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
    appendHandler
  )
where

import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           Control.Concurrent               (readMVar)
import           Control.Monad                    (void)
import           Data.Maybe                       (isJust)
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as C
import           HStream.Server.Exception         (StreamNotExist (..),
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (checkIfSubsOfStreamActive,
                                                   createStreamRelatedPath,
                                                   removeStreamRelatedPath,
                                                   shouldBeServedByThisServer)
import           HStream.Server.Persistence.Utils
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils
import           Z.IO.Exception                   (throwIO)
import           ZooKeeper                        (zooExists)

--------------------------------------------------------------------------------

createStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamHandler sc@ServerContext{..} (ServerNormalRequest _metadata stream@Stream{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Stream Request: " <> Log.buildString' stream
  let name = textToCBytes streamStreamName
  keys <- tryGetChildren zkHandle $ mkPartitionKeysPath name
  if null keys
    then do
      createStreamRelatedPath zkHandle name
      C.createStream sc stream
      returnResp stream
    else
      -- get here may because there is a previouse stream with same name failed to perform a deleted operation and
      -- did not retry, or a client try to create a stream already existed.
      returnErrResp StatusFailedPrecondition "Create failed because zk key path exists."

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
deleteStreamHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@DeleteStreamRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete Stream Request: " <> Log.buildString' request
  zNodeExists <- checkZkPathExist
  storeExists <- checkStreamExist
  case (zNodeExists, storeExists) of
    -- normal path
    (True, True) -> doDelete deleteStreamRequestForce
    -- if we delete stream but failed to clear zk path, we will get here when client retry the delete request
    (True, False) -> cleanZkNode >> returnResp Empty
    -- actually, it should not be here because we always delete stream before clear zk path, get here may
    -- means some unexpected error. since it is a delete request and we just want to destroy the resouce, so
    -- it could be fine to just delete the stream instead of throw an exception
    (False, True) -> C.deleteStream sc request >> returnResp Empty
    -- get here may because we meet a concurrency problem, or we finished delete request but client lose the
    -- response and retry
    (False, False) -> if deleteStreamRequestIgnoreNonExist then returnResp Empty else throwIO StreamNotExist
  where
    name = textToCBytes deleteStreamRequestStreamName
    streamName = transToStreamName deleteStreamRequestStreamName
    cleanZkNode = removeStreamRelatedPath zkHandle name
    checkZkPathExist = isJust <$> zooExists zkHandle (streamRootPath <> "/" <> name)
    checkStreamExist = S.doesStreamExist scLDClient streamName
    checkIfActive = checkIfSubsOfStreamActive zkHandle name

    doDelete False = do
      isActive <- checkIfActive
      if isActive
        then returnErrResp StatusFailedPrecondition "Can not delete stream with active subscription."
        else C.deleteStream sc request >> cleanZkNode >> returnResp Empty
    doDelete True = do
      isActive <- checkIfActive
      if isActive then S.archiveStream scLDClient streamName
                  else void $ C.deleteStream sc request
      cleanZkNode >> returnResp Empty

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
appendHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@AppendRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Append Request: StreamName {" <> Log.buildText appendRequestStreamName <> "}, nums of records = " <> Log.buildInt (V.length appendRequestRecords)
  hashRing <- readMVar loadBalanceHashRing
  let partitionKey = getRecordKey . V.head $ appendRequestRecords
  let identifier = case partitionKey of
                     Just key -> appendRequestStreamName <> key
                     Nothing  -> appendRequestStreamName
  if shouldBeServedByThisServer hashRing serverID identifier
    then C.appendStream sc request partitionKey >>= returnResp
    else returnErrResp StatusInvalidArgument "Send appendRequest to wrong Server."
