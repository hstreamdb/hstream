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
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as C
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (shouldBeServedByThisServer)
import           HStream.Server.Persistence.Utils
import           HStream.Server.Types             (ServerContext (..))
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

--------------------------------------------------------------------------------
-- TODO: use 'HStream.Server.Core.Stream'

createStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamHandler sc@ServerContext{..} (ServerNormalRequest _metadata stream@Stream{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Stream Request: " <> Log.buildString' stream
  let streamPath = streamRootPath <> "/" <> textToCBytes streamStreamName
  let keyPath = mkPartitionKeysPath (textToCBytes streamStreamName)
  keys <- tryGetChildren zkHandle keyPath
  if null keys
    then do
      let streamOp = createPathOp streamPath
      let keyOp = createPathOp keyPath
      tryCreateMulti zkHandle [streamOp, keyOp]
      C.createStream sc stream
      returnResp stream
    else 
      -- get here may because there is a previouse stream with same name failed to perform a deleted operation and
      -- did not retry, or a client try to create a stream already existed.
      returnErrResp StatusFailedPrecondition "Create failed because zk key path exists."

deleteStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal DeleteStreamRequest Empty ->
  IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@DeleteStreamRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Request: " <> Log.buildString' request
  tryDeleteAllPath zkHandle (streamRootPath <> "/" <> textToCBytes deleteStreamRequestStreamName)
  C.deleteStream sc request >>= returnResp

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
  if shouldBeServedByThisServer hashRing serverID appendRequestStreamName
    then C.appendStream sc request >>= returnResp
    else returnErrResp StatusInvalidArgument "Send appendRequest to wrong Server."
