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

import           Network.GRPC.HighLevel.Generated

import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as C
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

--------------------------------------------------------------------------------
-- TODO: use 'HStream.Server.Core.Stream'

createStreamHandler
  :: ServerContext
  -> ServerRequest 'Normal Stream Stream
  -> IO (ServerResponse 'Normal Stream)
createStreamHandler sc (ServerNormalRequest _metadata stream) = defaultExceptionHandle $ do
  Log.debug $ "Receive Create Stream Request: " <> Log.buildString' stream
  C.createStream sc stream
  returnResp stream

deleteStreamHandler ::
  ServerContext ->
  ServerRequest 'Normal DeleteStreamRequest Empty ->
  IO (ServerResponse 'Normal Empty)
deleteStreamHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $ do
  Log.debug $ "Receive Request: " <> Log.buildString' request
  C.deleteStream sc request >>= returnResp

listStreamsHandler ::
  ServerContext ->
  ServerRequest 'Normal ListStreamsRequest ListStreamsResponse ->
  IO (ServerResponse 'Normal ListStreamsResponse)
listStreamsHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $ do
  Log.debug "Receive List Stream Request"
  C.listStreams sc request >>= returnResp . ListStreamsResponse

appendHandler ::
  ServerContext ->
  ServerRequest 'Normal AppendRequest AppendResponse ->
  IO (ServerResponse 'Normal AppendResponse)
appendHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $ do
  Log.debug $ "Receive Stream Request: " <> Log.buildString' request
  C.appendStream sc request >>= returnResp
