{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.ShardReader
  ( -- * For grpc-haskell
    createShardReaderHandler
  , deleteShardReaderHandler
  , readShardHandler
  , listShardReadersHandler
  , readShardStreamHandler
  , readStreamHandler
  , readSingleShardStreamHandler
  , readStreamByKeyHandler
    -- * For hs-grpc-server
  , handleListShardReaders
  , handleCreateShardReader
  , handleDeleteShardReader
  , handleReadShard
  , handleReadShardStream
  , handleReadStream
  , handleReadSingleShardStream
  , handleReadStreamByKey
  )
where

import           Control.Exception
import           Data.Bifunctor                   (first)
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import           Control.Monad                    (unless)
import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Common       (lookupResource)
import qualified HStream.Server.Core.ShardReader  as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import           HStream.Server.Validation        (validateCreateShardReader)
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils

listShardReadersHandler :: ServerContext
  -> ServerRequest 'Normal ListShardReadersRequest ListShardReadersResponse
  -> IO (ServerResponse 'Normal ListShardReadersResponse)
listShardReadersHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $
  C.listShardReaders sc request >>= returnResp . ListShardReadersResponse

handleListShardReaders :: ServerContext -> G.UnaryHandler ListShardReadersRequest ListShardReadersResponse
handleListShardReaders sc _ req = catchDefaultEx $
  ListShardReadersResponse <$> C.listShardReaders sc req

createShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateShardReaderRequest CreateShardReaderResponse
  -> IO (ServerResponse 'Normal CreateShardReaderResponse)
createShardReaderHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $ do
  Log.info $ "Receive Create ShardReader Request" <> Log.buildString' (show request)
  validateCreateShardReader request
  C.createShardReader sc request >>= returnResp

handleCreateShardReader
  :: ServerContext
  -> G.UnaryHandler CreateShardReaderRequest CreateShardReaderResponse
handleCreateShardReader sc _ req = catchDefaultEx $ do
  Log.info $ "Receive Create ShardReader Request" <> Log.buildString' (show req)
  validateCreateShardReader req
  C.createShardReader sc req

deleteShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteShardReaderRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteShardReaderHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@DeleteShardReaderRequest{..}) = defaultExceptionHandle $ do
  Log.info $ "Receive Delete ShardReader Request" <> Log.buildString' (show request)
  validateNameAndThrow ResShardReader deleteShardReaderRequestReaderId
  ServerNode{..} <- lookupResource sc ResShardReader deleteShardReaderRequestReaderId
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "ShardReader is bound to a different node"
  C.deleteShardReader sc request >> returnResp Empty

handleDeleteShardReader
  :: ServerContext
  -> G.UnaryHandler DeleteShardReaderRequest Empty
handleDeleteShardReader sc@ServerContext{..} _ req@DeleteShardReaderRequest{..} = catchDefaultEx $ do
  Log.info $ "Receive Delete ShardReader Request" <> Log.buildString' (show req)
  validateNameAndThrow ResShardReader deleteShardReaderRequestReaderId
  ServerNode{..} <- lookupResource sc ResShardReader deleteShardReaderRequestReaderId
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "ShardReader is bound to a different node"
  C.deleteShardReader sc req >> pure Empty

readShardHandler
  :: ServerContext
  -> ServerRequest 'Normal ReadShardRequest ReadShardResponse
  -> IO (ServerResponse 'Normal ReadShardResponse)
readShardHandler sc (ServerNormalRequest _metadata request) = defaultExceptionHandle $ do
  Log.debug $ "Receive read shard Request: " <> Log.buildString (show request)
  C.readShard sc request >>= returnResp . ReadShardResponse

handleReadShard
  :: ServerContext
  -> G.UnaryHandler ReadShardRequest ReadShardResponse
handleReadShard sc _ req = catchDefaultEx $ do
  Log.debug $ "Receive read shard Request: " <> Log.buildString (show req)
  ReadShardResponse <$> C.readShard sc req

readShardStreamHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming ReadShardStreamRequest ReadShardStreamResponse
  -> IO (ServerResponse 'ServerStreaming ReadShardStreamResponse)
readShardStreamHandler sc (ServerWriterRequest _meta req streamSend) =
  defaultServerStreamExceptionHandle $ do
    Log.debug $ "Receive read shard stream Request: " <> Log.build (show req)
    C.readShardStream sc req streamWrite
    returnServerStreamingResp StatusOk ""
  where
    streamWrite x = first show <$> streamSend x

-- TODO: update the protocol to support delivery un-recoverable gap info to the client
handleReadShardStream
  :: ServerContext
  -> G.ServerStreamHandler ReadShardStreamRequest ReadShardStreamResponse ()
handleReadShardStream sc _ req stream = catchDefaultEx $ do
  Log.debug $ "Receive read shard stream Request: " <> Log.build (show req)
  C.readShardStream sc req (G.streamWrite stream . Just)

readStreamHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming ReadStreamRequest ReadStreamResponse
  -> IO (ServerResponse 'ServerStreaming ReadStreamResponse)
readStreamHandler sc (ServerWriterRequest _meta req streamSend) =
  defaultServerStreamExceptionHandle $ do
    Log.debug $ "Receive read shard stream Request: " <> Log.build (show req)
    C.readStream sc req streamWrite
    returnServerStreamingResp StatusOk ""
  where
    streamWrite x = first show <$> streamSend x

-- TODO: update the protocol to support delivery un-recoverable gap info to the client
handleReadStream
  :: ServerContext
  -> G.ServerStreamHandler ReadStreamRequest ReadStreamResponse ()
handleReadStream sc _ req stream = catchDefaultEx $ do
  Log.debug $ "Receive read shard stream Request: " <> Log.build (show req)
  C.readStream sc req (G.streamWrite stream . Just)

readSingleShardStreamHandler
  :: ServerContext
  -> ServerRequest 'ServerStreaming ReadSingleShardStreamRequest ReadSingleShardStreamResponse
  -> IO (ServerResponse 'ServerStreaming ReadSingleShardStreamResponse)
readSingleShardStreamHandler sc (ServerWriterRequest _meta req streamSend) =
  defaultServerStreamExceptionHandle $ do
    Log.debug $ "Receive read single shard stream Request: " <> Log.build (show req)
    C.readSingleShardStream sc req streamWrite
    returnServerStreamingResp StatusOk ""
  where
    streamWrite x = first show <$> streamSend x

-- TODO: update the protocol to support delivery un-recoverable gap info to the client
handleReadSingleShardStream
  :: ServerContext
  -> G.ServerStreamHandler ReadSingleShardStreamRequest ReadSingleShardStreamResponse ()
handleReadSingleShardStream sc _ req stream = catchDefaultEx $ do
  Log.debug $ "Receive read single shard stream Request: " <> Log.build (show req)
  C.readSingleShardStream sc req (G.streamWrite stream . Just)

readStreamByKeyHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming ReadStreamByKeyRequest ReadStreamByKeyResponse
  -> IO (ServerResponse 'BiDiStreaming ReadStreamByKeyResponse)
readStreamByKeyHandler ctx (ServerBiDiRequest _meta streamRecv streamSend) =
  defaultBiDiStreamExceptionHandle $ do
    Log.info "recv readStreamByKey request"
    C.readStreamByKey ctx streamSend streamRecv
    return $ ServerBiDiResponse mempty StatusOk ""

-- TODO: update the protocol to support delivery un-recoverable gap info to the client
handleReadStreamByKey
  :: ServerContext
  -> G.BidiStreamHandler ReadStreamByKeyRequest ReadStreamByKeyResponse ()
handleReadStreamByKey sc _gCtx stream = do
  Log.info "recv readStreamByKey request"
  let streamSend x = first (const GRPCIOShutdown) <$> G.streamWrite stream (Just x)
      streamRecv = do Right <$> G.streamRead stream
  catchDefaultEx $ C.readStreamByKey sc streamSend streamRecv
