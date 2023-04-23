
{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.ShardReader
  ( createShardReaderHandler
  , deleteShardReaderHandler
  , readShardHandler
  , listShardReadersHandler
  , handleListShardReaders
  , handleCreateShardReader
  , handleDeleteShardReader
  , handleReadShard
  , handleReadShardStream
  )
where

import           Control.Exception
import qualified Data.Text                        as T
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import           Control.Concurrent.STM           (readTVarIO)
import           Control.Monad                    (unless)
import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Common       (getResNode, lookupResource')
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
  Log.debug $ "Receive Create ShardReader Request" <> Log.buildString' (show request)
  validateCreateShardReader request
  C.createShardReader sc request >>= returnResp

handleCreateShardReader
  :: ServerContext
  -> G.UnaryHandler CreateShardReaderRequest CreateShardReaderResponse
handleCreateShardReader sc _ req = catchDefaultEx $ do
  Log.debug $ "Receive Create ShardReader Request" <> Log.buildString' (show req)
  validateCreateShardReader req
  C.createShardReader sc req

deleteShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteShardReaderRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteShardReaderHandler sc@ServerContext{..} (ServerNormalRequest _metadata request@DeleteShardReaderRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete ShardReader Request" <> Log.buildString' (show request)
  validateNameAndThrow ResShardReader deleteShardReaderRequestReaderId
  ServerNode{..} <- lookupResource' sc ResShardReader deleteShardReaderRequestReaderId
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "ShardReader is bound to a different node"
  C.deleteShardReader sc request >> returnResp Empty

handleDeleteShardReader
  :: ServerContext
  -> G.UnaryHandler DeleteShardReaderRequest Empty
handleDeleteShardReader sc@ServerContext{..} _ req@DeleteShardReaderRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Delete ShardReader Request" <> Log.buildString' (show req)
  validateNameAndThrow ResShardReader deleteShardReaderRequestReaderId
  ServerNode{..} <- lookupResource' sc ResShardReader deleteShardReaderRequestReaderId
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

handleReadShardStream
  :: ServerContext
  -> G.ServerStreamHandler ReadShardStreamRequest ReadShardStreamResponse ()
handleReadShardStream sc@ServerContext{..} _ req stream = catchDefaultEx $ do
  Log.debug $ "Receive read shard stream Request: " <> Log.build (show req)
  ServerNode{..} <- lookupResource' sc ResShardReader $ readShardStreamRequestReaderId req
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "The read shard stream is allocated to a different node"
  C.readShardStream sc req stream
