module HStream.Common.GrpcHaskell
  ( initGrpcClient
  , deleteGrpcClient
    -- * Re-exports
  , GRPC.Client
  ) where

import qualified Network.GRPC.LowLevel.Client as GRPC
import qualified Network.GRPC.LowLevel.GRPC   as GRPC
import qualified Network.GRPC.Unsafe          as GRPC

import qualified HStream.Logger               as Log

initGrpcClient :: GRPC.ClientConfig -> IO GRPC.Client
initGrpcClient config = do
  grpc <- GRPC.grpcInit >> return GRPC.GRPC
  GRPC.createClient grpc config

deleteGrpcClient :: GRPC.Client -> IO ()
deleteGrpcClient client = do
  Log.debug "Run destroyClient"
  GRPC.destroyClient client
  Log.debug "Run grpcShutdown"
  GRPC.grpcShutdown
