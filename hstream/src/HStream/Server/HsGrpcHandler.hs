{-# LANGUAGE DataKinds #-}

module HStream.Server.HsGrpcHandler (handlers) where

import           Control.Exception               (Handler (Handler), catches)
import           HsGrpc.Server
import           HsGrpc.Server.Types

import qualified HStream.Logger                  as Log
import qualified HStream.Server.Core.Cluster     as C
import qualified HStream.Server.Core.Stream      as C
import qualified HStream.Server.HStreamApi       as A
import           HStream.Server.Types            (ServerContext (..))
import qualified HStream.Store                   as Store
import qualified HStream.ThirdParty.Protobuf     as A
import qualified Proto.HStream.Server.HStreamApi as P

handlers :: ServerContext -> [ServiceHandler]
handlers sc =
  [ unary (GRPC :: GRPC P.HStreamApi "echo") handleEcho
  , unary (GRPC :: GRPC P.HStreamApi "describeCluster") (handleDescribeCluster sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupShard") (handleLookupShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "listShards") (handleListShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "createStream") (handleCreateStream sc)
  , unary (GRPC :: GRPC P.HStreamApi "append") (handleAppend sc)
  ]

-- TODO: catch exception
catchException :: IO a -> IO a
catchException action = action `catches` [storeEx]
  where
    storeEx = Handler $ \(ex :: Store.SomeHStoreException) -> do
      Log.warning $ Log.buildString' ex
      throwGrpcError $ GrpcStatus StatusCancelled Nothing Nothing

handleEcho :: A.EchoRequest -> IO A.EchoResponse
handleEcho A.EchoRequest{..} = return $ A.EchoResponse echoRequestMsg
{-# INLINE handleEcho #-}

handleDescribeCluster :: ServerContext -> A.Empty -> IO A.DescribeClusterResponse
handleDescribeCluster sc _ = C.describeCluster sc
{-# INLINE handleDescribeCluster #-}

handleLookupShard :: ServerContext -> A.LookupShardRequest -> IO A.LookupShardResponse
handleLookupShard sc req = catchException $ C.lookupShard sc req
{-# INLINE handleLookupShard #-}

handleListShard :: ServerContext -> A.ListShardsRequest -> IO A.ListShardsResponse
handleListShard sc req = A.ListShardsResponse <$> C.listShards sc req

handleCreateStream :: ServerContext -> A.Stream -> IO A.Stream
handleCreateStream sc stream = catchException $
  C.createStream sc stream >> pure stream
{-# INLINE handleCreateStream #-}

handleAppend :: ServerContext -> A.AppendRequest -> IO A.AppendResponse
handleAppend sc req = catchException $ C.append sc req
{-# INLINE handleAppend #-}
