{-# LANGUAGE DataKinds #-}

module HStream.Server.HsGrpcHandler (handlers) where

import           Control.Exception
import           HsGrpc.Server
import           HsGrpc.Server.Types

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Cluster      as C
import qualified HStream.Server.Core.Stream       as C
import qualified HStream.Server.Core.Subscription as C
import           HStream.Server.Exception         (defaultExHandlers)
import qualified HStream.Server.HStreamApi        as A
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.ThirdParty.Protobuf      as A
import qualified Proto.HStream.Server.HStreamApi  as P

handlers :: ServerContext -> [ServiceHandler]
handlers sc =
  [ unary (GRPC :: GRPC P.HStreamApi "echo") handleEcho
  , unary (GRPC :: GRPC P.HStreamApi "describeCluster") (handleDescribeCluster sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupShard") (handleLookupShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "listShards") (handleListShard sc)
  , unary (GRPC :: GRPC P.HStreamApi "createStream") (handleCreateStream sc)
  , unary (GRPC :: GRPC P.HStreamApi "append") (handleAppend sc)
  , unary (GRPC :: GRPC P.HStreamApi "createSubscription") (handleCreateSubscription sc)
  , unary (GRPC :: GRPC P.HStreamApi "lookupSubscription") (handleLookupSubscription sc)
  , bidiStream (GRPC :: GRPC P.HStreamApi "streamingFetch") (handleStreamingFetch sc)
  ]

catchException :: IO a -> IO a
catchException action = action `catches` defaultExHandlers

handleEcho :: A.EchoRequest -> IO A.EchoResponse
handleEcho A.EchoRequest{..} = return $ A.EchoResponse echoRequestMsg

handleDescribeCluster :: ServerContext -> A.Empty -> IO A.DescribeClusterResponse
handleDescribeCluster sc _ = catchException $ C.describeCluster sc

handleLookupShard :: ServerContext -> A.LookupShardRequest -> IO A.LookupShardResponse
handleLookupShard sc req = catchException $ C.lookupShard sc req

handleListShard :: ServerContext -> A.ListShardsRequest -> IO A.ListShardsResponse
handleListShard sc req = catchException $
  A.ListShardsResponse <$> C.listShards sc req

handleCreateStream :: ServerContext -> A.Stream -> IO A.Stream
handleCreateStream sc stream = catchException $
  C.createStream sc stream >> pure stream

handleAppend :: ServerContext -> A.AppendRequest -> IO A.AppendResponse
handleAppend sc req = catchException $ C.append sc req
{-# INLINE handleAppend #-}

handleCreateSubscription :: ServerContext -> A.Subscription -> IO A.Subscription
handleCreateSubscription sc sub = catchException $
  C.createSubscription sc sub >> pure sub

handleLookupSubscription
  :: ServerContext
  -> A.LookupSubscriptionRequest -> IO A.LookupSubscriptionResponse
handleLookupSubscription sc req = catchException $ C.lookupSubscription sc req

handleStreamingFetch
  :: ServerContext
  -> BiDiStream A.StreamingFetchRequest A.StreamingFetchResponse
  -> IO ()
handleStreamingFetch sc stream =
  -- TODO
  let streamSend x = streamWrite stream (Just x) >> pure (Right ())
      streamRecv = do Right <$> streamRead stream
   in catchException $ C.streamingFetchCore sc C.SFetchCoreInteractive (streamSend, streamRecv)
