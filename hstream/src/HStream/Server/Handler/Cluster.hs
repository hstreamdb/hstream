{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Cluster
  ( -- * For grpc-haskell
    describeClusterHandler
  , lookupShardHandler
  , lookupSubscriptionHandler
  , lookupConnectorHandler
  , lookupShardReaderHandler
  , lookupResourceHandler
    -- * For hs-grpc-server
  , handleDescribeCluster
  , handleLookupShard
  , handleLookupSubscription
  , handleLookupShardReader
  , handleLookupConnector
  ) where

import           Control.Exception                (throwIO)
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.Server.Core.Cluster      as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnResp)
import           Proto3.Suite                     (Enumerated (..))

-------------------------------------------------------------------------------

describeClusterHandler
  :: ServerContext
  -> ServerRequest 'Normal Empty DescribeClusterResponse
  -> IO (ServerResponse 'Normal DescribeClusterResponse)
describeClusterHandler sc (ServerNormalRequest _meta _) =
  defaultExceptionHandle $ returnResp =<< C.describeCluster sc

lookupShardHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupShardRequest LookupShardResponse
  -> IO (ServerResponse 'Normal LookupShardResponse)
lookupShardHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShard sc req

lookupSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupSubscriptionRequest LookupSubscriptionResponse
  -> IO (ServerResponse 'Normal LookupSubscriptionResponse)
lookupSubscriptionHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupSubscription sc req

lookupConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupConnectorRequest LookupConnectorResponse
  -> IO (ServerResponse 'Normal LookupConnectorResponse)
lookupConnectorHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupConnector sc req

lookupShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupShardReaderRequest LookupShardReaderResponse
  -> IO (ServerResponse 'Normal LookupShardReaderResponse)
lookupShardReaderHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShardReader sc req

lookupResourceHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupResourceRequest ServerNode
  -> IO (ServerResponse 'Normal ServerNode)
lookupResourceHandler sc (ServerNormalRequest _meta LookupResourceRequest{..}) = defaultExceptionHandle $ do
  case lookupResourceRequestResType of
    Enumerated (Right rType) -> returnResp =<< C.lookupResource sc rType lookupResourceRequestResId
    _ -> throwIO $ HE.InvalidResourceType "Invalid resource type"

-------------------------------------------------------------------------------

handleDescribeCluster :: ServerContext -> G.UnaryHandler Empty DescribeClusterResponse
handleDescribeCluster sc _ _ = catchDefaultEx $ C.describeCluster sc

handleLookupShard :: ServerContext -> G.UnaryHandler LookupShardRequest LookupShardResponse
handleLookupShard sc _ req = catchDefaultEx $ C.lookupShard sc req

handleLookupSubscription
  :: ServerContext
  -> G.UnaryHandler LookupSubscriptionRequest LookupSubscriptionResponse
handleLookupSubscription sc _ req = catchDefaultEx $ C.lookupSubscription sc req

handleLookupShardReader
  :: ServerContext
  -> G.UnaryHandler LookupShardReaderRequest LookupShardReaderResponse
handleLookupShardReader sc _ req = catchDefaultEx $
  C.lookupShardReader sc req

handleLookupConnector
  :: ServerContext -> G.UnaryHandler LookupConnectorRequest LookupConnectorResponse
handleLookupConnector sc _ req = catchDefaultEx $ C.lookupConnector sc req
