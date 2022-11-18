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
  , handleLookupResource
  , handleLookupShard
  , handleLookupSubscription
  , handleLookupShardReader
  , handleLookupConnector
  , getOverviewHandler
  ) where

import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Server.Core.Cluster      as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnResp)

-------------------------------------------------------------------------------

describeClusterHandler
  :: ServerContext
  -> ServerRequest 'Normal Empty DescribeClusterResponse
  -> IO (ServerResponse 'Normal DescribeClusterResponse)
describeClusterHandler sc (ServerNormalRequest _meta _) =
  defaultExceptionHandle $ returnResp =<< C.describeCluster sc

lookupResourceHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupResourceRequest ServerNode
  -> IO (ServerResponse 'Normal ServerNode)
lookupResourceHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupResource sc req

lookupShardHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupShardRequest LookupShardResponse
  -> IO (ServerResponse 'Normal LookupShardResponse)
lookupShardHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShard sc req

{-# DEPRECATED lookupSubscriptionHandler "Use lookupResourceHandler instead" #-}
lookupSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupSubscriptionRequest LookupSubscriptionResponse
  -> IO (ServerResponse 'Normal LookupSubscriptionResponse)
lookupSubscriptionHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupSubscription sc req

{-# DEPRECATED lookupConnectorHandler "Use lookupResourceHandler instead" #-}
lookupConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupConnectorRequest LookupConnectorResponse
  -> IO (ServerResponse 'Normal LookupConnectorResponse)
lookupConnectorHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupConnector sc req

{-# DEPRECATED lookupShardReaderHandler "Use lookupResourceHandler instead" #-}
lookupShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupShardReaderRequest LookupShardReaderResponse
  -> IO (ServerResponse 'Normal LookupShardReaderResponse)
lookupShardReaderHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShardReader sc req

getOverviewHandler :: ServerContext -> ServerRequest 'Normal GetOverviewRequest GetOverviewResponse -> IO (ServerResponse 'Normal GetOverviewResponse)
getOverviewHandler sc (ServerNormalRequest _meta req) = defaultExceptionHandle $ returnResp =<< C.getOverview sc req

-------------------------------------------------------------------------------

handleDescribeCluster :: ServerContext -> G.UnaryHandler Empty DescribeClusterResponse
handleDescribeCluster sc _ _ = catchDefaultEx $ C.describeCluster sc

handleLookupResource :: ServerContext -> G.UnaryHandler LookupResourceRequest ServerNode
handleLookupResource sc _ req = catchDefaultEx $ C.lookupResource sc req

handleLookupShard :: ServerContext -> G.UnaryHandler LookupShardRequest LookupShardResponse
handleLookupShard sc _ req = catchDefaultEx $ C.lookupShard sc req

{-# DEPRECATED handleLookupSubscription "Use handleLookupResource instead" #-}
handleLookupSubscription
  :: ServerContext
  -> G.UnaryHandler LookupSubscriptionRequest LookupSubscriptionResponse
handleLookupSubscription sc _ req = catchDefaultEx $ C.lookupSubscription sc req

{-# DEPRECATED handleLookupShardReader "Use handleLookupResource instead" #-}
handleLookupShardReader
  :: ServerContext
  -> G.UnaryHandler LookupShardReaderRequest LookupShardReaderResponse
handleLookupShardReader sc _ req = catchDefaultEx $
  C.lookupShardReader sc req

{-# DEPRECATED handleLookupConnector "Use handleLookupResource instead" #-}
handleLookupConnector
  :: ServerContext -> G.UnaryHandler LookupConnectorRequest LookupConnectorResponse
handleLookupConnector sc _ req = catchDefaultEx $ C.lookupConnector sc req
