{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Cluster
  ( -- * For grpc-haskell
    describeClusterHandler
  , lookupShardHandler
  , lookupSubscriptionHandler
  , lookupConnectorHandler
  , lookupShardReaderHandler
    -- * For hs-grpc-server
  , handleDescribeCluster
  , handleLookupShard
  , handleLookupSubscription
  , handleLookupShardReader
  , handleLookupConnector
  ) where

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

-------------------------------------------------------------------------------

handleDescribeCluster :: ServerContext -> Empty -> IO DescribeClusterResponse
handleDescribeCluster sc _ = catchDefaultEx $ C.describeCluster sc

handleLookupShard :: ServerContext -> LookupShardRequest -> IO LookupShardResponse
handleLookupShard sc req = catchDefaultEx $ C.lookupShard sc req

handleLookupSubscription
  :: ServerContext
  -> LookupSubscriptionRequest -> IO LookupSubscriptionResponse
handleLookupSubscription sc req = catchDefaultEx $ C.lookupSubscription sc req

handleLookupShardReader
  :: ServerContext
  -> LookupShardReaderRequest -> IO LookupShardReaderResponse
handleLookupShardReader sc req = catchDefaultEx $
  C.lookupShardReader sc req

handleLookupConnector
  :: ServerContext -> LookupConnectorRequest -> IO LookupConnectorResponse
handleLookupConnector sc req = catchDefaultEx $ C.lookupConnector sc req
