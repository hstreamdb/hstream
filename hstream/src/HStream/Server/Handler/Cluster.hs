{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Cluster
  ( describeClusterHandler
  , lookupShardHandler
  , lookupSubscriptionHandler
  , lookupShardReaderHandler
  ) where

import           Network.GRPC.HighLevel.Generated

import qualified HStream.Server.Core.Cluster      as C
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types             (ServerContext (..))
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnResp)

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

lookupShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupShardReaderRequest LookupShardReaderResponse
  -> IO (ServerResponse 'Normal LookupShardReaderResponse)
lookupShardReaderHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShardReader sc req
