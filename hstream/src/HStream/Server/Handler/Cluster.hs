{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Cluster
  ( describeClusterHandler
  , lookupStreamHandler
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

describeClusterHandler :: ServerContext
                       -> ServerRequest 'Normal Empty DescribeClusterResponse
                       -> IO (ServerResponse 'Normal DescribeClusterResponse)
describeClusterHandler sc (ServerNormalRequest _meta _) =
  defaultExceptionHandle $ returnResp =<< C.describeCluster sc

lookupStreamHandler :: ServerContext
                    -> ServerRequest 'Normal LookupStreamRequest LookupStreamResponse
                    -> IO (ServerResponse 'Normal LookupStreamResponse)
lookupStreamHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupStream sc req

lookupSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupSubscriptionRequest LookupSubscriptionResponse
  -> IO (ServerResponse 'Normal LookupSubscriptionResponse)
lookupSubscriptionHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupSubscription sc req

lookupShardReaderHandler :: ServerContext
                    -> ServerRequest 'Normal LookupShardReaderRequest LookupShardReaderResponse
                    -> IO (ServerResponse 'Normal LookupShardReaderResponse)
lookupShardReaderHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShardReader sc req
