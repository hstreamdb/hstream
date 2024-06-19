{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Cluster
  ( -- * For grpc-haskell
    describeClusterHandler
  , lookupShardHandler
  , lookupSubscriptionHandler
  , lookupShardReaderHandler
  , lookupResourceHandler
  , lookupKeyHandler
    -- * For hs-grpc-server
  , handleDescribeCluster
  , handleLookupResource
  , handleLookupShard
  , handleLookupSubscription
  , handleLookupShardReader
  , handleLookupKey
  ) where

import           Control.Exception                (throwIO, try)
import           Data.IORef                       (readIORef)
import           Network.GRPC.HighLevel.Generated

import qualified HsGrpc.Server                    as G
import           HStream.Common.Server.Lookup     (lookupNode)
import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Cluster      as C
import           HStream.Server.Core.Common       (lookupResource)
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
-- import           HStream.Server.Types             (ServerContext (..))
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerMode (..))
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnResp,
                                                   validateResourceIdAndThrow)
import           Proto3.Suite                     (Enumerated (..))

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
lookupResourceHandler sc@ServerContext{..} (ServerNormalRequest _meta req@LookupResourceRequest{..}) =
  defaultExceptionHandle $ do
  Log.info $ "receive lookup resource request: " <> Log.build (show req)
  case lookupResourceRequestResType of
    Enumerated (Right rType) -> do
      validateResourceIdAndThrow rType lookupResourceRequestResId
      -- returnResp =<< lookupResource sc rType lookupResourceRequestResId
      state <- readIORef serverState
      case state of
        ServerNormal -> do
          returnResp =<< lookupResource sc rType lookupResourceRequestResId
        ServerBackup -> do
          theNode <- case rType of
            ResourceTypeResShard -> do
              doLookup lookupResourceRequestResId
            -- ResourceTypeResStream -> doLookup lookupResourceRequestResId
            tp -> do
              Log.warning $ "reject lookup " <> Log.build (show tp) <> " request because server is in backup mode"
              throwIO $ HE.ResourceAllocationException "server is in backup mode, try later"
          returnResp theNode
    x -> throwIO $ HE.InvalidResourceType (show x)
 where
   doLookup rid = lookupNode loadBalanceHashRing rid scAdvertisedListenersKey

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

{-# DEPRECATED lookupShardReaderHandler "Use lookupResourceHandler instead" #-}
lookupShardReaderHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupShardReaderRequest LookupShardReaderResponse
  -> IO (ServerResponse 'Normal LookupShardReaderResponse)
lookupShardReaderHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupShardReader sc req

lookupKeyHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupKeyRequest ServerNode
  -> IO (ServerResponse 'Normal ServerNode)
lookupKeyHandler sc (ServerNormalRequest _meta req) =
  defaultExceptionHandle $ returnResp =<< C.lookupKey sc req

-------------------------------------------------------------------------------

handleDescribeCluster :: ServerContext -> G.UnaryHandler Empty DescribeClusterResponse
handleDescribeCluster sc _ _ = catchDefaultEx $ C.describeCluster sc

handleLookupResource :: ServerContext -> G.UnaryHandler LookupResourceRequest ServerNode
handleLookupResource sc@ServerContext{..} _sc req@LookupResourceRequest{..} = catchDefaultEx $ do
  -- Log.debug $ "receive lookup resource request: " <> Log.build (show req)
  -- case lookupResourceRequestResType of
  --   Enumerated (Right rType) -> do
  --     validateResourceIdAndThrow rType lookupResourceRequestResId
  --     C.lookupResource sc rType lookupResourceRequestResId
  --   x -> throwIO $ HE.InvalidResourceType (show x)
  Log.info $ "receive lookup resource request: " <> Log.build (show req)
  case lookupResourceRequestResType of
    Enumerated (Right rType) -> do
      validateResourceIdAndThrow rType lookupResourceRequestResId
      state <- readIORef serverState
      case state of
        ServerNormal -> do
          res <- try $ lookupResource sc rType lookupResourceRequestResId
          case res of
            Left (_ :: HE.ResourceAllocationException) -> handleLookupResource sc _sc req
            Right res' -> return res'
        ServerBackup -> do
          case rType of
            ResourceTypeResShard -> do
              doLookup lookupResourceRequestResId
            tp -> do
              Log.warning $ "reject lookup " <> Log.build (show tp) <> " request because server is in backup mode"
              throwIO $ HE.ResourceAllocationException "server is in backup mode, try later"
    x -> throwIO $ HE.InvalidResourceType (show x)
 where
   doLookup rid = lookupNode loadBalanceHashRing rid scAdvertisedListenersKey

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

handleLookupKey :: ServerContext -> G.UnaryHandler LookupKeyRequest ServerNode
handleLookupKey sc _ req = catchDefaultEx $ C.lookupKey sc req
