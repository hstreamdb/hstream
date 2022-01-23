{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Cluster
  ( describeClusterHandler
  , lookupStreamHandler
  , lookupSubscriptionHandler
  , lookupSubscriptionWithOrderingKeyHandler
  ) where

import           Control.Concurrent               (readMVar)
import           Data.Functor                     ((<&>))
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Common.ConsistentHashing (getAllocatedNode)
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnResp)

--------------------------------------------------------------------------------

describeClusterHandler :: ServerContext
                       -> ServerRequest 'Normal Empty DescribeClusterResponse
                       -> IO (ServerResponse 'Normal DescribeClusterResponse)
describeClusterHandler ServerContext{..} (ServerNormalRequest _meta _) = defaultExceptionHandle $ do
  let protocolVer = Types.protocolVersion
      serverVer   = Types.serverVersion
  nodes <- P.getServerNodes zkHandle <&> V.fromList
  let resp = DescribeClusterResponse {
      describeClusterResponseProtocolVersion = protocolVer
    , describeClusterResponseServerVersion   = serverVer
    , describeClusterResponseServerNodes     = nodes
    }
  returnResp resp

lookupStreamHandler :: ServerContext
                    -> ServerRequest 'Normal LookupStreamRequest LookupStreamResponse
                    -> IO (ServerResponse 'Normal LookupStreamResponse)
lookupStreamHandler ServerContext{..} (ServerNormalRequest _meta (LookupStreamRequest stream orderingKey)) = defaultExceptionHandle $ do
  hashRing <- readMVar loadBalanceHashRing
  let theNode = getAllocatedNode hashRing (stream `T.append` orderingKey)
  let resp = LookupStreamResponse {
      lookupStreamResponseStreamName = stream
    , lookupStreamResponseOrderingKey = orderingKey
    , lookupStreamResponseServerNode = Just theNode
    }
  returnResp resp

lookupSubscriptionHandler :: ServerContext
                          -> ServerRequest 'Normal LookupSubscriptionRequest LookupSubscriptionResponse
                          -> IO (ServerResponse 'Normal LookupSubscriptionResponse)
lookupSubscriptionHandler ServerContext{..} (ServerNormalRequest _meta (LookupSubscriptionRequest subId)) = defaultExceptionHandle $ do
  hashRing <- readMVar loadBalanceHashRing
  let theNode = getAllocatedNode hashRing subId
  let resp = LookupSubscriptionResponse {
      lookupSubscriptionResponseSubscriptionId = subId
    , lookupSubscriptionResponseServerNode = Just theNode
    }
  returnResp resp

lookupSubscriptionWithOrderingKeyHandler :: ServerContext
                          -> ServerRequest 'Normal LookupSubscriptionWithOrderingKeyRequest LookupSubscriptionWithOrderingKeyResponse
                          -> IO (ServerResponse 'Normal LookupSubscriptionWithOrderingKeyResponse)
lookupSubscriptionWithOrderingKeyHandler ServerContext{..} (ServerNormalRequest _meta (LookupSubscriptionWithOrderingKeyRequest {..})) = defaultExceptionHandle $ do
  hashRing <- readMVar loadBalanceHashRing
  let subId = lookupSubscriptionWithOrderingKeyRequestSubscriptionId
  let orderingKey = lookupSubscriptionWithOrderingKeyRequestOrderingKey
  let theNode = getAllocatedNode hashRing (subId `T.append` orderingKey)
  let resp = LookupSubscriptionWithOrderingKeyResponse {
      lookupSubscriptionWithOrderingKeyResponseSubscriptionId = subId
    , lookupSubscriptionWithOrderingKeyResponseOrderingKey = orderingKey
    , lookupSubscriptionWithOrderingKeyResponseServerNode = Just theNode
    }
  returnResp resp
