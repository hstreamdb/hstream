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
  ) where

import           Control.Concurrent               (readMVar)
import           Data.Functor                     ((<&>))
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
lookupStreamHandler ServerContext{..} (ServerNormalRequest _meta (LookupStreamRequest stream)) = defaultExceptionHandle $ do
  hashRing <- readMVar loadBalanceHashRing
  let theNode = getAllocatedNode hashRing stream
  let resp = LookupStreamResponse {
      lookupStreamResponseStreamName = stream
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
