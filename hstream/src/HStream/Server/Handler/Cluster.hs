{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Cluster
  ( describeClusterHandler
  , lookupStreamHandler
  , lookupSubscriptionHandler
  ) where

import           Control.Concurrent               (readMVar)
import           Control.Exception                (throwIO)
import           Control.Monad                    (unless)
import           Data.Functor                     ((<&>))
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (def)
import           ZooKeeper                        (zooExists)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Common.ConsistentHashing (getAllocatedNode)
import           HStream.Server.Exception         (SubscriptionIdNotFound (SubscriptionIdNotFound),
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (integralToCBytes, returnResp)

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
  exists <- P.checkIfExist @ZHandle @'P.SubRep subId zkHandle
  unless exists $ throwIO SubscriptionIdNotFound
  P.getObject subId zkHandle >>= \case
    Just P.SubscriptionContext{..} -> do
      let path = integralToCBytes subHServer
      zooExists zkHandle (P.serverRootPath <> path) >>= \case
        Just _ -> do
          theNode <- P.getServerNode' zkHandle path
          returnResp resp {lookupSubscriptionResponseServerNode = Just theNode}
        Nothing -> returnNewNode
    Nothing -> returnNewNode
  where
    resp = def { lookupSubscriptionResponseSubscriptionId = subId }
    returnNewNode = do
      hashRing <- readMVar loadBalanceHashRing
      let theNode = getAllocatedNode hashRing subId
      P.storeObject subId P.SubscriptionContext { subHServer = serverNodeId theNode} zkHandle
      returnResp resp {lookupSubscriptionResponseServerNode = Just theNode}
