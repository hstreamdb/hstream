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
import           Control.Exception                (Handler (..), catches,
                                                   throwIO)
import           Control.Monad                    (unless, void, when)
import           Data.Functor                     ((<&>))
import           Data.Maybe                       (fromMaybe, isNothing)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           ZooKeeper                        (zooExists)

import           HStream.Common.ConsistentHashing (getAllocatedNode)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (DataInconsistency (..),
                                                   StreamNotExist (..),
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (clientDefaultKey',
                                                   orderingKeyToStoreKey)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (returnResp, textToCBytes)
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
lookupStreamHandler ServerContext{..} (ServerNormalRequest _meta req@(LookupStreamRequest stream orderingKey)) = defaultExceptionHandle $ do
  Log.info $ "receive lookupStream request: " <> Log.buildString (show req)
  hashRing <- readMVar loadBalanceHashRing
  let theNode = getAllocatedNode hashRing (stream <> orderingKey)
      streamName = textToCBytes stream
      streamID = S.mkStreamId S.StreamTypeStream streamName
      key = orderingKeyToStoreKey orderingKey
      path = P.mkPartitionKeysPath streamName <> "/" <> fromMaybe clientDefaultKey' key

  keyExist <- S.doesStreamPartitionExist scLDClient streamID key
  when (keyExist && isNothing key) $ P.tryCreate zkHandle path
  unless keyExist $ do
    zooExists zkHandle path >>= \case
      Just _ -> do
        Log.fatal $ "key " <> Log.buildString (show orderingKey)
                 <> " of stream " <> Log.buildText stream
                 <> " doesn't appear in store, but find in zk"
        throwIO $ DataInconsistency stream orderingKey
      Nothing -> do
        Log.debug $ "createStraemingPartition, stream: " <> Log.buildText stream <> ", key: " <> Log.buildString (show key)
        catches (void $ S.createStreamPartition scLDClient streamID key)
          [ Handler (\(_ :: S.StoreError) -> throwIO StreamNotExist), -- Stream not exists
            Handler (\(_ :: S.EXISTS) -> pure ()) -- both stream and partition are already exist
          ]
        P.tryCreate zkHandle path
  let resp = LookupStreamResponse {
      lookupStreamResponseStreamName = stream
    , lookupStreamResponseOrderingKey = orderingKey
    , lookupStreamResponseServerNode = Just theNode
    }
  returnResp resp

lookupSubscriptionHandler :: ServerContext
                          -> ServerRequest 'Normal LookupSubscriptionRequest LookupSubscriptionResponse
                          -> IO (ServerResponse 'Normal LookupSubscriptionResponse)
lookupSubscriptionHandler ServerContext{..} (ServerNormalRequest _meta req@(LookupSubscriptionRequest subId)) = defaultExceptionHandle $ do
  Log.info $ "receive lookupSubscription request: " <> Log.buildString (show req)
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
lookupSubscriptionWithOrderingKeyHandler ServerContext{..} (ServerNormalRequest _meta LookupSubscriptionWithOrderingKeyRequest {..}) = defaultExceptionHandle $ do
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
