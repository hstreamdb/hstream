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
import           Control.Exception                (Exception (..), Handler (..),
                                                   catches, throwIO)
import           Control.Monad                    (unless, void)
import           Data.Functor                     ((<&>))
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           ZooKeeper                        (zooExists)

import           Data.Text                        (Text)
import           HStream.Common.ConsistentHashing (getAllocatedNode)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (alignDefault,
                                                   orderingKeyToStoreKey)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (mkServerErrResp, returnResp,
                                                   textToCBytes)

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
lookupStreamHandler ServerContext{..} (ServerNormalRequest _meta req@LookupStreamRequest {
  lookupStreamRequestStreamName  = stream,
  lookupStreamRequestOrderingKey = orderingKey}) = lookupStreamExceptionHandle $ do
  Log.info $ "receive lookupStream request: " <> Log.buildString' req
  hashRing <- readMVar loadBalanceHashRing
  let key      = alignDefault orderingKey
      keyCB    = textToCBytes key
      storeKey = orderingKeyToStoreKey key
      theNode  = getAllocatedNode hashRing (stream <> key)
      streamCB = textToCBytes stream
      streamID = S.mkStreamId S.StreamTypeStream streamCB
      path     = P.mkPartitionKeysPath streamCB <> "/" <> keyCB

  keyExist <- S.doesStreamPartitionExist scLDClient streamID storeKey
  unless keyExist $ do
    zooExists zkHandle path >>= \case
      Just _ -> do
        Log.fatal $ Log.buildText $ "key " <> key <> " of stream " <> stream <> " doesn't appear in store, but find in zk"
        throwIO $ DataInconsistency stream orderingKey
      Nothing -> do
        Log.debug $ "createStreamingPartition, stream: " <> Log.buildText stream <> ", key: " <> Log.buildText key
        catches (void $ S.createStreamPartition scLDClient streamID storeKey)
          [ Handler (\(_ :: S.StoreError) -> throwIO StreamNotExist), -- Stream does not exist
            Handler (\(_ :: S.EXISTS)     -> pure ()) -- Both stream and partition already exist
          ]
  P.tryCreate zkHandle path
  returnResp LookupStreamResponse {
    lookupStreamResponseStreamName  = stream
  , lookupStreamResponseOrderingKey = orderingKey
  , lookupStreamResponseServerNode  = Just theNode
  }

lookupSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupSubscriptionRequest LookupSubscriptionResponse
  -> IO (ServerResponse 'Normal LookupSubscriptionResponse)
lookupSubscriptionHandler ServerContext{..} (ServerNormalRequest _meta req@LookupSubscriptionRequest{
  lookupSubscriptionRequestSubscriptionId = subId}) = defaultExceptionHandle $ do
  Log.info $ "receive lookupSubscription request: " <> Log.buildString (show req)
  hashRing <- readMVar loadBalanceHashRing
  let theNode = getAllocatedNode hashRing subId
  returnResp LookupSubscriptionResponse {
    lookupSubscriptionResponseSubscriptionId = subId
  , lookupSubscriptionResponseServerNode     = Just theNode
  }

lookupSubscriptionWithOrderingKeyHandler
  :: ServerContext
  -> ServerRequest 'Normal LookupSubscriptionWithOrderingKeyRequest LookupSubscriptionWithOrderingKeyResponse
  -> IO (ServerResponse 'Normal LookupSubscriptionWithOrderingKeyResponse)
lookupSubscriptionWithOrderingKeyHandler ServerContext{..} (ServerNormalRequest _meta LookupSubscriptionWithOrderingKeyRequest {
    lookupSubscriptionWithOrderingKeyRequestSubscriptionId = subId
  , lookupSubscriptionWithOrderingKeyRequestOrderingKey    = key
  }) = defaultExceptionHandle $ do
  hashRing <- readMVar loadBalanceHashRing
  let theNode = getAllocatedNode hashRing (subId <> alignDefault key)
  returnResp LookupSubscriptionWithOrderingKeyResponse {
    lookupSubscriptionWithOrderingKeyResponseSubscriptionId = subId
  , lookupSubscriptionWithOrderingKeyResponseOrderingKey    = key
  , lookupSubscriptionWithOrderingKeyResponseServerNode     = Just theNode
  }

--------------------------------------------------------------------------------
-- Exception and Exception Handlers

data DataInconsistency = DataInconsistency Text Text
  deriving (Show)
instance Exception DataInconsistency where
  displayException (DataInconsistency streamName key) =
    "Partition " <> show key <> " of stream " <> show streamName
    <> " doesn't appear in store, but exists in zk."

lookupStreamExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
lookupStreamExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  dataInconsistencyHandler ++ defaultHandlers
  where
    dataInconsistencyHandler = [
      Handler (\(err :: DataInconsistency) ->
        return (StatusAborted, mkStatusDetails err))
      ]
