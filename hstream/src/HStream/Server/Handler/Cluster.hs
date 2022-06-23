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

import           Control.Concurrent.STM           (readTVarIO)
import           Control.Exception                (Exception (..), Handler (..),
                                                   catches, throwIO)
import           Control.Monad                    (unless, void)
import           Data.Functor                     ((<&>))
import           Data.Text                        (Text)
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Common.ConsistentHashing (getAllocatedNode)
import           HStream.Connector.HStore         (transToStreamName)
import           HStream.Gossip                   (getMemberList)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception
import           HStream.Server.Handler.Common    (alignDefault,
                                                   orderingKeyToStoreKey)
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Server.Types             as Types
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty)
import           HStream.Utils                    (mkServerErrResp, returnResp)

describeClusterHandler :: ServerContext
                       -> ServerRequest 'Normal Empty DescribeClusterResponse
                       -> IO (ServerResponse 'Normal DescribeClusterResponse)
describeClusterHandler ServerContext{..} (ServerNormalRequest _meta _) = defaultExceptionHandle $ do
  let protocolVer = Types.protocolVersion
      serverVer   = Types.serverVersion
  nodes <- getMemberList gossipContext
  let resp = DescribeClusterResponse {
      describeClusterResponseProtocolVersion = protocolVer
    , describeClusterResponseServerVersion   = serverVer
    , describeClusterResponseServerNodes     = V.fromList nodes
    }
  returnResp resp

lookupStreamHandler :: ServerContext
                    -> ServerRequest 'Normal LookupStreamRequest LookupStreamResponse
                    -> IO (ServerResponse 'Normal LookupStreamResponse)
lookupStreamHandler ServerContext{..} (ServerNormalRequest _meta req@LookupStreamRequest {
  lookupStreamRequestStreamName  = stream,
  lookupStreamRequestOrderingKey = orderingKey}) = lookupStreamExceptionHandle $ do
  Log.info $ "receive lookupStream request: " <> Log.buildString' req
  hashRing <- readTVarIO loadBalanceHashRing
  let key      = alignDefault orderingKey
      storeKey = orderingKeyToStoreKey key
      theNode  = getAllocatedNode hashRing (stream <> key)
      streamID = transToStreamName stream
  keyExist <- S.doesStreamPartitionExist scLDClient streamID storeKey
  unless keyExist $ do
    Log.debug $ "createStreamingPartition, stream: " <> Log.buildText stream <> ", key: " <> Log.buildText key
    catches (void $ S.createStreamPartition scLDClient streamID storeKey)
      [ Handler (\(_ :: S.NOTFOUND)   -> throwIO StreamNotExist)
      , Handler (\(_ :: S.EXISTS)     -> pure ())                -- Both stream and partition already exist
      , Handler (\(_ :: S.StoreError) -> throwIO StreamNotExist) -- FIXME: should not throw StreamNotFound
      ]
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
  hashRing <- readTVarIO loadBalanceHashRing
  let theNode = getAllocatedNode hashRing subId
  returnResp LookupSubscriptionResponse {
    lookupSubscriptionResponseSubscriptionId = subId
  , lookupSubscriptionResponseServerNode     = Just theNode
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
