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
import           Control.Concurrent               (readMVar)
import           Control.Exception                (Exception (..), Handler (..))
import           Data.Functor                     ((<&>))
import           Data.Text                        (Text)
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Common.ConsistentHashing (HashRing, getAllocatedNode)
import           HStream.Common.Types             (fromInternalServerNodeWithKey)
import           HStream.Connector.HStore         (transToStreamName)
import           HStream.Gossip                   (getMemberList)
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception
import           HStream.Server.Handler.Common    (alignDefault)
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
  nodes' <- mapM (fromInternalServerNodeWithKey scAdvertisedListenersKey) nodes

  returnResp $ DescribeClusterResponse
    { describeClusterResponseProtocolVersion = protocolVer
    , describeClusterResponseServerVersion   = serverVer
    , describeClusterResponseServerNodes     = V.concat nodes'
    }

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
      streamID = transToStreamName stream
  theNode <- getResNode hashRing (stream <> key) scAdvertisedListenersKey
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
  theNode <- getResNode hashRing subId scAdvertisedListenersKey
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

getResNode :: HashRing -> Text -> Maybe Text -> IO ServerNode
getResNode hashRing hashKey listenerKey = do
  let serverNode = getAllocatedNode hashRing hashKey
  theNodes <- fromInternalServerNodeWithKey listenerKey serverNode
  if V.null theNodes then throwIO ObjectNotExist
                     else pure $ V.head theNodes
