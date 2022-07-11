{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE MultiWayIf          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Subscription
  (
    createSubscriptionHandler,
    deleteSubscriptionHandler,
    listSubscriptionsHandler,
    checkSubscriptionExistHandler,
    streamingFetchHandler
  )
where

import           Control.Concurrent.STM
import           Control.Exception                (Handler (Handler), throwIO)
import           Control.Monad
import           Network.GRPC.HighLevel.Generated
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Common.ConsistentHashing (getAllocatedNodeId)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception         (ExceptionHandle, Handlers,
                                                   defaultHandlers,
                                                   mkExceptionHandle,
                                                   setRespType)
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence       (ObjRepType (..))
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (mkServerErrResp, returnResp)

--------------------------------------------------------------------------------

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ctx (ServerNormalRequest _metadata sub) = subExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  Core.createSubscription ctx sub
  returnResp sub

--------------------------------------------------------------------------------

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req) = subExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req

  let subId = deleteSubscriptionRequestSubscriptionId req
  hr <- readTVarIO loadBalanceHashRing
  unless (getAllocatedNodeId hr subId == serverID) $
    throwIO Core.SubscriptionOnDifferentNode

  Core.deleteSubscription ctx req
  Log.info " ----------- successfully deleted subscription  -----------"
  returnResp Empty

-----------------------------------------------------------------------------------

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = checkSubscriptionExistRequestSubscriptionId
  res <- P.checkIfExist @ZHandle @'SubRep sid zkHandle
  returnResp . CheckSubscriptionExistResponse $ res
-- --------------------------------------------------------------------------------

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler sc (ServerNormalRequest _metadata ListSubscriptionsRequest) = subExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse <$> Core.listSubscriptions sc
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res
-- --------------------------------------------------------------------------------

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx bidiRequest = subStreamingExceptionHandle do
  Log.debug "recv server call: streamingFetch"
  streamingFetchInternal ctx bidiRequest
  return $ ServerBiDiResponse mempty StatusUnknown "should not reach here"

streamingFetchInternal
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO ()
streamingFetchInternal ctx (ServerBiDiRequest _ streamRecv streamSend) = do
  Core.streamingFetchCore ctx Core.SFetchCoreInteractive (streamSend,streamRecv)

--------------------------------------------------------------------------------
-- Exception and Exception Handlers

subscriptionExceptionHandler :: Handlers (StatusCode, StatusDetails)
subscriptionExceptionHandler = [
  Handler (\(err :: Core.SubscriptionOnDifferentNode) -> do
    Log.warning $ Log.buildString' err
    return (StatusAborted, "Subscription is bound to a different node")),
  Handler (\(err :: Core.FoundActiveConsumers) -> do
    Log.warning $ Log.buildString' err
    return (StatusFailedPrecondition, "Subscription still has active consumers")),
  Handler (\(err :: Core.SubscriptionIsDeleting) -> do
    Log.warning $ Log.buildString' err
    return (StatusAborted, "Subscription is being deleted, please wait a while")),
  Handler (\(err :: Core.InvalidSubscriptionOffset) -> do
    Log.warning $ Log.buildString' err
    return (StatusInvalidArgument, "subscriptionOffset is invalid."))
  ]

subExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
subExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  subscriptionExceptionHandler ++ defaultHandlers

subStreamingExceptionHandle :: ExceptionHandle (ServerResponse 'BiDiStreaming a)
subStreamingExceptionHandle = mkExceptionHandle . setRespType (ServerBiDiResponse mempty) $
  innerErrorHandlers ++ subscriptionExceptionHandler ++ defaultHandlers

innerErrorHandlers :: Handlers (StatusCode, StatusDetails)
innerErrorHandlers = [Handler $ \(err :: Core.SubscribeInnerError) -> case err of
  Core.GRPCStreamRecvError      -> do
    Log.warning "Consumer recv error"
    return (StatusCancelled, "Consumer recv error")
  Core.GRPCStreamRecvCloseError -> do
    Log.warning "Consumer is closed"
    return (StatusCancelled, "Consumer is closed")
  Core.GRPCStreamSendError      -> do
    Log.warning "Consumer send request error"
    return (StatusCancelled, "Consumer send request error")
  Core.SubscribeInValidError    -> do
    Log.warning "Invalid Subscription"
    return (StatusAborted, "Invalid Subscription")
  Core.ConsumerInValidError     -> do
    Log.warning "Invalid Consumer"
    return (StatusAborted, "Invalid Consumer")
  ]
