{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Subscription
  ( -- * For grpc-haskell
    createSubscriptionHandler
  , deleteSubscriptionHandler
  , getSubscriptionHandler
  , listSubscriptionsHandler
  , listConsumersHandler
  , checkSubscriptionExistHandler
  , streamingFetchHandler
    -- * For hs-grpc-server
  , handleCreateSubscription
  , handleListConsumers
  , handleDeleteSubscription
  , handleGetSubscription
  , handleCheckSubscriptionExist
  , handleListSubscriptions
  , handleStreamingFetch
  )
where

import           Control.Exception                (throwIO)
import           Control.Monad
import           Data.Bifunctor                   (first)
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as T
import qualified HsGrpc.Server                    as G
import qualified HsGrpc.Server.Context            as G
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.Unsafe

import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.Core.Common       (lookupResource')
import qualified HStream.Server.Core.Subscription as Core
import           HStream.Server.Exception
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (ResourceType (ResSubscription),
                                                   returnResp,
                                                   validateNameAndThrow)

-------------------------------------------------------------------------------

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ctx (ServerNormalRequest _metadata sub) = defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  validateNameAndThrow $ subscriptionSubscriptionId sub
  Core.createSubscription ctx sub >>= returnResp

handleCreateSubscription :: ServerContext -> G.UnaryHandler Subscription Subscription
handleCreateSubscription sc _ sub = catchDefaultEx $
  Core.createSubscription sc sub

-------------------------------------------------------------------------------

getSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal GetSubscriptionRequest GetSubscriptionResponse
  -> IO (ServerResponse 'Normal GetSubscriptionResponse)
getSubscriptionHandler ctx (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  Log.debug $ "Receive getSubscription request: " <> Log.buildString' req
  validateNameAndThrow $ getSubscriptionRequestId req
  Core.getSubscription ctx req >>= returnResp

handleGetSubscription :: ServerContext -> G.UnaryHandler GetSubscriptionRequest GetSubscriptionResponse
handleGetSubscription sc _ req = catchDefaultEx $ do
  validateNameAndThrow $ getSubscriptionRequestId req
  Core.getSubscription sc req

-------------------------------------------------------------------------------

listConsumersHandler :: ServerContext -> ServerRequest 'Normal ListConsumersRequest ListConsumersResponse -> IO (ServerResponse 'Normal ListConsumersResponse)
listConsumersHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  let subId = listConsumersRequestSubscriptionId req
  validateNameAndThrow subId
  ServerNode{..} <- lookupResource' ctx ResSubscription subId
  unless (serverNodeId == serverID) $
    throwIO $ HE.SubscriptionOnDifferentNode "Subscription is bound to a different node"
  Core.listConsumers ctx req >>= returnResp

handleListConsumers :: ServerContext -> G.UnaryHandler ListConsumersRequest ListConsumersResponse
handleListConsumers ctx@ServerContext{..} _ req = catchDefaultEx $ do
  let subId = listConsumersRequestSubscriptionId req
  validateNameAndThrow subId
  ServerNode{..} <- lookupResource' ctx ResSubscription subId
  unless (serverNodeId == serverID) $
    throwIO $ HE.SubscriptionOnDifferentNode "Subscription is bound to a different node"
  Core.listConsumers ctx req

--------------------------------------------------------------------------------

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req
  let subId = deleteSubscriptionRequestSubscriptionId req
  validateNameAndThrow subId
  ServerNode{..} <- lookupResource' ctx ResSubscription subId
  unless (serverNodeId == serverID) $
    throwIO $ HE.SubscriptionOnDifferentNode "Subscription is bound to a different node"

  Core.deleteSubscription ctx req
  Log.info " ----------- successfully deleted subscription  -----------"
  returnResp Empty

handleDeleteSubscription :: ServerContext -> G.UnaryHandler DeleteSubscriptionRequest Empty
handleDeleteSubscription ctx@ServerContext{..} _ req = catchDefaultEx $ do
  let subId = deleteSubscriptionRequestSubscriptionId req
  validateNameAndThrow subId
  ServerNode{..} <- lookupResource' ctx ResSubscription subId
  unless (serverNodeId == serverID) $
    throwIO $ HE.SubscriptionOnDifferentNode "Subscription is bound to a different node"
  Core.deleteSubscription ctx req
  pure Empty

-------------------------------------------------------------------------------

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler ServerContext {..} (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  let sid = checkSubscriptionExistRequestSubscriptionId
  res <- M.checkMetaExists @SubscriptionWrap sid metaHandle
  returnResp . CheckSubscriptionExistResponse $ res

handleCheckSubscriptionExist
  :: ServerContext
  -> G.UnaryHandler CheckSubscriptionExistRequest CheckSubscriptionExistResponse
handleCheckSubscriptionExist ServerContext{..} _ req = catchDefaultEx $ do
  let sid = checkSubscriptionExistRequestSubscriptionId req
  res <- M.checkMetaExists @SubscriptionWrap sid metaHandle
  pure $ CheckSubscriptionExistResponse res

-------------------------------------------------------------------------------

listSubscriptionsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsHandler sc (ServerNormalRequest _metadata ListSubscriptionsRequest) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  res <- ListSubscriptionsResponse <$> Core.listSubscriptions sc
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res

handleListSubscriptions
  :: ServerContext
  -> G.UnaryHandler ListSubscriptionsRequest ListSubscriptionsResponse
handleListSubscriptions sc _ ListSubscriptionsRequest = catchDefaultEx $ do
  ListSubscriptionsResponse <$> Core.listSubscriptions sc

-------------------------------------------------------------------------------

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx (ServerBiDiRequest meta streamRecv streamSend) =
  defaultBiDiStreamExceptionHandle $ do
    uri <- grpcCallGetPeer $ unsafeSC meta
    let agent = case Map.lookup "user-agent" . unMap $ metadata meta of
          Nothing     -> ""
          Just []     -> ""
          Just (x:xs) -> T.decodeUtf8 x
    Log.debug "recv server call: streamingFetch"
    Core.streamingFetchCore ctx Core.SFetchCoreInteractive (streamSend, streamRecv, T.pack uri, agent)
    return $ ServerBiDiResponse mempty StatusUnknown "should not reach here"

-- TODO: improvements for read or write error
handleStreamingFetch
  :: ServerContext
  -> G.BidiStreamHandler StreamingFetchRequest StreamingFetchResponse ()
handleStreamingFetch sc gCtx stream = do
  uri <- G.serverContextPeer gCtx
  agent <- maybe "" T.decodeUtf8 <$> G.findClientMetadata gCtx "user-agent"
  let streamSend x = first (const GRPCIOShutdown) <$> G.streamWrite stream (Just x)
      streamRecv = do Right <$> G.streamRead stream
  catchDefaultEx $ Core.streamingFetchCore sc Core.SFetchCoreInteractive (streamSend, streamRecv, T.decodeUtf8 uri, agent)
