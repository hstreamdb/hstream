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
  , listSubscriptionsWithPrefixHandler
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
  , handleListSubscriptionsWithPrefix
  , handleStreamingFetch
  )
where

import           Control.Applicative              ((<|>))
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
import           HStream.Server.Validation
import           HStream.ThirdParty.Protobuf      as PB
import           HStream.Utils                    (ResourceType (ResSubscription),
                                                   returnResp,
                                                   validateNameAndThrow)

-------------------------------------------------------------------------------

-- NOTE: All validations should not be implemented in the core module
-- (HStream.Server.Handler.Core). Since some internal subscription names
-- (e.g. start with HStream.Server.HStore.hstoreSubscriptionPrefix) are "invalid"
-- (can not pass validateNameAndThrow). And other internal modules will call core
-- directly.

createSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal Subscription Subscription
  -> IO (ServerResponse 'Normal Subscription)
createSubscriptionHandler ctx (ServerNormalRequest _metadata sub) = defaultExceptionHandle $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  validateSubscription sub
  Core.createSubscription ctx sub >>= returnResp

handleCreateSubscription :: ServerContext -> G.UnaryHandler Subscription Subscription
handleCreateSubscription sc _ sub = catchDefaultEx $ do
  Log.debug $ "Receive createSubscription request: " <> Log.buildString' sub
  validateSubscription sub
  Core.createSubscription sc sub

-------------------------------------------------------------------------------

getSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal GetSubscriptionRequest GetSubscriptionResponse
  -> IO (ServerResponse 'Normal GetSubscriptionResponse)
getSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  Log.debug $ "Receive getSubscription request: " <> Log.buildString' req
  let subId = getSubscriptionRequestId req
  --validateNameAndThrow ResSubscription subId
  validateResLookup ctx ResSubscription subId "Subscription is bound to a different node"
  Core.getSubscription ctx req >>= returnResp

handleGetSubscription :: ServerContext -> G.UnaryHandler GetSubscriptionRequest GetSubscriptionResponse
handleGetSubscription ctx@ServerContext{..} _ req = catchDefaultEx $ do
  Log.debug $ "Receive getSubscription request: " <> Log.buildString' req
  let subId = getSubscriptionRequestId req
  -- FIXME: Some internal Subscription names have a
  -- HStream.Server.HStore.hstoreSubscriptionPrefix, which can not pass validateNameAndThrow
  --validateNameAndThrow ResSubscription subId
  validateResLookup ctx ResSubscription subId "Subscription is bound to a different node"
  Core.getSubscription ctx req

-------------------------------------------------------------------------------

listConsumersHandler :: ServerContext -> ServerRequest 'Normal ListConsumersRequest ListConsumersResponse -> IO (ServerResponse 'Normal ListConsumersResponse)
listConsumersHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  let subId = listConsumersRequestSubscriptionId req
  --validateNameAndThrow ResSubscription subId
  validateResLookup ctx ResSubscription subId "Subscription is bound to a different node"
  Core.listConsumers ctx req >>= returnResp

handleListConsumers :: ServerContext -> G.UnaryHandler ListConsumersRequest ListConsumersResponse
handleListConsumers ctx@ServerContext{..} _ req = catchDefaultEx $ do
  let subId = listConsumersRequestSubscriptionId req
  -- FIXME: Some internal Subscription names have a
  -- HStream.Server.HStore.hstoreSubscriptionPrefix, which can not pass validateNameAndThrow
  --validateNameAndThrow ResSubscription subId
  validateResLookup ctx ResSubscription subId "Subscription is bound to a different node"
  Core.listConsumers ctx req

--------------------------------------------------------------------------------

deleteSubscriptionHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteSubscriptionRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteSubscriptionHandler ctx@ServerContext{..} (ServerNormalRequest _metadata req) = defaultExceptionHandle $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req
  let subId = deleteSubscriptionRequestSubscriptionId req
  validateNameAndThrow ResSubscription subId
  validateResLookup ctx ResSubscription subId "Subscription is bound to a different node"
  Core.deleteSubscription ctx req
  returnResp Empty

handleDeleteSubscription :: ServerContext -> G.UnaryHandler DeleteSubscriptionRequest Empty
handleDeleteSubscription ctx@ServerContext{..} _ req = catchDefaultEx $ do
  Log.debug $ "Receive deleteSubscription request: " <> Log.buildString' req
  let subId = deleteSubscriptionRequestSubscriptionId req
  validateNameAndThrow ResSubscription subId
  validateResLookup ctx ResSubscription subId "Subscription is bound to a different node"
  Core.deleteSubscription ctx req
  pure Empty

-------------------------------------------------------------------------------

checkSubscriptionExistHandler
  :: ServerContext
  -> ServerRequest 'Normal CheckSubscriptionExistRequest CheckSubscriptionExistResponse
  -> IO (ServerResponse 'Normal CheckSubscriptionExistResponse)
checkSubscriptionExistHandler sc (ServerNormalRequest _metadata req@CheckSubscriptionExistRequest {..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive checkSubscriptionExistHandler request: " <> Log.buildString (show req)
  res <- Core.checkSubscriptionExist sc checkSubscriptionExistRequestSubscriptionId
  returnResp . CheckSubscriptionExistResponse $ res

handleCheckSubscriptionExist
  :: ServerContext
  -> G.UnaryHandler CheckSubscriptionExistRequest CheckSubscriptionExistResponse
handleCheckSubscriptionExist sc _ req = catchDefaultEx $ do
  let sid = checkSubscriptionExistRequestSubscriptionId req
  res <- Core.checkSubscriptionExist sc sid
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

listSubscriptionsWithPrefixHandler
  :: ServerContext
  -> ServerRequest 'Normal ListSubscriptionsWithPrefixRequest ListSubscriptionsResponse
  -> IO (ServerResponse 'Normal ListSubscriptionsResponse)
listSubscriptionsWithPrefixHandler sc (ServerNormalRequest _metadata ListSubscriptionsWithPrefixRequest{..}) = defaultExceptionHandle $ do
  Log.debug "Receive listSubscriptions request"
  validateNameAndThrow ResSubscription listSubscriptionsWithPrefixRequestPrefix
  res <- ListSubscriptionsResponse <$> Core.listSubscriptionsWithPrefix sc listSubscriptionsWithPrefixRequestPrefix
  Log.debug $ Log.buildString "Result of listSubscriptions: " <> Log.buildString (show res)
  returnResp res

handleListSubscriptionsWithPrefix
  :: ServerContext
  -> G.UnaryHandler ListSubscriptionsWithPrefixRequest ListSubscriptionsResponse
handleListSubscriptionsWithPrefix sc _ ListSubscriptionsWithPrefixRequest{..} = catchDefaultEx $ do
  Log.debug "Receive listSubscriptions request"
  validateNameAndThrow ResSubscription listSubscriptionsWithPrefixRequestPrefix
  ListSubscriptionsResponse <$> Core.listSubscriptionsWithPrefix sc listSubscriptionsWithPrefixRequestPrefix

-------------------------------------------------------------------------------

streamingFetchHandler
  :: ServerContext
  -> ServerRequest 'BiDiStreaming StreamingFetchRequest StreamingFetchResponse
  -> IO (ServerResponse 'BiDiStreaming StreamingFetchResponse)
streamingFetchHandler ctx (ServerBiDiRequest meta streamRecv streamSend) =
  defaultBiDiStreamExceptionHandle $ do
    let metaMap = unMap $ metadata meta
    uri <- case Map.lookup "x-forwarded-for" metaMap of
          Nothing     -> T.pack <$> grpcCallGetPeer (unsafeSC meta)
          Just []     -> T.pack <$> grpcCallGetPeer (unsafeSC meta)
          Just (x:xs) -> return $ T.decodeUtf8 x
    let agent = getMeta $ Map.lookup "proxy-agent" metaMap
                      <|> Map.lookup "user-agent"  metaMap
    Log.debug "recv server call: streamingFetch"
    Core.streamingFetchCore ctx Core.SFetchCoreInteractive (streamSend, streamRecv, uri, agent)
    return $ ServerBiDiResponse mempty StatusUnknown "should not reach here"
  where
    getMeta = \case Nothing -> ""; Just [] -> ""; Just (x:xs) -> T.decodeUtf8 x

-- TODO: improvements for read or write error
handleStreamingFetch
  :: ServerContext
  -> G.BidiStreamHandler StreamingFetchRequest StreamingFetchResponse ()
handleStreamingFetch sc gCtx stream = do
  uri <- G.findClientMetadata gCtx "x-forwarded-for" >>= \case
    Nothing -> G.serverContextPeer gCtx
    Just x  -> return x
  agent <- maybe "" T.decodeUtf8 <$> (G.findClientMetadata gCtx "proxy-agent"
                                  <|> G.findClientMetadata gCtx "user-agent")
  let streamSend x = first (const GRPCIOShutdown) <$> G.streamWrite stream (Just x)
      streamRecv = do Right <$> G.streamRead stream
  catchDefaultEx $ Core.streamingFetchCore sc Core.SFetchCoreInteractive (streamSend, streamRecv, T.decodeUtf8 uri, agent)
