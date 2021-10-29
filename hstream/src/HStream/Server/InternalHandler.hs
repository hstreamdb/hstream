{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.InternalHandler where

import           Control.Concurrent
import qualified Data.Map                         as Map
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamInternal
import           HStream.Server.LoadBalance       (getRanking)
import           HStream.Server.Persistence       (getServerNode)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types             (ProducerContext (ProducerContext),
                                                   ServerContext (..),
                                                   SubscriptionContext (..))
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (returnErrResp, returnResp)
import           Network.GRPC.HighLevel.Generated

internalHandlers :: ServerContext -> IO (HStreamInternal ServerRequest ServerResponse)
internalHandlers ctx = pure HStreamInternal {
  -- TODO : add corresponding implementation and subscription api
    hstreamInternalCreateQueryStream   = unimplemented
  , hstreamInternalRestartQuery        = unimplemented
  , hstreamInternalTerminateQueries    = unimplemented
  , hstreamInternalCreateSinkConnector = unimplemented
  , hstreamInternalRestartConnector    = unimplemented
  , hstreamInternalTerminateConnector  = unimplemented

  , hstreamInternalGetNodesRanking     = getNodesRankingHandler ctx
  , hstreamInternalTakeSubscription    = takeSubscription ctx
  , hstreamInternalTakeStream          = takeStream ctx
  }
  where
    unimplemented = const (returnErrResp StatusInternal "unimplemented method called")

getNodesRankingHandler :: ServerContext
                       -> ServerRequest 'Normal Empty GetNodesRankingResponse
                       -> IO (ServerResponse 'Normal GetNodesRankingResponse)
getNodesRankingHandler ServerContext{..} (ServerNormalRequest _meta _) = defaultExceptionHandle $ do
  nodes <- getRanking >>= mapM (P.getServerNode zkHandle)
  let resp = GetNodesRankingResponse $ V.fromList nodes
  returnResp resp

takeSubscription :: ServerContext
                 -> ServerRequest 'Normal TakeSubscriptionRequest Empty
                 -> IO (ServerResponse 'Normal Empty)
takeSubscription ServerContext{..} (ServerNormalRequest _ (TakeSubscriptionRequest subId))= defaultExceptionHandle $ do
  Log.debug . Log.buildString $ "I took the subscription " <> TL.unpack subId
  err_m <- modifyMVar subscriptionCtx
    (\subctxs -> do
        case Map.lookup (TL.unpack subId) subctxs of
          Nothing -> return (subctxs, Nothing)
          Just subctxMVar -> do
            modifyMVar_ subctxMVar
              (\subctx -> return $ subctx { _subctxNode = serverID })
            return (subctxs, Nothing)
    )
  case err_m of
    Nothing -> do
      P.getObject (TL.toStrict subId) zkHandle >>= \case
        Nothing -> returnErrResp StatusInternal "Tring to recover a deleted subscription"
        Just subctx -> do
          P.storeObject (TL.toStrict subId)
            (subctx { _subctxNode = serverID }) zkHandle
          returnResp Empty
    Just err -> returnErrResp StatusInternal err

takeStream :: ServerContext
           -> ServerRequest 'Normal TakeStreamRequest Empty
           -> IO (ServerResponse 'Normal Empty)
takeStream ServerContext{..} (ServerNormalRequest _ (TakeStreamRequest stream)) = defaultExceptionHandle $ do
  node <- getServerNode zkHandle serverID
  Log.debug . Log.buildString $ "I took the stream " <> TL.unpack stream
  P.storeObject (TL.toStrict stream) (ProducerContext (TL.toStrict stream) node) zkHandle
  returnResp Empty
