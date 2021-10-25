{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Cluster
  ( connectHandler
  ) where

import           Control.Concurrent
import           Control.Exception
import qualified Data.Map.Strict                  as Map
import qualified Data.Set                         as Set
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           HStream.Server.HStreamApi
import           HStream.Server.LoadBalance       (getRanking)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import           HStream.Utils
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Exception

--------------------------------------------------------------------------------

connectHandler :: ServerContext
               -> ServerRequest 'Normal ConnectRequest ConnectResponse
               -> IO (ServerResponse 'Normal ConnectResponse)
connectHandler ServerContext{..} (ServerNormalRequest _meta (ConnectRequest (Just strategy))) = do
  allNames <- getRanking
  allUris <- mapM (P.getServerUri zkHandle) allNames
  case strategy of
    ConnectRequestRedirectStrategyByLoad _ -> case allUris of
      [] -> returnErrResp StatusInternal "No available server found"
      _  -> if serverName == head allNames then do
          let serverList = ServerList (V.fromList allUris)
          returnResp $ ConnectResponse (Just $ ConnectResponseStatusAccepted serverList)
            else do
          let redirected = Redirected (head allUris)
          returnResp $ ConnectResponse (Just $ ConnectResponseStatusRedirected redirected)
    ConnectRequestRedirectStrategyBySubscription (SubReq subId clientId) -> do
      -- fetch zk because local subscriptions may not be the latest
      -- (e.g. some subscriptions were transferred)
      subs <- P.listObjects zkHandle
      case Map.lookup (TL.toStrict subId) subs of
        Nothing -> returnErrResp StatusInternal "No subscription found"
        Just sub@SubscriptionContext{..} -> do
          if CB.unpack serverName == _subctxNode -- this node is the proper one
            then do
            let sub' = sub{ _subctxClients = Set.insert (TL.unpack clientId) _subctxClients }
            modifyMVar_ subscriptionCtx (return . Map.insert (TL.unpack subId) sub')
            P.storeObject (TL.toStrict subId) sub' zkHandle -- sync to zk
            let serverList = ServerList (V.fromList allUris)
            returnResp $ ConnectResponse (Just $ ConnectResponseStatusAccepted serverList)
            else do -- redirect to the subscription node
            try (P.getServerUri zkHandle (CB.pack _subctxNode)) >>= \case
              Left (_ :: ZooException) ->
                returnErrResp StatusInternal "The server that holds the subscription is unavailable"
              Right real_uri -> do
                let redirected = Redirected real_uri
                returnResp $ ConnectResponse (Just $ ConnectResponseStatusRedirected redirected)
connectHandler _ctx (ServerNormalRequest _meta (ConnectRequest Nothing)) = do
  returnErrResp StatusInternal "Incorrect redirection strategy received"
