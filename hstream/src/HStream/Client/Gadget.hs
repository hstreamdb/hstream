{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Gadget where

import           Control.Concurrent
import           Control.Monad
import qualified Data.ByteString.Char8            as BSC
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (GRPCIOError (..),
                                                   withGRPCClient)

import           HStream.Client.Utils
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (ServerNode (serverNodeHost))
import qualified HStream.Server.HStreamApi        as API
import           HStream.ThirdParty.Protobuf      (Empty (Empty))

data ClientContext = ClientContext
  { availableServers :: MVar [API.ServerNode]
  , currentServer    :: MVar API.ServerNode
  , producers        :: MVar (Map.Map T.Text API.ServerNode)
  , clientId         :: String
}

--------------------------------------------------------------------------------

describeCluster :: ClientContext -> API.ServerNode -> IO (Maybe API.DescribeClusterResponse)
describeCluster ctx@ClientContext{..} node = do
  doActionWithNode ctx node getRespApp handleRespApp
  where
    getRespApp client = do
      API.HStreamApi{..} <- API.hstreamApiClient client
      hstreamApiDescribeCluster (mkClientNormalRequest Empty)
    handleRespApp :: ClientResult 'Normal API.DescribeClusterResponse -> IO (Maybe API.DescribeClusterResponse)
    handleRespApp
      (ClientNormalResponse resp@(API.DescribeClusterResponse _ _ nodes) _meta1 _meta2 _code _details) = do
      void $ swapMVar availableServers (V.toList nodes)
      return $ Just resp

lookupStream :: ClientContext -> API.ServerNode -> T.Text -> IO (Maybe API.ServerNode)
lookupStream ctx@ClientContext{..} node stream = do
  doActionWithNode ctx node getRespApp handleRespApp
  where
    getRespApp client = do
      API.HStreamApi{..} <- API.hstreamApiClient client
      let req = API.LookupStreamRequest { lookupStreamRequestStreamName = TL.fromStrict stream }
      hstreamApiLookupStream (mkClientNormalRequest req)
    handleRespApp :: ClientResult 'Normal API.LookupStreamResponse -> IO (Maybe API.ServerNode)
    handleRespApp
      (ClientNormalResponse (API.LookupStreamResponse _ Nothing) _meta1 _meta2 _code _details) = return Nothing
    handleRespApp
      (ClientNormalResponse (API.LookupStreamResponse _ (Just serverNode)) _meta1 _meta2 _code _details) = do
      modifyMVar_ producers (return . Map.insert stream serverNode)
      return $ Just serverNode

lookupSubscription :: ClientContext -> API.ServerNode -> T.Text -> IO (Maybe API.ServerNode)
lookupSubscription ctx node subId = do
  doActionWithNode ctx node getRespApp handleRespApp
  where
    getRespApp client = do
      API.HStreamApi{..} <- API.hstreamApiClient client
      let req = API.LookupSubscriptionRequest { lookupSubscriptionRequestSubscriptionId = TL.fromStrict subId }
      hstreamApiLookupSubscription (mkClientNormalRequest req)
    handleRespApp :: ClientResult 'Normal API.LookupSubscriptionResponse -> IO (Maybe API.ServerNode)
    handleRespApp (ClientNormalResponse (API.LookupSubscriptionResponse _ Nothing) _meta1 _meta2 _code _details) = return Nothing
    handleRespApp (ClientNormalResponse (API.LookupSubscriptionResponse _ (Just serverNode)) _meta1 _meta2 _code _details) = return $ Just serverNode

-- | Try the best to execute an GRPC request until all possible choices failed,
-- with the given node instead of which from ClientContext.
doActionWithNode :: ClientContext
                 -> API.ServerNode
                 -> (Client -> IO (ClientResult typ a))
                 -> (ClientResult typ a -> IO (Maybe b))
                 -> IO (Maybe b)
doActionWithNode ctx@ClientContext{..} node@API.ServerNode{..} getRespApp handleRespApp = do
  withGRPCClient (mkGRPCClientConf $ BSC.pack uri) $ \client -> do
    resp <- getRespApp client
    case resp of
      ClientErrorResponse (ClientIOError GRPCIOTimeout) -> do
        Log.w . Log.buildString $ "Failed to connect to Node " <> show uri <> ", redirecting..."
        modifyMVar_ availableServers (return . L.delete node)
        curNodes <- readMVar availableServers
        case L.null curNodes of
          True  -> do
            Log.w . Log.buildString $ "Error when executing an action."
            return Nothing
          False -> do
            let newNode = head curNodes
            doActionWithNode ctx newNode getRespApp handleRespApp
      ClientErrorResponse _ -> do
        Log.w . Log.buildString $ "Error when executing an action."
        return Nothing
      _ -> handleRespApp resp
  where uri = TL.unpack serverNodeHost <> ":" <> show serverNodePort
