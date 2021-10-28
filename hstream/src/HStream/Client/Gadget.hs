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
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (GRPCIOError (..),
                                                   withGRPCClient)
import           Z.IO.Network.SocketAddr          (SocketAddr (..))

import           HStream.Client.Utils
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (ServerNode (serverNodeHost))
import qualified HStream.Server.HStreamApi        as API
import           HStream.ThirdParty.Protobuf      (Empty (Empty))

data ClientContext = ClientContext
  { availableServers               :: MVar [SocketAddr]
  , currentServer                  :: MVar SocketAddr
  , producers                      :: MVar (Map.Map T.Text API.ServerNode)
  , clientId                       :: String
  , availableServersUpdateInterval :: Int
  }

--------------------------------------------------------------------------------

describeCluster :: ClientContext -> SocketAddr -> IO (Maybe API.DescribeClusterResponse)
describeCluster ctx@ClientContext{..} addr = do
  doActionWithAddr ctx addr getRespApp handleRespApp
  where
    getRespApp client = do
      API.HStreamApi{..} <- API.hstreamApiClient client
      hstreamApiDescribeCluster (mkClientNormalRequest Empty)
    handleRespApp :: ClientResult 'Normal API.DescribeClusterResponse -> IO (Maybe API.DescribeClusterResponse)
    handleRespApp
      (ClientNormalResponse resp@(API.DescribeClusterResponse _ _ nodes) _meta1 _meta2 _code _details) = do
      void $ swapMVar availableServers (serverNodeToSocketAddr <$> V.toList nodes)
      unless (V.null nodes) $ do
        void $ swapMVar currentServer (serverNodeToSocketAddr $ V.head nodes)
      return $ Just resp

lookupStream :: ClientContext -> SocketAddr -> T.Text -> IO (Maybe API.ServerNode)
lookupStream ctx@ClientContext{..} addr stream = do
  doActionWithAddr ctx addr getRespApp handleRespApp
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

lookupSubscription :: ClientContext -> SocketAddr -> T.Text -> IO (Maybe API.ServerNode)
lookupSubscription ctx addr subId = do
  doActionWithAddr ctx addr getRespApp handleRespApp
  where
    getRespApp client = do
      API.HStreamApi{..} <- API.hstreamApiClient client
      let req = API.LookupSubscriptionRequest { lookupSubscriptionRequestSubscriptionId = TL.fromStrict subId }
      hstreamApiLookupSubscription (mkClientNormalRequest req)
    handleRespApp :: ClientResult 'Normal API.LookupSubscriptionResponse -> IO (Maybe API.ServerNode)
    handleRespApp (ClientNormalResponse (API.LookupSubscriptionResponse _ Nothing) _meta1 _meta2 _code _details) = return Nothing
    handleRespApp (ClientNormalResponse (API.LookupSubscriptionResponse _ (Just serverNode)) _meta1 _meta2 _code _details) = return $ Just serverNode

-- | Try the best to execute an GRPC request until all possible choices failed,
-- with the given address instead of which from ClientContext.
doActionWithAddr :: ClientContext
                 -> SocketAddr
                 -> (Client -> IO (ClientResult typ a))
                 -> (ClientResult typ a -> IO (Maybe b))
                 -> IO (Maybe b)
doActionWithAddr ctx@ClientContext{..} addr getRespApp handleRespApp = do
  withGRPCClient (mkGRPCClientConf addr) $ \client -> do
    resp <- getRespApp client
    case resp of
      ClientErrorResponse err -> do
        Log.w . Log.buildString $
          "Failed to connect to Node " <> show uri <> ": " <> show err <> ", redirecting..."
        modifyMVar_ availableServers (return . L.delete addr)
        curServers <- readMVar availableServers
        case L.null curServers of
          True  -> do
            Log.w . Log.buildString $ "Error when executing an action."
            return Nothing
          False -> do
            Log.w . Log.buildString $ "Available servers: " <> show curServers
            let newAddr = head curServers
            doActionWithAddr ctx newAddr getRespApp handleRespApp
      _ -> handleRespApp resp
  where uri = show addr

doAction :: ClientContext
         -> (Client -> IO (ClientResult typ a))
         -> (ClientResult typ a -> IO (Maybe b))
         -> IO (Maybe b)
doAction ctx getRespApp handleRespApp = do
  curServer <- readMVar (currentServer ctx)
  doActionWithAddr ctx curServer getRespApp handleRespApp
