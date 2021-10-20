{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Gadget where

import           Control.Concurrent
import           Control.Monad
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Char8            as BSC
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import qualified Data.Text.Encoding               as TE
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (GRPCIOError (..),
                                                   withGRPCClient)

import           HStream.Client.Utils
import qualified HStream.Logger                   as Log
import qualified HStream.Server.HStreamApi        as API
import           HStream.ThirdParty.Protobuf      (Empty (Empty))

data ClientContext = ClientContext
  { availableServers :: MVar [ByteString]
  , currentServer    :: MVar ByteString
  , producers        :: MVar (Map.Map String ByteString)
  , clientId         :: String
}

data SubscriptionWithClient = SubscriptionWithClient
  { _subscriptionWithClientSubId    :: String
  , _subscriptionWithClientClientId :: String
  }

data ConnectStrategy
  = ByLoad
  | BySubscription SubscriptionWithClient

--------------------------------------------------------------------------------

-- | Try the best to connect to the cluster until all possible choices failed.
connect :: ClientContext -> ByteString -> ConnectStrategy -> IO (Maybe ByteString)
connect ctx@ClientContext{..} uri strategy = withGRPCClient (mkGRPCClientConf uri) $ \client -> do
    API.HStreamApi{..} <- API.hstreamApiClient client
    let req = case strategy of
          ByLoad -> API.ConnectRequest (Just $ API.ConnectRequestRedirectStrategyByLoad Empty)
          BySubscription (SubscriptionWithClient subId cId) ->
            API.ConnectRequest (Just $ API.ConnectRequestRedirectStrategyBySubscription
                                (API.SubReq (TL.pack subId) (TL.pack cId)))
    resp <- hstreamApiConnect $ mkClientNormalRequest req
    case resp of
      ClientNormalResponse (API.ConnectResponse (Just (API.ConnectResponseStatusAccepted (API.ServerList uris)))) _meta1 _meta2 _code _details      -> do
        void $ swapMVar currentServer uri
        void $ swapMVar availableServers (V.toList $ TE.encodeUtf8 . TL.toStrict <$> uris)
        Log.d . Log.buildByteString $ "Connected to " <> uri
          <> ". Available servers: " <> BSC.pack (show uris)
        return (Just uri)
      ClientNormalResponse (API.ConnectResponse (Just (API.ConnectResponseStatusRedirected (API.Redirected new_uri)))) _meta1 _meta2 _code _details -> do
        Log.d . Log.buildString $ "Redirected to " <> show new_uri
        connect ctx (TE.encodeUtf8 . TL.toStrict $ new_uri) strategy
      ClientErrorResponse err        -> do
        Log.w . Log.buildString $
          "Error when connecting to " <> BSC.unpack uri <> ": " <> show err
        modifyMVar_ availableServers (return . L.delete uri)
        uris <- readMVar availableServers
        Log.w . Log.buildString $ "Now trying: " <> show uris
        go uris
      _                              -> do
        Log.e "Unknown error"
        return Nothing
  where
    go :: [ByteString] -> IO (Maybe ByteString)
    go [] = return Nothing
    go (x:xs) = do
      m_uri <- connect ctx x strategy
      case m_uri of
        Just uri_ -> return (Just uri_)
        Nothing   -> go xs

-- | Try the best to execute an GRPC request until all possible choices failed.
doAction :: ClientContext
         -> ConnectStrategy
         -> (Client -> IO (ClientResult typ a))
         -> (ClientResult typ a -> IO ())
         -> IO ()
doAction ctx@ClientContext{..} strategy getRespApp handleRespApp = do
  uri <- readMVar currentServer
  doActionWithUri ctx uri strategy getRespApp handleRespApp

-- | Try the best to execute an GRPC request until all possible choices failed,
-- with the given uri instead of which from ClientContext.
doActionWithUri :: ClientContext
                -> ByteString
                -> ConnectStrategy
                -> (Client -> IO (ClientResult typ a))
                -> (ClientResult typ a -> IO ())
                -> IO ()
doActionWithUri ctx uri strategy getRespApp handleRespApp = do
  m_realUri <- connect ctx uri strategy
  case m_realUri of
    Nothing      -> Log.w . Log.buildString $ "Error when executing an action."
    Just realUri -> withGRPCClient (mkGRPCClientConf realUri) $ \client -> do
      resp <- getRespApp client
      case resp of
        ClientErrorResponse (ClientIOError GRPCIOTimeout) -> do
          Log.w . Log.buildString $ "Error when executing an action." <> show realUri
        _ -> handleRespApp resp
