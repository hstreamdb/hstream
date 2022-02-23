{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Gadget where

import           Control.Concurrent
import           Control.Monad
import qualified Data.List                     as L
import qualified Data.Text                     as T
import qualified Data.Text.Encoding            as BS
import qualified Data.Text.IO                  as T
import qualified Data.Vector                   as V
import           Network.GRPC.HighLevel
import           Network.GRPC.HighLevel.Client
import           Proto3.Suite                  (def)
import           Z.IO.Network.SocketAddr       (SocketAddr (..))

import           HStream.Client.Action
import           HStream.Client.Type           (ClientContext (..))
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi     as API
import           HStream.ThirdParty.Protobuf   (Empty (Empty))
import           HStream.Utils

--------------------------------------------------------------------------------

describeCluster :: ClientContext -> SocketAddr -> IO (Maybe API.DescribeClusterResponse)
describeCluster ctx@ClientContext{..} addr = do
  getInfoWithAddr ctx addr getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} =
      hstreamApiDescribeCluster (mkClientNormalRequest Empty)
    handleRespApp :: ClientResult 'Normal API.DescribeClusterResponse -> IO (Maybe API.DescribeClusterResponse)
    handleRespApp
      (ClientNormalResponse resp@(API.DescribeClusterResponse _ _ nodes) _meta1 _meta2 _code _details) = do
      void $ swapMVar availableServers (serverNodeToSocketAddr <$> V.toList nodes)
      unless (V.null nodes) $ do
        void $ swapMVar currentServer (serverNodeToSocketAddr $ V.head nodes)
      return $ Just resp

lookupStream :: ClientContext -> SocketAddr -> T.Text -> IO (Maybe API.ServerNode)
lookupStream ctx addr stream = do
  getInfoWithAddr ctx addr getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} = do
      let req = def { API.lookupStreamRequestStreamName = stream }
      hstreamApiLookupStream (mkClientNormalRequest req)
    handleRespApp = getServerResp >=> return . API.lookupStreamResponseServerNode

lookupSubscription :: ClientContext -> SocketAddr -> T.Text -> IO (Maybe API.ServerNode)
lookupSubscription ctx addr subId = do
  getInfoWithAddr ctx addr getRespApp handleRespApp
  where
    getRespApp API.HStreamApi{..} = do
      let req = API.LookupSubscriptionRequest { lookupSubscriptionRequestSubscriptionId = subId }
      hstreamApiLookupSubscription (mkClientNormalRequest req)
    handleRespApp = getServerResp >=> return . API.lookupSubscriptionResponseServerNode

-- | Try the best to execute an GRPC request until all possible choices failed,
-- with the given address instead of which from ClientContext.
getInfoWithAddr
  :: ClientContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO (Maybe b))
  -> IO (Maybe b)
getInfoWithAddr ctx@ClientContext{..} addr action cont = do
  resp <- runActionWithAddr addr action
  case resp of
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _ details)) -> do
      T.putStrLn $ "Error: " <> BS.decodeUtf8 (unStatusDetails details)
      return Nothing
    ClientErrorResponse err -> do
      putStrLn $ "Error: " <> show err <> " , retry on a different server node"
      modifyMVar_ availableServers (return . L.delete addr)
      curServers <- readMVar availableServers
      case curServers of
        []  -> return Nothing
        x:_ -> getInfoWithAddr ctx x action cont
    _ -> do
      void . swapMVar currentServer $ addr
      cont resp
