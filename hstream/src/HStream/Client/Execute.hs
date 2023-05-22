{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Client.Execute
  ( executeShowPlan
  , execute_
  , execute
  , executeWithAddr
  , executeWithAddr_
  , executeWithLookupResource_
  , executeWithLookupResource
  , updateClusterInfo
  , lookupWithAddr
  , simpleExecuteWithAddr
  , initCliContext
  , simpleExecute
  ) where

import           Control.Concurrent
import           Control.Monad
import qualified Data.List                        as L
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as BS
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel           (GRPCIOError (..))
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (withGRPCClient)

import           HStream.Base                     (setupFatalSignalHandler)
import           HStream.Client.Action
import           HStream.Client.Types             (HStreamCliContext (..),
                                                   RefinedCliConnOpts (..),
                                                   Resource)
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL
import           HStream.Utils                    (Format, HStreamClientApi,
                                                   SocketAddr (..),
                                                   getServerResp,
                                                   mkGRPCClientConfWithSSL,
                                                   serverNodeToSocketAddr)

executeShowPlan :: HStreamCliContext -> ShowObject -> IO ()
executeShowPlan ctx showObject =
  case showObject of
    SStreams    -> execute_ ctx listStreams
    SViews      -> execute_ ctx listViews
    SQueries    -> execute_ ctx listQueries
    SConnectors -> execute_ ctx listConnectors

execute_ :: Format a => HStreamCliContext
  -> Action a -> IO ()
execute_ ctx@HStreamCliContext{..} action = do
  addr <- readMVar currentServer
  void $ executeWithAddr_ ctx addr action printResult

executeWithLookupResource_ :: Format a => HStreamCliContext
  -> Resource -> (HStreamClientApi -> IO a)  -> IO ()
executeWithLookupResource_ ctx rtype action =
  executeWithLookupResource ctx rtype action >>= printResult

executeWithLookupResource :: Format a => HStreamCliContext
  -> Resource -> (HStreamClientApi -> IO a)  -> IO a
executeWithLookupResource ctx@HStreamCliContext{..} rtype action = do
  addr <- readMVar currentServer
  lookupWithAddr ctx addr rtype
  >>= \sn -> simpleExecuteWithAddr (serverNodeToSocketAddr sn) sslConfig action

execute :: HStreamCliContext -> Action a -> IO (Maybe a)
execute ctx@HStreamCliContext{..} action = do
  addr <- readMVar currentServer
  executeWithAddr ctx addr action ((Just <$>) . getServerResp)

executeWithAddr
  :: HStreamCliContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO (Maybe a))
  -> IO (Maybe a)
executeWithAddr ctx addr action handleOKResp = do
  getInfoWithAddr ctx addr action handleOKResp

executeWithAddr_
  :: HStreamCliContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO ())
  -> IO ()
executeWithAddr_ ctx addr action handleOKResp = do
  void $ getInfoWithAddr ctx addr action (\x -> handleOKResp x >> return Nothing)

updateClusterInfo :: HStreamCliContext -> SocketAddr -> IO (Maybe API.DescribeClusterResponse)
updateClusterInfo ctx@HStreamCliContext{..} addr = do
  getInfoWithAddr ctx addr describeCluster handleRespApp
  where
    handleRespApp :: ClientResult 'Normal API.DescribeClusterResponse -> IO (Maybe API.DescribeClusterResponse)
    handleRespApp
      (ClientNormalResponse resp@API.DescribeClusterResponse{ describeClusterResponseServerNodes = nodes } _meta1 _meta2 _code _details) = do
      void $ swapMVar availableServers (serverNodeToSocketAddr <$> V.toList nodes)
      unless (V.null nodes) $ do
        void $ swapMVar currentServer (serverNodeToSocketAddr $ V.head nodes)
      return $ Just resp
    handleRespApp _ = return Nothing

lookupWithAddr :: HStreamCliContext -> SocketAddr -> Resource
  -> IO API.ServerNode
lookupWithAddr ctx addr rType = getInfoWithAddr ctx addr (lookupResource rType) getServerResp

--------------------------------------------------------------------------------

-- | Try the best to execute an GRPC request until all possible choices failed,
-- with the given address instead of which from HStreamCliContext.
getInfoWithAddr
  :: HStreamCliContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO b)
  -> IO b
getInfoWithAddr ctx@HStreamCliContext{..} addr action cont = do
  resp <- simpleExecuteWithAddr addr sslConfig action
  case resp of
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _ details)) -> do
      errorWithoutStackTrace $ "Error: " <> T.unpack (BS.decodeUtf8 (unStatusDetails details))
    ClientErrorResponse err -> do
      putStrLn $ "Error: " <> (case err of ClientIOError ge -> show ge; _ -> show err )<> " , retrying on a different server node"
      modifyMVar_ availableServers (return . L.delete addr)
      curServers <- readMVar availableServers
      case curServers of
        []  -> errorWithoutStackTrace "No more server nodes available!"
        x:_ -> getInfoWithAddr ctx x action cont
    _ -> do
      void . swapMVar currentServer $ addr
      cont resp

simpleExecuteWithAddr :: SocketAddr -> Maybe ClientSSLConfig -> (HStreamClientApi -> IO a) -> IO a
simpleExecuteWithAddr addr sslConfig action =
  withGRPCClient (mkGRPCClientConfWithSSL addr sslConfig) (API.hstreamApiClient >=> action)

simpleExecute :: ClientConfig -> (HStreamClientApi -> IO a) -> IO a
simpleExecute conf action = withGRPCClient conf (API.hstreamApiClient >=> action)

initCliContext :: RefinedCliConnOpts -> IO HStreamCliContext
initCliContext RefinedCliConnOpts{..} = do
  availableServers <- newMVar []
  currentServer    <- newMVar addr
  let sslConfig = clientSSLConfig clientConfig
  let ctx = HStreamCliContext {..}
  setupFatalSignalHandler
  connected <- waitForServerToStart (retryTimeout * 1000000) addr sslConfig
  case connected of
    Nothing -> errorWithoutStackTrace "Connection timed out. Please check the server URI and try again."
    Just _  -> pure ()
  void $ updateClusterInfo ctx addr
  return ctx
