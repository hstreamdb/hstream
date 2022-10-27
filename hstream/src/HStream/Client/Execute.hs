{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module HStream.Client.Execute
  ( executeShowPlan
  , execute_
  , execute
  , executeWithAddr
  -- , executeInsert
  , executeWithAddr_
  , executeWithLookupResource_
  , updateClusterInfo
  , lookupWithAddr
  , simpleExecuteWithAddr
  ) where

import           Control.Concurrent
import           Control.Monad
import qualified Data.List                        as L
import qualified Data.Text.Encoding               as BS
import qualified Data.Text.IO                     as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel           (GRPCIOError (..))
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (withGRPCClient)

import           HStream.Client.Action
import           HStream.Client.Types             (HStreamSqlContext (..),
                                                   ResourceType)
import           HStream.Client.Utils
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL
import           HStream.Utils                    (Format, HStreamClientApi,
                                                   SocketAddr, getServerResp,
                                                   mkGRPCClientConfWithSSL,
                                                   serverNodeToSocketAddr)

executeShowPlan :: HStreamSqlContext -> ShowObject -> IO ()
executeShowPlan ctx showObject =
  case showObject of
    SStreams    -> execute_ ctx listStreams
    SViews      -> execute_ ctx listViews
    SQueries    -> execute_ ctx listQueries
    SConnectors -> execute_ ctx listConnectors

execute_ :: Format a => HStreamSqlContext
  -> Action a -> IO ()
execute_ ctx@HStreamSqlContext{..} action = do
  addr <- readMVar currentServer
  void $ executeWithAddr_ ctx addr action printResult

executeWithLookupResource_ :: Format a => HStreamSqlContext
  -> ResourceType -> (HStreamClientApi -> IO a)  -> IO ()
executeWithLookupResource_ ctx@HStreamSqlContext{..} rtype action = do
  addr <- readMVar currentServer
  lookupWithAddr ctx addr rtype >>= \case
     Nothing -> putStrLn "Lookup failed"
     Just sn -> simpleExecuteWithAddr (serverNodeToSocketAddr sn) sslConfig action
            >>= printResult

execute :: HStreamSqlContext -> Action a -> IO (Maybe a)
execute ctx@HStreamSqlContext{..} action = do
  addr <- readMVar currentServer
  executeWithAddr ctx addr action ((Just <$>) . getServerResp)

executeWithAddr
  :: HStreamSqlContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO (Maybe a))
  -> IO (Maybe a)
executeWithAddr ctx addr action handleOKResp = do
  getInfoWithAddr ctx addr action handleOKResp

executeWithAddr_
  :: HStreamSqlContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO ())
  -> IO ()
executeWithAddr_ ctx addr action handleOKResp = do
  void $ getInfoWithAddr ctx addr action (\x -> handleOKResp x >> return Nothing)

updateClusterInfo :: HStreamSqlContext -> SocketAddr -> IO (Maybe API.DescribeClusterResponse)
updateClusterInfo ctx@HStreamSqlContext{..} addr = do
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

lookupWithAddr :: HStreamSqlContext -> SocketAddr -> ResourceType
  -> IO (Maybe API.ServerNode)
lookupWithAddr ctx addr rType = getInfoWithAddr ctx addr (lookupResource rType) getServerResp

--------------------------------------------------------------------------------

-- | Try the best to execute an GRPC request until all possible choices failed,
-- with the given address instead of which from HStreamSqlContext.
getInfoWithAddr
  :: HStreamSqlContext -> SocketAddr
  -> Action a
  -> (ClientResult 'Normal a -> IO (Maybe b))
  -> IO (Maybe b)
getInfoWithAddr ctx@HStreamSqlContext{..} addr action cont = do
  resp <- simpleExecuteWithAddr addr sslConfig action
  case resp of
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _ details)) -> do
      T.putStrLn $ "Error: " <> BS.decodeUtf8 (unStatusDetails details)
      return Nothing
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
