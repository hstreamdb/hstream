{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Control.Concurrent               (newMVar, threadDelay)
import           Control.Monad                    (void, when)
import           Data.Aeson                       as Aeson
import qualified Data.Char                        as Char
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import           Data.Maybe                       (isNothing, mapMaybe,
                                                   maybeToList)
import qualified Data.Text.Encoding               as T
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Client    (ClientConfig,
                                                   ClientSSLConfig (..),
                                                   ClientSSLKeyCertPair (..))
import           Network.GRPC.HighLevel.Generated (ClientError (..),
                                                   ClientResult (..),
                                                   GRPCIOError (..),
                                                   StatusDetails (..),
                                                   withGRPCClient)
import qualified Options.Applicative              as O
import           System.Exit                      (exitFailure)
import           System.Timeout                   (timeout)
import           Text.RawString.QQ                (r)

import qualified HStream.Admin.Server.Command     as Admin
import           HStream.Admin.Server.Types       (StreamCommand (..),
                                                   SubscriptionCommand (..))
import           HStream.Client.Action            (createSubscription',
                                                   deleteSubscription,
                                                   dropAction, listStreams,
                                                   listSubscriptions)
import           HStream.Client.Execute           (simpleExecuteWithAddr,
                                                   updateClusterInfo)
import           HStream.Client.SQL               (commandExec,
                                                   interactiveSQLApp)
import           HStream.Client.Types             (CliConnOpts (..),
                                                   Command (..),
                                                   HStreamCommand (..),
                                                   HStreamInitOpts (..),
                                                   HStreamNodes (..),
                                                   HStreamSqlContext (..),
                                                   HStreamSqlOpts (..),
                                                   commandParser)
import           HStream.Client.Utils             (mkClientNormalRequest',
                                                   printResult,
                                                   waitForServerToStart)
import           HStream.Server.HStreamApi        (DescribeClusterResponse (..),
                                                   HStreamApi (..),
                                                   ServerNode (..),
                                                   hstreamApiClient)
import qualified HStream.Server.HStreamApi        as API
import           HStream.SQL                      (DropObject (..))
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (SocketAddr (..),
                                                   fillWithJsonString',
                                                   formatResult,
                                                   mkGRPCClientConfWithSSL,
                                                   pattern EnumPB,
                                                   setupSigsegvHandler)

main :: IO ()
main = runCommand =<<
  O.customExecParser (O.prefs (O.showHelpOnEmpty <> O.showHelpOnError))
                     (O.info (commandParser O.<**> O.helper)
                             (O.fullDesc <> O.header "======= HStream CLI =======")
                     )

data RefinedCliConnOpts = RefinedCliConnOpts {
    addr         :: SocketAddr
  , sslConfig    :: Maybe ClientSSLConfig
  , clientConfig :: ClientConfig
  }

refineCliConnOpts :: CliConnOpts -> IO RefinedCliConnOpts
refineCliConnOpts CliConnOpts {..} = do
  let addr = SocketAddr _serverHost _serverPort
  clientSSLKeyCertPair <- do
    case _tlsKey of
      Nothing -> case _tlsCert of
        Nothing -> pure Nothing
        Just _  -> putStrLn "got `tls-cert`, but `tls-key` is missing" >> exitFailure
      Just tlsKey -> case _tlsCert of
        Nothing      -> putStrLn "got `tls-key`, but `tls-cert` is missing" >> exitFailure
        Just tlsCert -> pure . Just $ ClientSSLKeyCertPair {
          clientPrivateKey = tlsKey
        , clientCert       = tlsCert
        }
  let sslConfig = if isNothing _tlsCa && isNothing clientSSLKeyCertPair
        then Nothing
        else Just $ ClientSSLConfig {
          serverRootCert       = _tlsCa
        , clientSSLKeyCertPair = clientSSLKeyCertPair
        , clientMetadataPlugin = Nothing
        }
  let clientConfig = mkGRPCClientConfWithSSL addr sslConfig
  pure $ RefinedCliConnOpts addr sslConfig clientConfig

runCommand :: HStreamCommand -> IO ()
runCommand HStreamCommand {..} = do
  refinedCliConnOpts <- refineCliConnOpts cliConnOpts
  case cliCommand of
    HStreamSql    opts       -> hstreamSQL   refinedCliConnOpts opts
    HStreamNodes  opts       -> hstreamNodes refinedCliConnOpts opts
    HStreamInit   opts       -> hstreamInit  refinedCliConnOpts opts
    HStreamStream opts       -> hstreamStream refinedCliConnOpts opts
    HStreamSubscription opts -> hstreamSubscription refinedCliConnOpts opts

hstreamSQL :: RefinedCliConnOpts -> HStreamSqlOpts -> IO ()
hstreamSQL RefinedCliConnOpts{..} HStreamSqlOpts{_updateInterval = updateInterval, .. } = do
  availableServers <- newMVar []
  currentServer    <- newMVar addr
  let ctx = HStreamSqlContext {..}
  setupSigsegvHandler
  connected <- waitForServerToStart (_retryTimeout * 1000000) addr sslConfig
  case connected of
    Nothing -> errorWithoutStackTrace "Connection timed out. Please check the server URI and try again."
    Just _  -> pure ()
  void $ updateClusterInfo ctx addr
  case _execute of
    Nothing        -> showHStream *> interactiveSQLApp ctx _historyFile
    Just statement -> do
      when (Char.isSpace `all` statement) $ do putStrLn "Empty statement" *> exitFailure
      commandExec ctx statement
  where
    showHStream = putStrLn [r|
      __  _________________  _________    __  ___
     / / / / ___/_  __/ __ \/ ____/   |  /  |/  /
    / /_/ /\__ \ / / / /_/ / __/ / /| | / /|_/ /
   / __  /___/ // / / _, _/ /___/ ___ |/ /  / /
  /_/ /_//____//_/ /_/ |_/_____/_/  |_/_/  /_/
  |]

hstreamNodes :: RefinedCliConnOpts -> HStreamNodes -> IO ()
hstreamNodes connOpts HStreamNodesList =
  getNodes connOpts >>= putStrLn . formatResult . L.sort . V.toList . API.describeClusterResponseServerNodes
hstreamNodes connOpts (HStreamNodesStatus mid) = do
  nodes <- getNodes connOpts
  let target = case mid of
        Nothing  -> V.toList . API.describeClusterResponseServerNodesStatus $ nodes
        Just sid -> maybeToList . L.find (compareServerId sid) . V.toList . API.describeClusterResponseServerNodesStatus $ nodes
  when (null target) $ errorWithoutStackTrace "Node(s) not found in the cluster"
  putStrLn . formatResult $ target
  where
    compareServerId x API.ServerNodeStatus{..} =
      case serverNodeStatusNode of
        Just API.ServerNode{..} -> serverNodeId == x
        Nothing                 -> False
hstreamNodes connOpts (HStreamNodesCheck nMaybe) = do
  nodes <- describeClusterResponseServerNodesStatus <$> getNodes connOpts
  let n' = length nodes
  case nMaybe of
    Just n -> when (n' < fromIntegral n) $ errorWithoutStackTrace "No enough nodes in the cluster"
    Nothing -> return ()
  let nodesNotRunning = V.filter ((/= EnumPB API.NodeStateRunning) . API.serverNodeStatusState) nodes
  if null nodesNotRunning
    then putStrLn "All nodes in the cluster are running."
    else errorWithoutStackTrace $ "Some Nodes are not running: "
                                <> show (mapMaybe ((API.serverNodeId <$>) . API.serverNodeStatusNode) (V.toList nodesNotRunning))

-- TODO: Init should have it's own rpc call
hstreamInit :: RefinedCliConnOpts -> HStreamInitOpts -> IO ()
hstreamInit RefinedCliConnOpts{..} HStreamInitOpts{..} = do
  ready <- timeout (_timeoutSec * 1000000) $
    withGRPCClient clientConfig $ \client -> do
      api <- hstreamApiClient client
      Admin.sendAdminCommand "server init" api >>= Admin.formatCommandResponse >>= putStrLn
      loop api
  case ready of
    Just s  -> putStrLn s
    Nothing -> putStrLn "Time out waiting for cluster ready" >> exitFailure
  where
    loop api = do
      threadDelay 1000000
      resp <- Admin.sendAdminCommand "server ready" api
      case Aeson.eitherDecodeStrict (T.encodeUtf8 resp) of
        Left errmsg              -> pure $ "Decode json error: " <> errmsg
        Right (Aeson.Object obj) -> do
          let m_type = HM.lookup "type" obj
          case m_type of
            Just (Aeson.String "plain") -> pure $ fillWithJsonString' "content" obj
            _                           -> loop api
        _ -> loop api

hstreamStream :: RefinedCliConnOpts -> StreamCommand -> IO ()
hstreamStream RefinedCliConnOpts{..}  = \case
  StreamCmdList ->
    simpleExecuteWithAddr addr sslConfig listStreams >>= printResult
  StreamCmdCreate stream ->
    simpleExecuteWithAddr addr sslConfig (\HStreamApi{..} -> hstreamApiCreateStream (mkClientNormalRequest' stream)) >>= printResult
  StreamCmdDelete sName ignoreNonExist ->
    simpleExecuteWithAddr addr sslConfig (dropAction ignoreNonExist (DStream sName)) >>= printResult

hstreamSubscription :: RefinedCliConnOpts -> SubscriptionCommand -> IO ()
hstreamSubscription RefinedCliConnOpts{..}  = \case
  SubscriptionCmdList ->
    simpleExecuteWithAddr addr sslConfig listSubscriptions >>= printResult
  SubscriptionCmdCreate subscription ->
    simpleExecuteWithAddr addr sslConfig (createSubscription' subscription) >>= printResult
  SubscriptionCmdDelete sid bool ->
    simpleExecuteWithAddr addr sslConfig (deleteSubscription sid bool) >>= printResult

getNodes :: RefinedCliConnOpts -> IO DescribeClusterResponse
getNodes RefinedCliConnOpts{..} =
  withGRPCClient clientConfig $ \client -> do
    HStreamApi{..} <- hstreamApiClient client
    res <- hstreamApiDescribeCluster (mkClientNormalRequest' Empty)
    case res of
      ClientNormalResponse resp _ _ _ _ -> return resp
      ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode x details))
        -> putStrLn (show x <> " Error: "  <> show (unStatusDetails details))
           >> exitFailure
      ClientErrorResponse err -> error $ "Server Error: " <> show err <> "\n"
