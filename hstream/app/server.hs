{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

import           Control.Concurrent               (MVar, forkIO, newEmptyMVar,
                                                   putMVar, readMVar)
import qualified Control.Concurrent.Async         as Async
import           Control.Exception                (SomeException, bracket,
                                                   catch, handle)
import           Control.Monad                    (forM, forM_, join, void,
                                                   when)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Short            as BS
import qualified Data.Map                         as Map
import           Data.Maybe                       (isJust)
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import           Data.Word                        (Word16)
import qualified HsGrpc.Server                    as HsGrpc
import qualified HsGrpc.Server.Types              as HsGrpc
import           Network.HTTP.Client              (defaultManagerSettings,
                                                   newManager)
import           System.Environment               (getArgs)
import           System.IO                        (hPutStrLn, stderr)

import           HStream.Base                     (setupFatalSignalHandler)
import           HStream.Common.Server.HashRing   (updateHashRing)
import           HStream.Common.Server.MetaData   (TaskAllocation (..),
                                                   clusterStartTimeId)
import           HStream.Common.Types             (getHStreamVersion)
import           HStream.Common.ZookeeperClient   (unsafeGetZHandle,
                                                   withZookeeperClient)
import           HStream.Exception
import           HStream.Gossip                   (GossipContext (..),
                                                   defaultGossipOpts,
                                                   initGossipContext,
                                                   startGossip, waitGossipBoot)
import           HStream.Gossip.Types             (InitType (Gossip))
import qualified HStream.Gossip.Types             as Gossip
import qualified HStream.Kafka.Server.Config      as Ka
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          as M (MetaHandle (..),
                                                        MetaStore (..),
                                                        RHandle (..))
import           HStream.RawString                (banner)
import           HStream.Server.Config            (AdvertisedListeners,
                                                   ExperimentalFeature (..),
                                                   FileLoggerSettings (..),
                                                   ListenersSecurityProtocolMap,
                                                   MetaStoreAddr (..),
                                                   SecurityProtocolMap,
                                                   ServerCli (..),
                                                   ServerOpts (..), TlsConfig,
                                                   advertisedListenersToPB,
                                                   getConfig, runServerCli)
import qualified HStream.Server.Core.Cluster      as Cluster
import qualified HStream.Server.Experimental      as Exp
import           HStream.Server.HealthMonitor     (mkHealthMonitor,
                                                   startMonitor)
import qualified HStream.Server.HsGrpcHandler     as HsGrpcHandler
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Server.Initialization    (closeRocksDBHandle,
                                                   initializeServer,
                                                   openRocksDBHandle,
                                                   readTlsPemFile)
import           HStream.Server.MetaData          (initHStreamFileTables,
                                                   initHStreamRqTables,
                                                   initHStreamZkPaths)
import           HStream.Server.QueryWorker       (QueryWorker (QueryWorker))
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Store.Logger             as Log
import qualified HStream.ThirdParty.Protobuf      as Proto
import           HStream.Utils                    (getProtoTimestamp)
import qualified KafkaServer                      as Ka

#ifdef HStreamUseGrpcHaskell
import           HStream.Server.Handler           (handlers)
import           HStream.Server.Initialization    (initializeTlsConfig)
import qualified Network.GRPC.HighLevel           as GRPC
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
#endif

-------------------------------------------------------------------------------

main :: IO ()
main = do
  args <- getArgs
  serverCli <- runServerCli args
  case serverCli of
    ShowVersion      -> showVersion
    Cli cliOpts      -> getConfig cliOpts >>= app
    KafkaCli cliOpts -> Ka.runServerFromCliOpts cliOpts Ka.app

-------------------------------------------------------------------------------
-- HStream Server

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupFatalSignalHandler
  Log.setLogDeviceDbgLevel' _ldLogLevel
  let logType = case config.serverFileLog of
        Nothing -> Log.LogStderr
        Just FileLoggerSettings{..} -> Log.LogFileRotate $
          Log.FileLogSpec logpath logsize lognum
  Log.setDefaultLogger _serverLogLevel _serverLogWithColor
                       logType _serverLogFlushImmediately

  bracket (openRocksDBHandle _querySnapshotPath) closeRocksDBHandle $ \db_m ->
    case _metaStore of
      ZkAddr addr -> do
        withZookeeperClient addr 5000 $ \zkclient -> do
          initHStreamZkPaths zkclient
          action (ZKHandle zkclient) db_m
      RqAddr addr -> do
        m <- newManager defaultManagerSettings
        let rq = RHandle m addr
        initHStreamRqTables rq
        action (RLHandle rq) db_m
      FileAddr addr -> do
        initHStreamFileTables addr
        action (FileHandle addr) db_m
  where
    action h db_m = do
      hstreamVersion <- getHStreamVersion
      let serverNode =
            I.ServerNode{ serverNodeId = _serverID
                        , serverNodePort = fromIntegral _serverPort
                        , serverNodeAdvertisedAddress = encodeUtf8 . T.pack $ _serverAddress
                        , serverNodeGossipPort = fromIntegral _serverInternalPort
                        , serverNodeGossipAddress = encodeUtf8 . T.pack $ _serverGossipAddress
                        , serverNodeAdvertisedListeners = advertisedListenersToPB _serverAdvertisedListeners
                        , serverNodeVersion = Just hstreamVersion
                        }

      scMVar <- newEmptyMVar
      gossipContext <- initGossipContext defaultGossipOpts mempty (Just $ nodeChangeEventHd scMVar) serverNode _seedNodes

      serverContext <- initializeServer config gossipContext h db_m
      putMVar scMVar serverContext

      -- FIXME: safer way to handle this: what if updateHashRing failed?
      void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

#ifdef HStreamUseGrpcHaskell
      grpcOpts <- defGrpcOpts _serverHost _serverPort _tlsConfig
      let mainGrpcOpts = grpcOpts
#else
      when (not . null $ grpcChannelArgs) $
        Log.debug $ "Set grpcChannelArgs: " <> Log.buildString' grpcChannelArgs
      grpcOpts <- defGrpcOpts _serverHost _serverPort _tlsConfig grpcChannelArgs
      -- TODO: auth tokens
      let mainGrpcOpts = grpcOpts{ HsGrpc.serverAuthTokens = serverTokens }
#endif

      -- Experimental features
      let enableStreamV2 = ExperimentalStreamV2 `elem` experimentalFeatures
      Async.withAsync (serve serverContext mainGrpcOpts enableStreamV2) $ \a -> do
        -- start gossip
        a1 <- startGossip _serverHost gossipContext
        Async.link2Only (const True) a a1
        -- start extra listeners
        as <- serveListeners serverContext
                             grpcOpts
                             _securityProtocolMap
                             _serverAdvertisedListeners
                             _listenersSecurityProtocolMap
                             enableStreamV2
        forM_ as (Async.link2Only (const True) a)
        -- wati the default server
        waitGossipBoot gossipContext

        when (_enableServerCache) $ do
          let ServerContext{scLDClient, metaHandle, scStatsHolder} = serverContext
          healthMonitor <- mkHealthMonitor scLDClient scStatsHolder metaHandle 1
          aMonitor <- Async.async $ startMonitor serverContext healthMonitor 3
          Log.info $ "Start healthy monitor"
          Async.link2Only (const True) a aMonitor

        Async.wait a

serve
  :: ServerContext
#ifdef HStreamUseGrpcHaskell
  -> GRPC.ServiceOptions
#else
  -> HsGrpc.ServerOptions
#endif
  -> Bool
  -- ^ Experimental features
  -> IO ()
serve sc@ServerContext{..} rpcOpts enableStreamV2 = do
  Log.i "************************"
  hPutStrLn stderr banner
  Log.i "************************"

  let serverOnStarted = do
        Log.info $ "Server is started on port "
#ifdef HStreamUseGrpcHaskell
                <> Log.build (GRPC.unPort . GRPC.serverPort $ rpcOpts)
#else
                <> Log.build (HsGrpc.serverPort rpcOpts)
#endif
                <> ", waiting for cluster to get ready"
        void $ forkIO $ do
          void (readMVar (clusterReady gossipContext))
          readMVar (clusterInited gossipContext) >>= \case
            Gossip -> return ()
            _ -> do
              getProtoTimestamp >>= \x -> upsertMeta @Proto.Timestamp clusterStartTimeId x metaHandle
              -- FIXME: The following line is very delicate and can cause weird problems.
              --        It was intended to re-allocate tasks after a server restart. However,
              --        this should be done BEFORE any node serves any client request or
              --        internal task. However, the current `serverOnStarted` is not
              --        ensured to be called before serving outside.
              -- TODO:  I do not have 100% confidence this is correct. So it should be
              --        carefully investigated and tested.
              -- handle (\(_ :: RQLiteRowNotFound) -> return ()) $ deleteAllMeta @TaskAllocation metaHandle
          -- recover tasks
          Log.info "recovering local io tasks"
          Cluster.recoverLocalTasks sc scIOWorker
          Log.info "recovering local query tasks"
          Cluster.recoverLocalTasks sc (QueryWorker sc)
          Log.info "recovered tasks"

#ifdef HStreamUseGrpcHaskell
  let rpcOpts' = rpcOpts{ GRPC.serverOnStarted = Just serverOnStarted }
  Log.info $ "Starting"
          <> if isJust (GRPC.sslConfig rpcOpts') then " secure " else " insecure "
          <> "server with grpc-haskell..."
  api <- handlers sc
  API.hstreamApiServer api rpcOpts'
#else
  let rpcOpts' = rpcOpts{ HsGrpc.serverOnStarted = Just serverOnStarted}
  Log.info $ "Starting"
        <> if isJust (HsGrpc.serverSslOptions rpcOpts') then " secure " else " insecure "
        <> "server with hs-grpc-server..."
  if enableStreamV2
     then do Log.info "Enable experimental feature: stream-v2"
             slotConfig <- Exp.doStreamV2Init sc
             HsGrpc.runServer rpcOpts' (Exp.streamV2Handlers sc slotConfig)
     else HsGrpc.runServer rpcOpts' (HsGrpcHandler.handlers sc)
#endif

serveListeners
  :: ServerContext
#ifdef HStreamUseGrpcHaskell
  -> GRPC.ServiceOptions
#else
  -> HsGrpc.ServerOptions
#endif
  -> SecurityProtocolMap
  -> AdvertisedListeners
  -> ListenersSecurityProtocolMap
  -> Bool
  -- ^ Experimental features
  -> IO [Async.Async ()]
serveListeners sc grpcOpts
               securityMap listeners listenerSecurityMap
               enableStreamV2 = do
  let listeners' = [(k, v) | (k, vs) <- Map.toList listeners, v <- Set.toList vs]
  forM listeners' $ \(key, I.Listener{..}) -> Async.async $ do
    let listenerOnStarted = Log.info $ "Extra listener is started on port "
                                    <> Log.build listenerPort
    let sc' = sc{scAdvertisedListenersKey = Just key}
#ifdef HStreamUseGrpcHaskell
    let newSslOpts = initializeTlsConfig <$> join ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap )
    let grpcOpts' = grpcOpts{ GRPC.serverPort = GRPC.Port $ fromIntegral listenerPort
                            , GRPC.serverOnStarted = Just listenerOnStarted
                            , GRPC.sslConfig = newSslOpts
                            }
    api <- handlers sc'
    Log.info $ "Starting"
            <> (if isJust (GRPC.sslConfig grpcOpts') then " secure " else " insecure ")
            <> "advertised listener: "
            <> Log.build key <> ":"
            <> Log.build listenerAddress <> ":"
            <> Log.build listenerPort
    API.hstreamApiServer api grpcOpts'
#else
    newSslOpts <- mapM readTlsPemFile $ join ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap )
    let grpcOpts' = grpcOpts{ HsGrpc.serverPort = fromIntegral listenerPort
                            , HsGrpc.serverOnStarted = Just listenerOnStarted
                            , HsGrpc.serverSslOptions = newSslOpts
                            }
    Log.info $ "Starting"
            <> (if isJust (HsGrpc.serverSslOptions grpcOpts') then " secure " else " insecure ")
            <> "advertised listener: "
            <> Log.build key <> ":"
            <> Log.build listenerAddress <> ":"
            <> Log.build listenerPort
    if enableStreamV2
       then do Log.info "Enable experimental feature: stream-v2"
               slotConfig <- Exp.doStreamV2Init sc'
               HsGrpc.runServer grpcOpts' (Exp.streamV2Handlers sc' slotConfig)
       else HsGrpc.runServer grpcOpts' (HsGrpcHandler.handlers sc')
#endif

-- default grpc options
#ifdef HStreamUseGrpcHaskell
defGrpcOpts
  :: ByteString
  -> Word16
  -> Maybe TlsConfig
  -> IO GRPC.ServiceOptions
defGrpcOpts host port tlsConfig = do
  let sslOpts = initializeTlsConfig <$> tlsConfig
  pure $
    GRPC.defaultServiceOptions
      { GRPC.serverHost = GRPC.Host host
      , GRPC.serverPort = GRPC.Port $ fromIntegral port
      , GRPC.sslConfig = sslOpts
      }
#else
defGrpcOpts
  :: ByteString
  -> Word16
  -> Maybe TlsConfig
  -> [HsGrpc.ChannelArg]
  -> IO HsGrpc.ServerOptions
defGrpcOpts host port tlsConfig chanArgs = do
  sslOpts <- mapM readTlsPemFile $ tlsConfig
  pure $
    HsGrpc.defaultServerOpts
      { HsGrpc.serverHost = BS.toShort host
      , HsGrpc.serverPort = fromIntegral port
      , HsGrpc.serverParallelism = 0
      , HsGrpc.serverSslOptions = sslOpts
      , HsGrpc.serverInternalChannelSize = 64
      , HsGrpc.serverChannelArgs = chanArgs
      }
#endif

-------------------------------------------------------------------------------
-- Misc

showVersion :: IO ()
showVersion = do
  API.HStreamVersion{..} <- getHStreamVersion
  putStrLn $ "version: " <> T.unpack hstreamVersionVersion
          <> " (" <> T.unpack hstreamVersionCommit <> ")"

-- TODO: here, we just catch `SomeException` and then show some error messages.
--
-- 1. There may need to be a uniform way to handle all gossip handles(threads).
-- 2. Retry retriable actions
nodeChangeEventHd
  :: MVar ServerContext
  -> Gossip.ServerState
  -> I.ServerNode
  -> IO ()
nodeChangeEventHd m s n = catch (Cluster.nodeChangeEventHandler m s n) $
  \(e :: SomeException) -> Log.fatal $ "handle node change event error: " <> Log.buildString' e
