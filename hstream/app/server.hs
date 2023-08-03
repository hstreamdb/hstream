{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

import           Control.Concurrent               (forkIO, newEmptyMVar,
                                                   putMVar, readMVar)
import qualified Control.Concurrent.Async         as Async
import           Control.Concurrent.STM           (TVar, atomically, retry,
                                                   writeTVar)
import           Control.Exception                (bracket, handle)
import           Control.Monad                    (forM_, join, void, when)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Short            as BS
import qualified Data.Map                         as Map
import           Data.Maybe                       (isJust)
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import           Data.Word                        (Word16)
import qualified HsGrpc.Server                    as HsGrpc
import           Network.HTTP.Client              (defaultManagerSettings,
                                                   newManager)
import           System.IO                        (hPutStrLn, stderr)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)

import           HStream.Base                     (setupFatalSignalHandler)
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import           HStream.Common.Types             (getHStreamVersion)
import           HStream.Exception
import           HStream.Gossip                   (GossipContext (..),
                                                   defaultGossipOpts,
                                                   initGossipContext,
                                                   startGossip, waitGossipBoot)
import           HStream.Gossip.Types             (Epoch, InitType (Gossip))
import           HStream.Gossip.Utils             (getMemberListWithEpochSTM)
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          as M (MetaHandle (..),
                                                        MetaStore (..),
                                                        RHandle (..))
import           HStream.Server.Config            (AdvertisedListeners,
                                                   ExperimentalFeature (..),
                                                   ListenersSecurityProtocolMap,
                                                   MetaStoreAddr (..),
                                                   SecurityProtocolMap,
                                                   ServerCli (..),
                                                   ServerOpts (..), TlsConfig,
                                                   advertisedListenersToPB,
                                                   getConfig, runServerCli)
import qualified HStream.Server.Core.Cluster      as Cluster
import qualified HStream.Server.Experimental      as Exp
import qualified HStream.Server.HsGrpcHandler     as HsGrpcHandler
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Server.Initialization    (closeRocksDBHandle,
                                                   initializeServer,
                                                   openRocksDBHandle,
                                                   readTlsPemFile)
import           HStream.Server.MetaData          (TaskAllocation (..),
                                                   clusterStartTimeId,
                                                   initializeAncestors,
                                                   initializeFile,
                                                   initializeTables)
import           HStream.Server.QueryWorker       (QueryWorker (QueryWorker))
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Store.Logger             as Log
import qualified HStream.ThirdParty.Protobuf      as Proto
import           HStream.Utils                    (getProtoTimestamp)
import           System.Environment               (getArgs)

#ifdef HStreamUseGrpcHaskell
import           HStream.Server.Handler           (handlers)
import           HStream.Server.Initialization    (initializeTlsConfig)
import qualified Network.GRPC.HighLevel           as GRPC
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
#endif

#ifndef HSTREAM_ENABLE_ASAN
import           Text.RawString.QQ                (r)
#endif

-------------------------------------------------------------------------------

main :: IO ()
main = do
  args <- getArgs
  serverCli <- runServerCli args
  case serverCli of
    ShowVersion -> showVersion
    Cli cliOpts -> getConfig cliOpts >>= app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupFatalSignalHandler
  Log.setDefaultLogger _serverLogLevel _serverLogWithColor Log.LogStderr
  Log.setLogDeviceDbgLevel' _ldLogLevel

  bracket (openRocksDBHandle _querySnapshotPath) closeRocksDBHandle $ \db_m ->
   case _metaStore of
     ZkAddr addr -> do
       let zkRes = zookeeperResInit addr Nothing 5000 Nothing 0
       withResource zkRes $ \zk -> initializeAncestors zk >> action (ZkHandle zk) db_m
     RqAddr addr -> do
       m <- newManager defaultManagerSettings
       let rq = RHandle m addr
       initializeTables rq
       action (RLHandle rq) db_m
     FileAddr addr -> do
       initializeFile addr
       action (FileHandle addr) db_m
  where
    action h db_m = do
      let serverNode =
            I.ServerNode { serverNodeId = _serverID
                         , serverNodePort = fromIntegral _serverPort
                         , serverNodeAdvertisedAddress = encodeUtf8 . T.pack $ _serverAddress
                         , serverNodeGossipPort = fromIntegral _serverInternalPort
                         , serverNodeGossipAddress = encodeUtf8 . T.pack $ _serverGossipAddress
                         , serverNodeAdvertisedListeners = advertisedListenersToPB _serverAdvertisedListeners
                         }

      scMVar <- newEmptyMVar
      gossipContext <- initGossipContext defaultGossipOpts mempty (Just $ Cluster.nodeChangeEventHandler scMVar) serverNode _seedNodes

      serverContext <- initializeServer config gossipContext h db_m
      putMVar scMVar serverContext

      void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

      Async.withAsync
        (serve _serverHost _serverPort
               serverContext
               _tlsConfig
               _securityProtocolMap
               _serverAdvertisedListeners
               _listenersSecurityProtocolMap
               (ExperimentalStreamV2 `elem` experimentalFeatures)
        ) $ \a -> do
          a1 <- startGossip _serverHost gossipContext
          Async.link2Only (const True) a a1
          waitGossipBoot gossipContext
          Async.wait a

serve :: ByteString
      -> Word16
      -> ServerContext
      -> Maybe TlsConfig
      -- ^ tls config for default port
      -> SecurityProtocolMap
      -> AdvertisedListeners
      -> ListenersSecurityProtocolMap
      -> Bool
      -- ^ Experimental features
      -> IO ()
serve host port
      sc@ServerContext{..} tlsConfig
      securityMap listeners listenerSecurityMap
      enableExpStreamV2 = do
  Log.i "************************"
#ifndef HSTREAM_ENABLE_ASAN
  hPutStrLn stderr $ [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
#else
  hPutStrLn stderr "ONLY FOR DEBUG: Enable ASAN"
#endif
  Log.i "************************"

  let serverOnStarted = do
        Log.info $ "Server is started on port " <> Log.build port <> ", waiting for cluster to get ready"
        void $ forkIO $ do
          void (readMVar (clusterReady gossipContext)) >> Log.info "Cluster is ready!"
          readMVar (clusterInited gossipContext) >>= \case
            Gossip -> return ()
            _ -> do
              getProtoTimestamp >>= \x -> upsertMeta @Proto.Timestamp clusterStartTimeId x metaHandle
              handle (\(_ :: RQLiteRowNotFound) -> return ()) $ deleteAllMeta @TaskAllocation metaHandle
          -- recover tasks
          Log.info "recovering local io tasks"
          Cluster.recoverLocalTasks sc scIOWorker
          Log.info "recovering local query tasks"
          Cluster.recoverLocalTasks sc (QueryWorker sc)
          Log.info "recovered tasks"

#ifdef HStreamUseGrpcHaskell
  let sslOpts = initializeTlsConfig <$> tlsConfig
#else
  sslOpts <- mapM readTlsPemFile $ tlsConfig
#endif

  let grpcOpts =
#ifdef HStreamUseGrpcHaskell
        GRPC.defaultServiceOptions
        { GRPC.serverHost = GRPC.Host host
        , GRPC.serverPort = GRPC.Port $ fromIntegral port
        , GRPC.serverOnStarted = Just serverOnStarted
        , GRPC.sslConfig = sslOpts
        }
#else
        HsGrpc.defaultServerOpts
          { HsGrpc.serverHost = BS.toShort host
          , HsGrpc.serverPort = fromIntegral port
          , HsGrpc.serverParallelism = 0
          , HsGrpc.serverSslOptions = sslOpts
          , HsGrpc.serverOnStarted = Just serverOnStarted
          , HsGrpc.serverInternalChannelSize = 64
          }
#endif
  forM_ (Map.toList listeners) $ \(key, vs) ->
    forM_ vs $ \I.Listener{..} -> do
      forkIO $ do
        let listenerOnStarted = Log.info $ "Extra listener is started on port "
                                        <> Log.build listenerPort
        let sc' = sc{scAdvertisedListenersKey = Just key}
#ifdef HStreamUseGrpcHaskell
        let newSslOpts = initializeTlsConfig <$> join ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap )
        let grpcOpts' = grpcOpts { GRPC.serverPort = GRPC.Port $ fromIntegral listenerPort
                                 , GRPC.serverOnStarted = Just listenerOnStarted
                                 , GRPC.sslConfig = newSslOpts
                                 }
        api <- handlers sc'
        Log.info $ "Starting"
                <> (if isJust (GRPC.sslConfig grpcOpts') then " secure " else " ")
                <> "advertised listener: "
                <> Log.build key <> ":"
                <> Log.build listenerAddress <> ":"
                <> Log.build listenerPort
        API.hstreamApiServer api grpcOpts'
#else
        newSslOpts <- mapM readTlsPemFile $ join ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap )
        let grpcOpts' = grpcOpts { HsGrpc.serverPort = fromIntegral listenerPort
                                 , HsGrpc.serverOnStarted = Just listenerOnStarted
                                 , HsGrpc.serverSslOptions = newSslOpts
                                 }
        Log.info $ "Starting"
                <> (if isJust (HsGrpc.serverSslOptions grpcOpts') then " secure " else " ")
                <> "advertised listener: "
                <> Log.build key <> ":"
                <> Log.build listenerAddress <> ":"
                <> Log.build listenerPort
        if enableExpStreamV2
           then do Log.info "Enable experimental feature: stream-v2"
                   slotConfig <- Exp.doStreamV2Init sc'
                   HsGrpc.runServer grpcOpts' (Exp.streamV2Handlers sc' slotConfig)
           else HsGrpc.runServer grpcOpts' (HsGrpcHandler.handlers sc')
#endif

#ifdef HStreamUseGrpcHaskell
  Log.info $ "Starting"
          <> if isJust (GRPC.sslConfig grpcOpts) then " secure " else " "
          <> "server with grpc-haskell..."
  api <- handlers sc
  API.hstreamApiServer api grpcOpts
#else
  Log.info $ "Starting"
        <> if isJust (HsGrpc.serverSslOptions grpcOpts) then " secure " else " "
        <> "server with hs-grpc-server..."
  if enableExpStreamV2
     then do Log.info "Enable experimental feature: stream-v2"
             slotConfig <- Exp.doStreamV2Init sc
             HsGrpc.runServer grpcOpts (Exp.streamV2Handlers sc slotConfig)
     else HsGrpc.runServer grpcOpts (HsGrpcHandler.handlers sc)
#endif

-------------------------------------------------------------------------------

showVersion :: IO ()
showVersion = do
  API.HStreamVersion{..} <- getHStreamVersion
  putStrLn $ "version: " <> T.unpack hstreamVersionVersion
          <> " (" <> T.unpack hstreamVersionCommit <> ")"

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
updateHashRing :: GossipContext -> TVar (Epoch, HashRing) -> IO ()
updateHashRing gc hashRing = loop (0,[])
  where
    loop (epoch, list)=
      loop =<< atomically
        ( do (epoch', list') <- getMemberListWithEpochSTM gc
             when (epoch == epoch' && list == list') retry
             writeTVar hashRing (epoch', constructServerMap list')
             return (epoch', list')
        )
