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

import           Control.Concurrent               (MVar, forkIO, newMVar,
                                                   readMVar, swapMVar)
import qualified Control.Concurrent.Async         as Async
import           Control.Concurrent.STM           (TVar, atomically, retry,
                                                   writeTVar)
import           Control.Exception                (handle)
import           Control.Monad                    (forM_, void, when)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Short            as BS
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import           Data.Word                        (Word16)
import qualified Network.GRPC.HighLevel           as GRPC
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           Network.HTTP.Client              (defaultManagerSettings,
                                                   newManager)
import           Text.RawString.QQ                (r)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)
import           ZooKeeper.Types                  (ZHandle, ZooEvent, ZooState,
                                                   pattern ZooConnectedState,
                                                   pattern ZooConnectingState,
                                                   pattern ZooSessionEvent)

import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import           HStream.Exception
import           HStream.Gossip                   (GossipContext (..),
                                                   defaultGossipOpts,
                                                   getMemberListSTM,
                                                   initGossipContext,
                                                   startGossip, waitGossipBoot)
import           HStream.Gossip.Types             (InitType (Gossip))
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          (MetaHandle (..),
                                                   MetaStore (..), RHandle (..))
import           HStream.Server.Config            (AdvertisedListeners,
                                                   MetaStoreAddr (..),
                                                   ServerOpts (..), TlsConfig,
                                                   advertisedListenersToPB,
                                                   getConfig)
import           HStream.Server.Handler           (handlers)
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   hstreamApiServer)
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Server.Initialization    (initializeServer,
                                                   initializeTlsConfig,
                                                   readTlsPemFile)
import           HStream.Server.MetaData          (TaskAllocation,
                                                   clusterStartTimeId,
                                                   initializeAncestors,
                                                   initializeTables)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerState)
import qualified HStream.Store.Logger             as Log
import qualified HStream.ThirdParty.Protobuf      as Proto
import           HStream.Utils                    (getProtoTimestamp,
                                                   pattern EnumPB,
                                                   setupSigsegvHandler)

#ifdef HStreamUseHsGrpc
import qualified HsGrpc.Server                    as HsGrpc
import qualified HStream.Server.HsGrpcHandler     as HsGrpc
#endif

main :: IO ()
main = getConfig >>= app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
  Log.setLogLevel _serverLogLevel _serverLogWithColor
  Log.setLogDeviceDbgLevel' _ldLogLevel

  -- FIXME: do we need this serverState?
  serverState <- newMVar (EnumPB NodeStateStarting)
  case _metaStore of
    ZkAddr addr -> do
      let zkRes = zookeeperResInit addr (Just $ globalWatcherFn serverState) 5000 Nothing 0
      withResource zkRes $ \zk ->
        initializeAncestors zk >> action serverState (ZkHandle zk)
    RqAddr addr -> do
      m <- newManager defaultManagerSettings
      let rq = RHandle m addr
      initializeTables rq
      action serverState $ RLHandle rq
  where
    action serverState h = do
      let serverNode =
            I.ServerNode { serverNodeId = _serverID
                         , serverNodeHost = encodeUtf8 . T.pack $ _serverAddress
                         , serverNodePort = fromIntegral _serverPort
                         , serverNodeGossipPort = fromIntegral _serverInternalPort
                         , serverNodeAdvertisedListeners = advertisedListenersToPB _serverAdvertisedListeners
                         }
      gossipContext <- initGossipContext defaultGossipOpts mempty serverNode _seedNodes

      serverContext <- initializeServer config gossipContext h serverState
      void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

      Async.withAsync
        (serve _serverHost _serverPort _tlsConfig serverContext
               _serverAdvertisedListeners) $ \a -> do
        a1 <- startGossip _serverHost gossipContext
        Async.link2Only (const True) a a1
        waitGossipBoot gossipContext
        Async.wait a

serve :: ByteString
      -> Word16
      -> Maybe TlsConfig
      -> ServerContext
      -> AdvertisedListeners
      -> IO ()
serve host port tlsConfig sc@ServerContext{..} listeners = do
  Log.i "************************"
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
  Log.i "*************************"

  let serverOnStarted = do
        Log.info $ "Server is started on port " <> Log.buildInt port <> ", waiting for cluster to get ready"
        void $ forkIO $ do
          void (readMVar (clusterReady gossipContext)) >> Log.i "Cluster is ready!"
          readMVar (clusterInited gossipContext) >>= \case
            Gossip -> return ()
            _ -> do
              getProtoTimestamp >>= \x -> upsertMeta @Proto.Timestamp clusterStartTimeId x metaHandle
              handle (\(_ :: RQLiteRowNotFound) -> return ()) $ deleteAllMeta @TaskAllocation metaHandle

#ifdef HStreamUseHsGrpc
  sslOpts <- mapM readTlsPemFile tlsConfig
#else
  let sslOpts = fmap initializeTlsConfig tlsConfig
#endif

  let grpcOpts =
#ifdef HStreamUseHsGrpc
        HsGrpc.ServerOptions
          { HsGrpc.serverHost = BS.toShort host
          , HsGrpc.serverPort = fromIntegral port
          , HsGrpc.serverParallelism = 0
          , HsGrpc.serverSslOptions = sslOpts
          , HsGrpc.serverOnStarted = Just serverOnStarted
          }
#else
        GRPC.defaultServiceOptions
        { GRPC.serverHost = GRPC.Host host
        , GRPC.serverPort = GRPC.Port $ fromIntegral port
        , GRPC.serverOnStarted = Just serverOnStarted
        , GRPC.sslConfig = sslOpts
        }
#endif
  forM_ (Map.toList listeners) $ \(key, vs) ->
    forM_ vs $ \I.Listener{..} -> do
      Log.debug $ "Starting advertised listener, "
               <> "key: " <> Log.buildText key <> ", "
               <> "address: " <> Log.buildText listenerAddress <> ", "
               <> "port: " <> Log.buildInt listenerPort
      forkIO $ do
        let listenerOnStarted = Log.info $ "Extra listener is started on port "
                                        <> Log.buildInt listenerPort
        let sc' = sc{scAdvertisedListenersKey = Just key}
#ifdef HStreamUseHsGrpc
        let grpcOpts' = grpcOpts { HsGrpc.serverPort = fromIntegral listenerPort
                                 , HsGrpc.serverOnStarted = Just listenerOnStarted
                                 }
        HsGrpc.runServer grpcOpts' (HsGrpc.handlers sc')
#else
        let grpcOpts' = grpcOpts { GRPC.serverPort = GRPC.Port $ fromIntegral listenerPort
                                 , GRPC.serverOnStarted = Just listenerOnStarted
                                 }
        api <- handlers sc'
        hstreamApiServer api grpcOpts'
#endif

#ifdef HStreamUseHsGrpc
  Log.warning "Starting server with a still in development lib hs-grpc-server!"
  HsGrpc.runServer grpcOpts (HsGrpc.handlers sc)
#else
  api <- handlers sc
  hstreamApiServer api grpcOpts
#endif

--------------------------------------------------------------------------------

globalWatcherFn :: MVar ServerState -> ZHandle -> ZooEvent -> ZooState -> CBytes -> IO ()
globalWatcherFn mStateS _ ZooSessionEvent stateZ _ = do
  let newServerState = case stateZ of
        ZooConnectedState  -> EnumPB NodeStateRunning
        ZooConnectingState -> EnumPB NodeStateUnavailable
        _                  -> EnumPB NodeStateUnavailable
  void $ swapMVar mStateS newServerState
  Log.info $ "Status of Zookeeper connection has changed to " <> Log.buildString' stateZ
  Log.info $ "Server currently has the state: " <> Log.buildString' newServerState
globalWatcherFn _ _ event stateZ _ = Log.debug $ "Event " <> Log.buildString' event
                                               <> "happened, current state is " <> Log.buildString' stateZ

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
updateHashRing :: GossipContext -> TVar HashRing -> IO ()
updateHashRing gc hashRing = loop []
  where
    loop list =
      loop =<< atomically
        ( do list' <- getMemberListSTM gc
             when (list == list') retry
             writeTVar hashRing $ constructServerMap list'
             return list'
        )
