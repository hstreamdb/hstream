{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent               (MVar, forkIO, newMVar,
                                                   readMVar, swapMVar)
import           Control.Concurrent.Async         (concurrently_)
import           Control.Concurrent.STM           (TVar, atomically, retry,
                                                   writeTVar)
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
import           Text.RawString.QQ                (r)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)
import           ZooKeeper.Types                  (ZHandle, ZooEvent, ZooState,
                                                   pattern ZooConnectedState,
                                                   pattern ZooConnectingState,
                                                   pattern ZooSessionEvent)

import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import           HStream.Gossip                   (GossipContext (..),
                                                   defaultGossipOpts,
                                                   getMemberListSTM,
                                                   initGossipContext,
                                                   startGossip)
import qualified HStream.Logger                   as Log
import           HStream.Server.Config            (AdvertisedListeners,
                                                   ServerOpts (..), TlsConfig,
                                                   advertisedListenersToPB,
                                                   getConfig)
import           HStream.Server.Handler           (handlers)
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   hstreamApiServer)
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Server.Initialization    (initializeServer,
                                                   initializeTlsConfig)
import           HStream.Server.MetaData          (initializeAncestors)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerState)
import qualified HStream.Store.Logger             as Log
import           HStream.Utils                    (cbytes2bs, pattern EnumPB,
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

  -- TODO: remove me
  serverState <- newMVar (EnumPB NodeStateStarting)

  let zkRes = zookeeperResInit _zkUri (Just $ globalWatcherFn serverState) 5000 Nothing 0
      serverHostBS = cbytes2bs _serverHost
  withResource zkRes $ \zk -> do
    initializeAncestors zk

    let serverNode =
          I.ServerNode { serverNodeId = _serverID
                       , serverNodeHost = encodeUtf8 . T.pack $ _serverAddress
                       , serverNodePort = fromIntegral _serverPort
                       , serverNodeGossipPort = fromIntegral _serverInternalPort
                       , serverNodeAdvertisedListeners = advertisedListenersToPB _serverAdvertisedListeners
                       }
    gossipContext <- initGossipContext defaultGossipOpts mempty serverNode _seedNodes

    serverContext <- initializeServer config gossipContext zk serverState
    void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

    concurrently_ (startGossip gossipContext)
      (serve serverHostBS _serverPort _tlsConfig serverContext _serverAdvertisedListeners)

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
        void $ forkIO $ void (readMVar (clusterReady gossipContext)) >> Log.i "Cluster is ready!"
  let grpcOpts =
        GRPC.defaultServiceOptions
        { GRPC.serverHost = GRPC.Host host
        , GRPC.serverPort = GRPC.Port $ fromIntegral port
        , GRPC.serverOnStarted = Just serverOnStarted
        , GRPC.sslConfig = fmap initializeTlsConfig tlsConfig
        }

  forM_ (Map.toList listeners) $ \(key, vs) ->
    forM_ vs $ \I.Listener{..} -> do
      Log.debug $ "Starting advertised listener, "
               <> "key: " <> Log.buildText key <> ", "
               <> "address: " <> Log.buildText listenerAddress <> ", "
               <> "port: " <> Log.buildInt listenerPort
      forkIO $ do
        -- TODO: support HStreamUseHsGrpc
        let listenerOnStarted = Log.info $ "Extra listener is started on port "
                                        <> Log.buildInt listenerPort
        let grpcOpts' = grpcOpts { GRPC.serverPort = GRPC.Port $ fromIntegral listenerPort
                                 , GRPC.serverOnStarted = Just listenerOnStarted
                                 }
        api <- handlers sc{scAdvertisedListenersKey = Just key}
        hstreamApiServer api grpcOpts'
#ifdef HStreamUseHsGrpc
  Log.warning "Starting server with a still in development lib hs-grpc-server!"
  let serverOptions = HsGrpc.ServerOptions { HsGrpc.serverHost = BS.toShort host
                                           , HsGrpc.serverPort = fromIntegral port
                                           , HsGrpc.serverParallelism = 0
                                           , HsGrpc.serverOnStarted = Just serverOnStarted
                                           }
  HsGrpc.runServer serverOptions (HsGrpc.handlers sc)
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
