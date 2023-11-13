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

import           Control.Concurrent             (forkIO)
import qualified Control.Concurrent.Async       as Async
import           Control.Concurrent.MVar        (MVar, newEmptyMVar, putMVar,
                                                 readMVar)
import           Control.Exception              (handle)
import           Control.Monad                  (forM, forM_, void)
import qualified Data.Map                       as Map
import           Data.Maybe                     (isJust)
import qualified Data.Set                       as Set
import qualified Data.Text                      as T
import           Data.Text.Encoding             (decodeUtf8, encodeUtf8)
import           Network.HTTP.Client            (defaultManagerSettings,
                                                 newManager)
import           System.Environment             (getArgs)
import           System.IO                      (hPutStrLn, stderr)
import           ZooKeeper                      (withResource, zookeeperResInit)

import           HStream.Base                   (setupFatalSignalHandler)
import           HStream.Common.Server.HashRing (updateHashRing)
import qualified HStream.Common.Server.MetaData as M
import           HStream.Common.Types           (getHStreamVersion)
import qualified HStream.Exception              as HE
import           HStream.Gossip                 (GossipContext (..),
                                                 defaultGossipOpts,
                                                 initGossipContext, startGossip,
                                                 waitGossipBoot)
import qualified HStream.Gossip.Types           as Gossip
import qualified HStream.Kafka.Network          as K
import           HStream.Kafka.Server.Config    (AdvertisedListeners,
                                                 ListenersSecurityProtocolMap,
                                                 MetaStoreAddr (..),
                                                 SecurityProtocolMap,
                                                 ServerOpts (..),
                                                 advertisedListenersToPB,
                                                 runServerConfig)
import qualified HStream.Kafka.Server.Handler   as K
import           HStream.Kafka.Server.Types     (ServerContext (..),
                                                 initServerContext)
import qualified HStream.Logger                 as Log
import           HStream.MetaStore.Types        (MetaHandle (..),
                                                 MetaStore (..), RHandle (..))
import qualified HStream.Server.HStreamInternal as I
import qualified HStream.Store.Logger           as S
import qualified HStream.ThirdParty.Protobuf    as Proto
import           HStream.Utils                  (getProtoTimestamp)

#ifndef HSTREAM_ENABLE_ASAN
import           Text.RawString.QQ              (r)
#endif

-------------------------------------------------------------------------------

main :: IO ()
main = do
  args <- getArgs
  runServerConfig args app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupFatalSignalHandler
  Log.setDefaultLogger _serverLogLevel _serverLogWithColor
                       Log.LogStderr _serverLogFlushImmediately
  S.setLogDeviceDbgLevel' _ldLogLevel
  case _metaStore of
    ZkAddr addr -> do
      let zkRes = zookeeperResInit addr Nothing 5000 Nothing 0
      withResource zkRes $ \zk -> M.initKafkaZkPaths zk >> action (ZkHandle zk)
    RqAddr addr -> do
      m <- newManager defaultManagerSettings
      let rq = RHandle m addr
      M.initKafkaRqTables rq
      action (RLHandle rq)
    FileAddr addr -> do
      M.initKafkaFileTables addr
      action (FileHandle addr)

  where
    action h = do
      hstreamVersion <- getHStreamVersion
      let serverNode =
            I.ServerNode{ serverNodeId = _serverID
                        , serverNodeVersion = Just hstreamVersion
                        , serverNodePort = fromIntegral _serverPort
                        , serverNodeAdvertisedAddress = encodeUtf8 . T.pack $ _advertisedAddress
                        , serverNodeGossipPort = fromIntegral _serverGossipPort
                        , serverNodeGossipAddress = encodeUtf8 . T.pack $ _serverGossipAddress
                        , serverNodeAdvertisedListeners = advertisedListenersToPB _serverAdvertisedListeners
                        }
      scMVar <- newEmptyMVar
      gossipContext <- initGossipContext defaultGossipOpts mempty (Just $ nodeChangeEventHandler scMVar) serverNode _seedNodes
      serverContext <- initServerContext config gossipContext h
      putMVar scMVar serverContext

      void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

      -- TODO: support tls (_tlsConfig)
      -- TODO: support SASL options
      let netOpts = K.defaultServerOpts
                      { K.serverHost = T.unpack $ decodeUtf8 _serverHost
                      , K.serverPort = fromIntegral _serverPort
                      , K.serverSaslOptions = if _enableSaslAuth then Just K.SaslOptions else Nothing
                      }
      Async.withAsync (serve serverContext netOpts) $ \a -> do
        -- start gossip
        a1 <- startGossip _serverHost gossipContext
        Async.link2Only (const True) a a1
        -- start extra listeners
        as <- serveListeners serverContext
                             netOpts
                             _securityProtocolMap
                             _serverAdvertisedListeners
                             _listenersSecurityProtocolMap
        forM_ as (Async.link2Only (const True) a)
        -- wati the default server
        waitGossipBoot gossipContext
        Async.wait a

-- TODO: This server primarily serves as a demonstration, and there
-- is certainly room for enhancements and refinements.
serve :: ServerContext -> K.ServerOptions -> IO ()
serve sc@ServerContext{..} netOpts = do
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
        Log.info $ "HStream Kafka Server is started on port "
               <> Log.build (K.serverPort netOpts)
               <> ", waiting for cluster to get ready"
        void $ forkIO $ do
          void (readMVar (clusterReady gossipContext)) >> Log.info "Cluster is ready!"
          readMVar (clusterInited gossipContext) >>= \case
            Gossip.Gossip -> return ()
            _ -> do
              getProtoTimestamp >>= \x -> upsertMeta @Proto.Timestamp M.clusterStartTimeId x metaHandle
              handle (\(_ :: HE.RQLiteRowNotFound) -> return ()) $ deleteAllMeta @M.TaskAllocation metaHandle

  let netOpts' = netOpts{ K.serverOnStarted = Just serverOnStarted}
  Log.info $ "Starting"
        <> if isJust (K.serverSslOptions netOpts') then " secure " else " insecure "
        <> (if isJust (K.serverSaslOptions netOpts') then "SASL " else "")
        <> "kafka server..."
  K.runServer netOpts' sc K.unAuthedHandlers K.handlers

serveListeners
  :: ServerContext
  -> K.ServerOptions
  -> SecurityProtocolMap
  -> AdvertisedListeners
  -> ListenersSecurityProtocolMap
  -> IO [Async.Async ()]
serveListeners sc netOpts
               _securityMap listeners _listenerSecurityMap
               = do
  let listeners' = [(k, v) | (k, vs) <- Map.toList listeners, v <- Set.toList vs]
  forM listeners' $ \(key, I.Listener{..}) -> Async.async $ do
    let listenerOnStarted = Log.info $ "Extra listener is started on port "
                                    <> Log.build listenerPort
    let sc' = sc{scAdvertisedListenersKey = Just key}
    -- TODO: tls
    -- newSslOpts <- mapM readTlsPemFile $
    --   join ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap)
    let newSslOpts = Nothing

    -- TODO: sasl
    let newSaslOpts = Nothing
    let netOpts' = netOpts{ K.serverPort = fromIntegral listenerPort
                          , K.serverOnStarted = Just listenerOnStarted
                          , K.serverSslOptions = newSslOpts
                          , K.serverSaslOptions = newSaslOpts
                          }
    Log.info $ "Starting"
            <> (if isJust (K.serverSslOptions netOpts') then " secure " else " insecure ")
            <> (if isJust (K.serverSaslOptions netOpts') then "SASL " else "")
            <> "advertised listener: "
            <> Log.build key <> ":"
            <> Log.build listenerAddress <> ":"
            <> Log.build listenerPort
    K.runServer netOpts' sc' K.unAuthedHandlers K.handlers

-------------------------------------------------------------------------------

-- TODO
nodeChangeEventHandler
  :: MVar ServerContext -> Gossip.ServerState -> I.ServerNode -> IO ()
nodeChangeEventHandler _scMVar Gossip.ServerDead I.ServerNode {..} = do
  Log.info $ "(TODO) Handle Server Dead event: " <> Log.buildString' serverNodeId
nodeChangeEventHandler _ _ _ = return ()
