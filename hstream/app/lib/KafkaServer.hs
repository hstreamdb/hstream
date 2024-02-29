{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- FIXME: How about merging this module into hstream/app/server.hs?
module KafkaServer
  ( app
  , runApp
  ) where

import           Control.Concurrent                (forkIO)
import qualified Control.Concurrent.Async          as Async
import           Control.Concurrent.MVar           (MVar, newEmptyMVar, putMVar,
                                                    readMVar)
import           Control.Exception                 (Handler (Handler), catches)
import           Control.Monad                     (forM, forM_, join, void)
import qualified Data.Map                          as Map
import           Data.Maybe                        (isJust)
import qualified Data.Set                          as Set
import qualified Data.Text                         as T
import           Data.Text.Encoding                (decodeUtf8, encodeUtf8)
import           Network.HTTP.Client               (defaultManagerSettings,
                                                    newManager)
import           System.Environment                (getArgs)
import           System.IO                         (hPutStrLn, stderr)
import           ZooKeeper                         (withResource,
                                                    zookeeperResInit)

import qualified Data.Vector                       as V

import           HStream.Base                      (setupFatalSignalHandler)
import           HStream.Common.Server.HashRing    (updateHashRing)
import qualified HStream.Common.Server.MetaData    as M
import qualified HStream.Common.Server.TaskManager as TM
import           HStream.Common.Types              (getHStreamVersion)
import qualified HStream.Exception                 as HE
import           HStream.Gossip                    (GossipContext (..),
                                                    defaultGossipOpts,
                                                    initGossipContext,
                                                    startGossip, waitGossipBoot)
import qualified HStream.Gossip.Types              as Gossip
import           HStream.Kafka.Common.Metrics      (startMetricsServer)
import qualified HStream.Kafka.Network             as K
import           HStream.Kafka.Server.Config       (AdvertisedListeners,
                                                    ExperimentalFeature (..),
                                                    FileLoggerSettings (..),
                                                    ListenersSecurityProtocolMap,
                                                    MetaStoreAddr (..),
                                                    SecurityProtocolMap,
                                                    ServerOpts (..),
                                                    advertisedListenersToPB,
                                                    runServerConfig)
import qualified HStream.Kafka.Server.Handler      as K
import qualified HStream.Kafka.Server.MetaData     as M
import           HStream.Kafka.Server.Types        (ServerContext (..),
                                                    initServerContext)
import qualified HStream.Logger                    as Log
import           HStream.MetaStore.Types           (MetaHandle (..),
                                                    MetaStore (..),
                                                    RHandle (..))
import           HStream.RawString                 (banner)
import qualified HStream.Server.HStreamInternal    as I
import qualified HStream.Store.Logger              as S
import qualified HStream.ThirdParty.Protobuf       as Proto
import           HStream.Utils                     (getProtoTimestamp)

-------------------------------------------------------------------------------

runApp :: IO ()
runApp = do
  args <- getArgs
  runServerConfig args app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupFatalSignalHandler
  S.setLogDeviceDbgLevel' _ldLogLevel
  let logType = case config.serverFileLog of
        Nothing -> Log.LogStderr
        Just FileLoggerSettings{..} -> Log.LogFileRotate $
          Log.FileLogSpec logpath logsize lognum
  Log.setDefaultLogger _serverLogLevel _serverLogWithColor
                       logType _serverLogFlushImmediately
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

      -- FIXME: safer way to handle this: what if updateHashRing failed?
      void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

      -- TODO: support tls (_tlsConfig)
      -- TODO: support SASL options
      -- FIXME: currently only listeners support SASL authentication
      let netOpts = K.defaultServerOpts
                      { K.serverHost = T.unpack $ decodeUtf8 _serverHost
                      , K.serverPort = _serverPort
                      , K.serverSaslOptions = Nothing
                      }

      -- Experimental features
      let usingCppServer = ExperimentalCppServer `elem` experimentalFeatures
      Async.withAsync (serve serverContext netOpts usingCppServer) $ \a -> do
        -- start gossip
        a1 <- startGossip _serverHost gossipContext
        Async.link2Only (const True) a a1
        -- start extra listeners
        as <- serveListeners serverContext
                             netOpts
                             _securityProtocolMap
                             _serverAdvertisedListeners
                             _listenersSecurityProtocolMap
                             usingCppServer
        forM_ as (Async.link2Only (const True) a)
        -- wait the default server
        waitGossipBoot gossipContext
        -- start prometheus server to export metrics
        a2 <- Async.async $ startMetricsServer "*4" (fromIntegral _metricsPort)
        Async.link2Only (const True) a a2
        Async.wait a

-- TODO: This server primarily serves as a demonstration, and there
-- is certainly room for enhancements and refinements.
serve :: ServerContext -> K.ServerOptions
      -> Bool
      -- ^ ExperimentalFeature: ExperimentalCppServer
      -> IO ()
serve sc@ServerContext{..} netOpts usingCppServer = do
  Log.i "************************"
  hPutStrLn stderr banner
  Log.i "************************"

  let serverOnStarted = do
        Log.info $ "HStream Kafka Server is started on port "
               <> Log.build (K.serverPort netOpts)
               <> ", waiting for cluster to get ready"
        void $ forkIO $ do
          void (readMVar (clusterReady gossipContext)) >> Log.info "Gossip is ready!"
          readMVar (clusterInited gossipContext) >>= \case
            Gossip.Gossip -> return ()
            _ -> do
              getProtoTimestamp >>= \x -> upsertMeta @Proto.Timestamp M.clusterStartTimeId x metaHandle
              -- FIXME: Why need to call deleteAll here?
              -- Also in CI, getRqResult(common/hstream/HStream/MetaStore/RqliteUtils.hs) may throw a RQLiteUnspecifiedErr
              -- because the affected rows are more than 1, why that's invalid ?
              deleteAllMeta @M.TaskAllocation metaHandle `catches` exceptionHandlers

          Log.info "starting task detector"
          TM.runTaskDetector $ TM.TaskDetector {
            advertisedListenersKey=scAdvertisedListenersKey
            , managers=V.fromList [scGroupCoordinator]
            , config=TM.TaskDetectorConfig { intervalMs = 30000 }
            , serverID=serverID
            , metaHandle=metaHandle
            , loadBalanceHashRing=loadBalanceHashRing
            , gossipContext=gossipContext
            }
          Log.info "Cluster is ready!"

  let netOpts' = netOpts{ K.serverOnStarted = Just serverOnStarted}
  Log.info $ "Starting"
        <> if isJust (K.serverSslOptions netOpts') then " secure " else " insecure "
        <> "kafka server..."
  if usingCppServer
     then do Log.warning "Using a still-in-development c++ kafka server!"
             K.runCppServer netOpts' sc K.handlers
     else K.runHsServer netOpts' sc K.unAuthedHandlers K.handlers
  where
   exceptionHandlers =
     [ Handler $ \(_ :: HE.RQLiteRowNotFound)    -> return ()
     , Handler $ \(_ :: HE.RQLiteUnspecifiedErr) -> return ()
     ]

serveListeners
  :: ServerContext
  -> K.ServerOptions
  -> SecurityProtocolMap
  -> AdvertisedListeners
  -> ListenersSecurityProtocolMap
  -> Bool
  -- ^ ExperimentalFeature: ExperimentalCppServer
  -> IO [Async.Async ()]
serveListeners sc netOpts
               securityMap listeners listenerSecurityMap
               usingCppServer
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

    -- sasl
    let newSaslOpts =
          if _enableSaslAuth (serverOpts sc)
          then join (snd <$> ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap))
          else Nothing
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
    if usingCppServer
       then do Log.warning "Using a still-in-development c++ kafka server!"
               K.runCppServer netOpts' sc' K.handlers
       else K.runHsServer netOpts' sc' K.unAuthedHandlers K.handlers

-------------------------------------------------------------------------------

-- TODO
nodeChangeEventHandler
  :: MVar ServerContext -> Gossip.ServerState -> I.ServerNode -> IO ()
nodeChangeEventHandler _scMVar Gossip.ServerDead I.ServerNode {..} = do
  Log.info $ "(TODO) Handle Server Dead event: " <> Log.buildString' serverNodeId
nodeChangeEventHandler _ _ _ = return ()
