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

import           Control.Concurrent               (MVar, forkIO, newEmptyMVar,
                                                   newMVar, putMVar, readMVar,
                                                   swapMVar)
import qualified Control.Concurrent.Async         as Async
import           Control.Concurrent.STM           (TVar, atomically, retry,
                                                   writeTVar)
import           Control.Exception                (bracket, handle)
import           Control.Monad                    (forM_, join, void, when)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Short            as BS
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import           Data.Word                        (Word16)
import qualified HsGrpc.Server                    as HsGrpc
import qualified Network.GRPC.HighLevel           as GRPC
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           Network.HTTP.Client              (defaultManagerSettings,
                                                   newManager)
import           System.IO                        (BufferMode (NoBuffering),
                                                   hPutStrLn, stderr)
import           Text.RawString.QQ                (r)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)
import           ZooKeeper.Types                  (ZHandle, ZooEvent, ZooState,
                                                   pattern ZooConnectedState,
                                                   pattern ZooConnectingState,
                                                   pattern ZooSessionEvent)

import           HStream.Base                     (setupFatalSignalHandler)
import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import           HStream.Common.Types             (getHStreamVersion)
import           HStream.Exception
import           HStream.Gossip                   (GossipContext (..),
                                                   defaultGossipOpts,
                                                   initGossipContext,
                                                   startGossip, waitGossipBoot)
import           HStream.Gossip.Types             (Epoch, InitType (Gossip),
                                                   ServerState (..))
import           HStream.Gossip.Utils             (getMemberListWithEpochSTM)
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          as M (MetaHandle (..),
                                                        MetaStore (..),
                                                        RHandle (..))
import           HStream.Server.Config            (AdvertisedListeners,
                                                   CliOptions (..),
                                                   ExperimentalFeature (..),
                                                   ListenersSecurityProtocolMap,
                                                   MetaStoreAddr (..),
                                                   SecurityProtocolMap,
                                                   ServerOpts (..), TlsConfig,
                                                   advertisedListenersToPB,
                                                   cliOptionsParser, getConfig)
import qualified HStream.Server.Core.Cluster      as Cluster
import qualified HStream.Server.Experimental      as Exp
import           HStream.Server.Handler           (handlers)
import qualified HStream.Server.HsGrpcHandler     as HsGrpcHandler
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   ServerNode (ServerNode),
                                                   hstreamApiServer)
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Server.Initialization    (closeRocksDBHandle,
                                                   initializeServer,
                                                   initializeTlsConfig,
                                                   openRocksDBHandle,
                                                   readTlsPemFile)
import           HStream.Server.MetaData          (TaskAllocation (..),
                                                   clusterStartTimeId,
                                                   initializeAncestors,
                                                   initializeFile,
                                                   initializeTables)
import           HStream.Server.MetaData          as P
import           HStream.Server.QueryWorker       (QueryWorker (QueryWorker))
import           HStream.Server.Types             (ServerContext (..),
                                                   TaskManager (recoverTask, resourceType),
                                                   resourceType)
import qualified HStream.Store.Logger             as Log
import qualified HStream.ThirdParty.Protobuf      as Proto
import           HStream.Utils                    (ResourceType (..),
                                                   getProtoTimestamp,
                                                   pattern EnumPB)
import           Options.Applicative              as O (CompletionResult (execCompletion),
                                                        Parser,
                                                        ParserResult (..),
                                                        defaultPrefs,
                                                        execParserPure,
                                                        fullDesc, help, helper,
                                                        info, long, progDesc,
                                                        renderFailure, short,
                                                        switch, (<**>))
import           System.Environment               (getArgs, getProgName)
import           System.Exit                      (exitSuccess)

-- TODO: Improvements
-- Rename HStream.Server.Config to HStream.Server.Cli and move these
-- "parsing code" to it. Keep main function simple and clear.
parseStartArgs :: [String] -> ParserResult StartArgs
parseStartArgs = execParserPure defaultPrefs $
  info (startArgsParser <**> helper) (fullDesc <> progDesc "HStream-Server")

data StartArgs = StartArgs
  { cliOpts      :: CliOptions
  , isGetVersion :: Bool
  }

startArgsParser :: O.Parser StartArgs
startArgsParser = StartArgs
  <$> cliOptionsParser
  <*> O.switch ( O.long "version" <> O.short 'v' <> O.help "Show server version." )

main :: IO ()
main = do
  args <- getArgs
  case parseStartArgs args of
    Success StartArgs{..} -> do
      if isGetVersion
         then do
           API.HStreamVersion{..} <- getHStreamVersion
           putStrLn $ "version: " <> T.unpack hstreamVersionVersion <> " (" <> T.unpack hstreamVersionCommit <> ")"
         else getConfig cliOpts >>= app
    Failure failure -> do
      progn <- getProgName
      let (msg, _) = renderFailure failure progn
      putStrLn msg
      exitSuccess
    CompletionInvoked compl -> handleCompletion compl
 where
   handleCompletion compl = do
     progn <- getProgName
     msg <- execCompletion compl progn
     putStr msg
     exitSuccess

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
      -> SecurityProtocolMap
      -> AdvertisedListeners
      -> ListenersSecurityProtocolMap
      -> Bool
      -- ^ Experimental features
      -> IO ()
serve host port
      sc@ServerContext{..}
      securityMap listeners listenerSecurityMap
      enableExpStreamV2 = do
  Log.i "************************"
  hPutStrLn stderr $ [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
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
  let sslOpts = initializeTlsConfig <$> join (Map.lookup "tls" securityMap)
#else
  sslOpts <- mapM readTlsPemFile $ join (Map.lookup "tls" securityMap)
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
      Log.debug $ "Starting advertised listener, "
               <> "key: " <> Log.build key <> ", "
               <> "address: " <> Log.build listenerAddress <> ", "
               <> "port: " <> Log.build listenerPort
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
        hstreamApiServer api grpcOpts'
#else
        newSslOpts <- mapM readTlsPemFile $ join ((`Map.lookup` securityMap) =<< Map.lookup key listenerSecurityMap )
        let grpcOpts' = grpcOpts { HsGrpc.serverPort = fromIntegral listenerPort
                                 , HsGrpc.serverOnStarted = Just listenerOnStarted
                                 , HsGrpc.serverSslOptions = newSslOpts
                                 }
        if enableExpStreamV2
           then do Log.info "Enable experimental feature: stream-v2"
                   slotConfig <- Exp.doStreamV2Init sc'
                   HsGrpc.runServer grpcOpts' (Exp.streamV2Handlers sc' slotConfig)
           else HsGrpc.runServer grpcOpts' (HsGrpcHandler.handlers sc')
#endif

#ifdef HStreamUseGrpcHaskell
  Log.info "Starting server with grpc-haskell..."
  api <- handlers sc
  hstreamApiServer api grpcOpts
#else
  Log.info "Starting server with hs-grpc-server..."
  if enableExpStreamV2
     then do Log.info "Enable experimental feature: stream-v2"
             slotConfig <- Exp.doStreamV2Init sc
             HsGrpc.runServer grpcOpts (Exp.streamV2Handlers sc slotConfig)
     else HsGrpc.runServer grpcOpts (HsGrpcHandler.handlers sc)
#endif

--------------------------------------------------------------------------------

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
