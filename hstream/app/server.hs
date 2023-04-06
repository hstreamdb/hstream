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
import           System.IO                        (hPutStrLn, stderr)
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
                                                   ListenersSecurityProtocolMap,
                                                   MetaStoreAddr (..),
                                                   SecurityProtocolMap,
                                                   ServerOpts (..), TlsConfig,
                                                   advertisedListenersToPB,
                                                   getConfig)
import           HStream.Server.Core.Common       (parseAllocationKey)
import           HStream.Server.Handler           (handlers)
import qualified HStream.Server.HsGrpcHandler     as HsGrpc
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   hstreamApiServer)
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
import           HStream.Server.Types             (ServerContext (..))
import qualified HStream.Store.Logger             as Log
import qualified HStream.ThirdParty.Protobuf      as Proto
import           HStream.Utils                    (ResourceType (..),
                                                   getProtoTimestamp,
                                                   pattern EnumPB,
                                                   setupSigsegvHandler)

main :: IO ()
main = getConfig >>= app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
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
      gossipContext <- initGossipContext defaultGossipOpts mempty (Just $ nodeChangeEventHandler h) serverNode _seedNodes

      serverContext <- initializeServer config gossipContext h db_m
      void . forkIO $ updateHashRing gossipContext (loadBalanceHashRing serverContext)

      Async.withAsync
        (serve _serverHost _serverPort _securityProtocolMap serverContext
               _serverAdvertisedListeners _listenersSecurityProtocolMap) $ \a -> do
        a1 <- startGossip _serverHost gossipContext
        Async.link2Only (const True) a a1
        waitGossipBoot gossipContext
        Async.wait a
    nodeChangeEventHandler :: MetaHandle -> ServerState -> I.ServerNode -> IO ()
    nodeChangeEventHandler h ServerDead I.ServerNode {..} = do
      allocations <- M.getAllMeta @TaskAllocation h
      let taskIds = map parseAllocationKey . Map.keys . Map.filter ((== serverNodeId) . taskAllocationServerId) $ allocations
      let queryIds = [qid | Right (ResQuery, qid) <- taskIds ]
      Log.debug $ "The following queries were aborted along with the death of node " <> Log.buildString' serverNodeId <> ":" <> Log.buildString' queryIds
      mapM_ (\qid -> M.updateMeta qid P.QueryAborted Nothing h) queryIds
    nodeChangeEventHandler _ _ _ = return ()
serve :: ByteString
      -> Word16
      -> SecurityProtocolMap
      -> ServerContext
      -> AdvertisedListeners
      -> ListenersSecurityProtocolMap
      -> IO ()
serve host port securityMap sc@ServerContext{..} listeners listenerSecurityMap = do
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
        HsGrpc.ServerOptions
          { HsGrpc.serverHost = BS.toShort host
          , HsGrpc.serverPort = fromIntegral port
          , HsGrpc.serverParallelism = 0
          , HsGrpc.serverSslOptions = sslOpts
          , HsGrpc.serverOnStarted = Just serverOnStarted
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
        HsGrpc.runServer grpcOpts' (HsGrpc.handlers sc')
#endif

#ifdef HStreamUseGrpcHaskell
  Log.info "Starting server with grpc-haskell..."
  api <- handlers sc
  hstreamApiServer api grpcOpts
#else
  Log.info "Starting server with hs-grpc-server..."
  HsGrpc.runServer grpcOpts (HsGrpc.handlers sc)
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
