{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
import           Control.Concurrent               (MVar, forkIO, newMVar,
                                                   swapMVar)
import           Control.Concurrent.STM           (TVar, atomically, retry,
                                                   writeTVar)
import           Control.Monad                    (void, when)
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
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
import           HStream.Gossip                   (getMemberListSTM,
                                                   initGossipContext,
                                                   startGossip)
import           HStream.Gossip.Types             (GossipContext (..),
                                                   defaultGossipOpts)
import qualified HStream.Logger                   as Log
import           HStream.Server.Config            (ServerOpts (..), getConfig)
import           HStream.Server.Handler           (handlers)
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   hstreamApiServer)
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Server.Initialization    (initializeServer,
                                                   initializeTlsConfig)
import           HStream.Server.Persistence       (initializeAncestors)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerState)
import qualified HStream.Store.Logger             as Log
import           HStream.Utils                    (cbytes2bs,
                                                   fromInternalServerNode,
                                                   pattern EnumPB,
                                                   setupSigsegvHandler)

main :: IO ()
main = getConfig >>= app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
  Log.setLogLevel _serverLogLevel _serverLogWithColor
  Log.setLogDeviceDbgLevel' _ldLogLevel
  serverState <- newMVar (EnumPB NodeStateStarting)
  let zkRes = zookeeperResInit _zkUri (Just $ globalWatcherFn serverState) 5000 Nothing 0
  withResource zkRes $ \zk -> do
    let serverOnStarted = Log.i $ "Server is started on port " <> Log.buildInt _serverPort
    let grpcOpts =
          GRPC.defaultServiceOptions
          { GRPC.serverHost = GRPC.Host . cbytes2bs $ _serverHost
          , GRPC.serverPort = GRPC.Port . fromIntegral $ _serverPort
          , GRPC.serverOnStarted = Just serverOnStarted
          , GRPC.sslConfig = fmap initializeTlsConfig _tlsConfig
          }
    initializeAncestors zk
    let serverNode = I.ServerNode _serverID
                                  (encodeUtf8 . T.pack $ _serverAddress)
                                  (fromIntegral _serverPort)
                                  (fromIntegral _serverInternalPort)
    gossipContext <- initGossipContext defaultGossipOpts mempty serverNode
    serverContext <- initializeServer config gossipContext zk serverState
    void $ startGossip (cbytes2bs _serverHost) _seedNodes gossipContext
    serve grpcOpts serverContext

serve :: GRPC.ServiceOptions -> ServerContext -> IO ()
serve options sc@ServerContext{..} = do
  void . forkIO $ updateHashRing gossipContext loadBalanceHashRing
  -- GRPC service
  Log.i "************************"
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
  Log.i "*************************"
  api <- handlers sc
  hstreamApiServer api options

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
        ( do
            list' <- map fromInternalServerNode <$> getMemberListSTM gc
            when (list == list') retry
            writeTVar hashRing $ constructServerMap list'
            return list'
        )
