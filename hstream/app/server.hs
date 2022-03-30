{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent               (MVar, forkIO, modifyMVar_,
                                                   newMVar, swapMVar)
import           Control.Monad                    (void)
import           Data.List                        (sort)
import qualified Data.Text                        as T
import qualified Network.GRPC.HighLevel           as GRPC
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           Text.RawString.QQ                (r)
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (withResource,
                                                   zooWatchGetChildren,
                                                   zookeeperResInit)
import           ZooKeeper.Types

import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import qualified HStream.Logger                   as Log
import           HStream.Server.Config            (getConfig)
import           HStream.Server.Handler           (handlers)
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   hstreamApiServer)
import           HStream.Server.Initialization    (initNodePath,
                                                   initializeServer,
                                                   initializeTlsConfig)
import           HStream.Server.Persistence       (getServerNode',
                                                   initializeAncestors,
                                                   serverRootPath)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerOpts (..), ServerState)
import qualified HStream.Store.Logger             as Log
import           HStream.Utils                    (cbytes2bs, pattern EnumPB,
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
    let serverOnStarted = do
          initNodePath zk _serverID (T.pack _serverAddress) (fromIntegral _serverPort) (fromIntegral _serverInternalPort)
          Log.i $ "Server is started on port " <> Log.buildInt _serverPort
    let grpcOpts =
          GRPC.defaultServiceOptions
          { GRPC.serverHost = GRPC.Host . cbytes2bs $ _serverHost
          , GRPC.serverPort = GRPC.Port . fromIntegral $ _serverPort
          , GRPC.serverOnStarted = Just serverOnStarted
          , GRPC.sslConfig = fmap initializeTlsConfig _tlsConfig
          }
    initializeAncestors zk
    serverContext <- initializeServer config zk serverState
    serve grpcOpts serverContext

serve :: GRPC.ServiceOptions -> ServerContext -> IO ()
serve options sc@ServerContext{..} = do
  void . forkIO $ updateHashRing zkHandle loadBalanceHashRing
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
-- TODO: Instead of reconstruction, we should use the operation insert/delete.
updateHashRing :: ZHandle -> MVar HashRing -> IO ()
updateHashRing zk mhr = zooWatchGetChildren zk serverRootPath callback action
  where
    callback HsWatcherCtx{..} = updateHashRing watcherCtxZHandle mhr

    action (StringsCompletion (StringVector children)) = do
      modifyMVar_ mhr $ \_ -> do
        serverNodes <- mapM (getServerNode' zk) children
        pure $ constructServerMap . sort $ serverNodes
