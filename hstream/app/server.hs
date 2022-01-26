{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent               (forkIO)
import           Control.Monad                    (void)
import qualified Data.Text                        as T
import qualified Network.GRPC.HighLevel           as GRPC
import qualified Network.GRPC.HighLevel.Client    as GRPC
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           Text.RawString.QQ                (r)
import           ZooKeeper                        (withResource,
                                                   zookeeperResInit)


import qualified HStream.Logger                   as Log
import           HStream.Server.Config            (getConfig)
import           HStream.Server.Handler           (handlers)
import           HStream.Server.HStreamApi        (hstreamApiServer)
import           HStream.Server.Initialization    (initNodePath,
                                                   initializeServer)
import           HStream.Server.LoadBalance       (actionTriggeredByServerNodeChange)
import           HStream.Server.Persistence       (initializeAncestors)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerOpts (..))
import qualified HStream.Store.Logger             as Log
import           HStream.Utils                    (cbytes2bs,
                                                   setupSigsegvHandler)

main :: IO ()
main = getConfig >>= app

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  let serverOnStarted = Log.i $ "Server is started on port " <> Log.buildInt _serverPort
  let grpcOpts =
        GRPC.defaultServiceOptions
        { GRPC.serverHost = GRPC.Host . cbytes2bs $ _serverHost
        , GRPC.serverPort = GRPC.Port . fromIntegral $ _serverPort
        , GRPC.serverOnStarted = Just serverOnStarted
        }
  setupSigsegvHandler
  Log.setLogDeviceDbgLevel' _ldLogLevel
  let zkRes = zookeeperResInit _zkUri Nothing{- WatcherFn -} 5000 Nothing 0
  withResource zkRes $ \zk -> do
    initializeAncestors zk

    serverContext <- initializeServer config zk
    initNodePath zk _serverID (T.pack _serverAddress) (fromIntegral _serverPort) (fromIntegral _serverInternalPort)
    serve grpcOpts serverContext

serve :: GRPC.ServiceOptions -> ServerContext -> IO ()
serve options sc = do
  void . forkIO $ actionTriggeredByServerNodeChange sc
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
