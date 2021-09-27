{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import qualified Data.UUID                     as UUID
import           Data.UUID.V4                  (nextRandom)
import           Options.Applicative
import           Text.RawString.QQ             (r)
import           ZooKeeper
import           ZooKeeper.Types

import           Control.Concurrent            (forkIO, isEmptyMVar, putMVar,
                                                readMVar, swapMVar)
import           Control.Monad                 (void)
import qualified HStream.Logger                as Log
import           HStream.Server.Bootstrap
import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           HStream.Server.Initialization
import           HStream.Server.LoadBalance
import           HStream.Server.Persistence
import           HStream.Server.Types
import           HStream.Server.Watcher        (actionTriggedByNodesChange)
import           HStream.Store                 (Compression (..))
import qualified HStream.Store.Admin.API       as AA
import           Network.GRPC.HighLevel        (ServiceOptions)
import qualified Z.Data.CBytes                 as CB
import           ZooKeeper.Recipe.Election     (election)

-- TODO
-- 1. config file for the Server

parseConfig :: Parser ServerOpts
parseConfig =
  ServerOpts
    <$> strOption ( long "host" <> metavar "HOST"
                 <> showDefault <> value "127.0.0.1"
                 <> help "server host value"
                  )
    <*> strOption ( long "address" <> metavar "ADDRESS"
                 <> help "server address"
                  )
    <*> option auto ( long "port" <> short 'p' <> metavar "INT"
                   <> showDefault <> value 6570
                   <> help "server port value"
                    )
    <*> option auto ( long "internal-port" <> metavar "INT"
                   <> showDefault <> value 6571
                   <> help "server channel port value for internal communication"
                    )
    <*> strOption ( long "name" <> metavar "NAME"
                 <> showDefault <> value "hserver-1"
                 <> help "name of the hstream server node"
                  )
    <*> option auto ( long "min-servers" <> metavar "INT"
                   <> showDefault <> value 1
                   <> help "minimal hstream servers")
    <*> strOption ( long "zkuri" <> metavar "STR"
                 <> showDefault
                 <> value "127.0.0.1:2181"
                 <> help ( "comma separated host:port pairs, each corresponding"
                      <> "to a zk zookeeper server. "
                      <> "e.g. \"127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183\""
                         )
                  )
    <*> strOption ( long "store-config" <> metavar "PATH"
                 <> showDefault <> value "/data/store/logdevice.conf"
                 <> help "logdevice config path"
                  )
    <*> option auto ( long "replicate-factor" <> metavar "INT"
                   <> showDefault <> value 3
                   <> help "topic replicate factor"
                    )
    <*> option auto ( long "ckp-replicate-factor" <> metavar "INT"
                   <> showDefault <> value 1
                   <> help "checkpoint replicate factor"
                    )
    <*> option auto ( long "timeout" <> metavar "INT"
                   <> showDefault <> value 1000
                   <> help "the timer timeout in milliseconds"
                    )
    <*> option auto ( long "compression" <> metavar "none|lz4|lz4hc"
                   <> showDefault <> value CompressionLZ4
                   <> help "Specify the compression policy for gdevice"
                    )
    <*> strOption   ( long "store-admin-host" <> metavar "HOST"
                   <> showDefault <> value "127.0.0.1" <> help "logdevice admin host"
                    )
    <*> option auto ( long "store-admin-port" <> metavar "INT"
                   <> showDefault <> value 6440 <> help "logdevice admin port"
                    )
    <*> option auto ( long "store-admin-protocol-id" <> metavar "ProtocolId"
                   <> showDefault <> value AA.binaryProtocolId <> help "logdevice admin thrift protocol id"
                    )
    <*> option auto ( long "store-admin-conn-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift connection timeout in milliseconds"
                    )
    <*> option auto ( long "store-admin-send-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift sending timeout in milliseconds"
                    )
    <*> option auto ( long "store-admin-recv-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift receiving timeout in milliseconds"
                    )
    <*> option auto ( long "log-level" <> metavar "[critical|fatal|warning|info|debug]"
                   <> showDefault <> value (Log.Level Log.INFO)
                   <> help "server log level"
                    )
    <*> switch ( long "log-with-color"
              <> help "print logs with color or not" )

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  (options, options', mkSC, lm) <- initializeServer config
  withResource (defaultHandle _zkUri) $ \zk -> do
    startServer zk config (serve config options options' (mkSC zk) lm)

serve :: ServerOpts -> ServiceOptions -> ServiceOptions -> ServerContext -> LoadManager -> IO ()
serve config options options' sc@ServerContext{..} lm@LoadManager{..} = do
  -- Load balancing data
  lr <- readMVar loadReport
  writeLoadReportToZooKeeper zkHandle serverName lr
  updateLoadReports zkHandle loadReports ranking
  Log.debug . Log.buildCBytes $ "Server data initialized "

  -- Leader election
  -- uuid <- nextRandom
  -- _ <- forkIO $ election zkHandle "/election" (CB.pack . UUID.toString $ uuid)
  --   (do
  --       void $ zooSet zkHandle leaderPath (Just $ CB.toBytes serverName) Nothing
  --       noLeader <- isEmptyMVar leaderName
  --       case noLeader of
  --         True  -> putMVar leaderName serverName
  --         False -> void $ swapMVar leaderName serverName
  --   )
  --   (\_ -> do
  --       (DataCompletion v_ _) <- zooGet zkHandle leaderPath
  --       case v_ of
  --         Nothing -> return ()
  --         Just v  -> do
  --           noLeader <- isEmptyMVar leaderName
  --           case noLeader of
  --             True  -> putMVar leaderName (CB.fromBytes v)
  --             False -> void $ swapMVar leaderName (CB.fromBytes v)
  --   )

  -- Load balancing service
  localReportUpdateTimer sc lm
  zkReportUpdateTimer serverName sc

  -- Set watcher for nodes changes
  void $ forkIO $ actionTriggedByNodesChange config sc lm

  -- GRPC service
  Log.debug "**************************************************"
  Log.debug . Log.buildCBytes $
    "Server " <> serverName <> "started on port " <> CB.pack (show serverPort)
  Log.debug . Log.buildCBytes $
    "Internal Server " <> serverName <> "started on port " <> CB.pack (show serverInternalPort)
  Log.debug "**************************************************"
  -- let api = mkInternalHandlers sc
  let api' = handlers sc
  -- _ <- forkIO $ serverChannelServer api' options'
  hstreamApiServer api' options

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Server")
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
  app config
