{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Concurrent             (forkIO)
import           Control.Monad                  (void)
import           Network.GRPC.HighLevel         (ServiceOptions (..))
import           Network.GRPC.HighLevel.Client  (Port (unPort))
import           Options.Applicative
import           Text.RawString.QQ              (r)
import           ZooKeeper                      (withResource)

import qualified HStream.Logger                 as Log
import           HStream.Server.Bootstrap       (startServer)
import           HStream.Server.HStreamApi      (hstreamApiServer)
import           HStream.Server.HStreamInternal (hstreamInternalServer)
import           HStream.Server.Handler         (handlers)
import           HStream.Server.Initialization  (initializeServer)
import           HStream.Server.InternalHandler (internalHandlers)
import           HStream.Server.Leader          (selectLeader)
import           HStream.Server.LoadBalance     (startWritingLoadReport)
import           HStream.Server.Persistence     (defaultHandle,
                                                 initializeAncestors)
import           HStream.Server.Types           (LoadBalanceMode (..),
                                                 LoadManager,
                                                 ServerContext (..),
                                                 ServerOpts (..))
import           HStream.Store                  (Compression (..))
import qualified HStream.Store.Admin.API        as AA
import qualified HStream.Store.Logger           as Log
import           HStream.Utils                  (setupSigsegvHandler)

-- TODO
-- 1. config file for the Server

parseConfig :: Parser ServerOpts
parseConfig =
  ServerOpts
    <$> strOption ( long "host" <> metavar "HOST"
                 <> showDefault <> value "127.0.0.1"
                 <> help "server host value"
                  )
    <*> option auto ( long "port" <> short 'p' <> metavar "INT"
                   <> showDefault <> value 6570
                   <> help "server port value"
                    )
    <*> strOption ( long "address" <> metavar "ADDRESS"
                 <> showDefault <> value "127.0.0.1"
                 <> help "server address"
                  )
    <*> option auto ( long "internal-port" <> metavar "INT"
                  <> showDefault <> value 6571
                  <> help "server channel port value for internal communication"
                  )
    <*> option auto ( long "server-id" <> metavar "UINT32"
                 <> help "ID of the hstream server node"
                  )
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
              <> help "print logs with color or not"
               )
    <*> option auto ( long "ld-log-level" <> metavar "[critical|error|warning|notify|info|debug|spew]"
                   <> showDefault <> value (Log.LDLogLevel Log.C_DBG_INFO)
                   <> help "hstore log level"
                    )
    <*> option auto ( long "load-balance-mode" <> metavar "[round-robin|hardware-usage]"
                   <> showDefault <> value RoundRobin
                   <> help "hserver cluster load balance mode"
                    )

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
  Log.setLogDeviceDbgLevel' _ldLogLevel
  withResource (defaultHandle _zkUri) $ \zk -> do
    (options, options', serverContext, lm) <- initializeServer config zk
    initializeAncestors zk
    startServer zk config (serve options options' serverContext lm)

serve :: ServiceOptions -> ServiceOptions -> ServerContext -> LoadManager -> IO ()
serve options@ServiceOptions{..} optionsInternal sc@ServerContext{..} lm = do
  startWritingLoadReport zkHandle lm

  selectLeader sc

  -- GRPC service
  Log.i "************************"
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
  Log.i $ "Server started on port " <> Log.buildInt (unPort serverPort)
  Log.i "*************************"
  api <- handlers sc
  internalApi <- internalHandlers sc
  void . forkIO $ hstreamInternalServer internalApi optionsInternal
  hstreamApiServer api options

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Server")
  app config
