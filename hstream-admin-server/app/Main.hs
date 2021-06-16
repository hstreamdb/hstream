{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

import           Control.Exception              (catch)
import           Control.Monad                  (void)
import           Control.Monad.IO.Class         (liftIO)
import           Data.ByteString                (ByteString)
import           Network.Wai.Handler.Warp       (run)
import           Options.Applicative            (Parser, auto, execParser,
                                                 fullDesc, help, helper, info,
                                                 long, metavar, option,
                                                 progDesc, short, showDefault,
                                                 strOption, value, (<**>))
import           Servant                        (Proxy (..), (:<|>) (..))
import           Servant.Server                 (Server, serve)
import qualified Z.Data.CBytes                  as ZDC
import qualified ZooKeeper                      as ZK
import qualified ZooKeeper.Exception            as ZK
import qualified ZooKeeper.Types                as ZK

import           HStream.Admin.Server.Connector (ConnectorsAPI, connectorServer)
import           HStream.Admin.Server.Node      (NodesAPI, nodeServer)
import           HStream.Admin.Server.Overview  (OverviewAPI, overviewServer)
import           HStream.Admin.Server.Query     (QueriesAPI, queryServer)
import           HStream.Admin.Server.Stream    (StreamsAPI, streamServer)
import           HStream.Server.Persistence     as HSP
import qualified HStream.Store                  as HS

data ServerConfig = ServerConfig
  { _serverHost          :: ZDC.CBytes
  , _serverPort          :: Int
  , _zkHost              :: ZDC.CBytes
  , _zkPort              :: ZDC.CBytes
  , _logdeviceConfigPath :: ZDC.CBytes
  , _checkpointRootPath  :: ZDC.CBytes
  , _streamRepFactor     :: Int
  , _ldAdminHost         :: ByteString
  , _ldAdminPort         :: Int
  } deriving (Show)

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> strOption   (long "host"                 <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "server host value")
    <*> option auto (long "port"                 <> metavar "INT"  <> showDefault <> value 8000 <> short 'p'            <> help "server port value")
    <*> strOption   (long "zkhost"               <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "zookeeper host value, only meaningful when persistent flag is set")
    <*> strOption   (long "zkport"               <> metavar "INT"  <> showDefault <> value "2181"                       <> help "zookeeper port value, only meaningful when persistent flag is set")
    <*> strOption   (long "config-path"          <> metavar "PATH" <> showDefault <> value "/data/store/logdevice.conf" <> help "logdevice config path")
    <*> strOption   (long "checkpoint-path"      <> metavar "PATH" <> showDefault <> value "/tmp/checkpoint"            <> help "checkpoint root path")
    <*> option auto (long "replicate-factor"     <> metavar "INT"  <> showDefault <> value 3 <> short 'f'               <> help "topic replicate factor")
    <*> strOption   (long "logdevice-admin-host" <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "logdevice admin host value")
    <*> option auto (long "logdevice-admin-port" <> metavar "INT"  <> showDefault <> value 39609                        <> help "logdevice admin port value")

initZooKeeper :: ZK.ZHandle -> IO ()
initZooKeeper zk = catch (initializeAncestors zk) (\e -> void $ return (e :: ZK.ZNODEEXISTS))

type API =
  StreamsAPI
  :<|> QueriesAPI
  :<|> NodesAPI
  :<|> ConnectorsAPI
  :<|> OverviewAPI

api :: Proxy API
api = Proxy

apiServer :: HS.LDClient -> Maybe ZK.ZHandle -> ServerConfig -> Server API
apiServer ldClient zk ServerConfig{..} = do
  (streamServer ldClient)
  :<|> (queryServer ldClient zk (_streamRepFactor, _checkpointRootPath))
  :<|> (nodeServer _ldAdminHost _ldAdminPort)
  :<|> (connectorServer ldClient zk)
  :<|> (overviewServer ldClient zk _ldAdminHost _ldAdminPort)

app :: ServerConfig -> IO ()
app config@ServerConfig{..} =
  ZK.withResource (HSP.defaultHandle (_zkHost <> ":" <> _zkPort)) $
    \zk -> do
      initZooKeeper zk
      ldClient <- liftIO (HS.newLDClient _logdeviceConfigPath)
      run _serverPort $ serve api $ apiServer ldClient (Just zk) config

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Admin-Server")
  app config
