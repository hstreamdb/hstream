{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Control.Exception
import           Control.Monad
import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           HStream.Server.Persistence
import           Network.GRPC.HighLevel.Generated
import           Options.Applicative
import           Z.Data.CBytes                    (CBytes, toBytes)
import           Z.Foreign                        (toByteString)
import           Z.IO.Network
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

data ServerConfig = ServerConfig
  { _serverHost          :: CBytes
  , _serverPort          :: PortNumber
  , _persistent          :: Bool
  , _zkHost              :: CBytes
  , _zkPort              :: CBytes
  , _logdeviceConfigPath :: CBytes
  , _topicRepFactor      :: Int
  } deriving (Show)

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> strOption   (long "host"             <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "server host value")
    <*> option auto (long "port"             <> metavar "INT"  <> showDefault <> value 6570 <> short 'p'            <> help "server port value")
    <*> flag False True (long "persistent"                                                                          <> help "set flag to store queries in zookeeper")
    <*> strOption   (long "zkhost"           <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "zookeeper host value, only meaningful when persistent flag is set")
    <*> strOption   (long "zkport"           <> metavar "INT"  <> showDefault <> value "2181"                       <> help "zookeeper port value, only meaningful when persistent flag is set")
    <*> strOption   (long "config-path"      <> metavar "PATH" <> showDefault <> value "/data/store/logdevice.conf" <> help "logdevice config path")
    <*> option auto (long "replicate-factor" <> metavar "INT"  <> showDefault <> value 3 <> short 'f'               <> help "topic replicate factor")

app :: ServerConfig -> IO ()
app config@ServerConfig{..} = if _persistent
  then withResource (defaultHandle (_zkHost <> ":" <> _zkPort)) $
    \zk -> initZooKeeper zk >> app' config (Just zk)
  else app' config Nothing

app' :: ServerConfig -> Maybe ZHandle ->  IO ()
app' ServerConfig{..} zk = do
  let options = defaultServiceOptions
                { serverHost = Host . toByteString . toBytes $ _serverHost
                , serverPort = Port . fromIntegral $ _serverPort
                }
  api <- handlers _logdeviceConfigPath zk
  print _logdeviceConfigPath
  hstreamApiServer api options

initZooKeeper :: ZHandle -> IO ()
initZooKeeper zk = catch (initializeAncestors zk) (\e -> void $ return (e :: ZNODEEXISTS))

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Server")
  putStrLn "HStream Server"
  app config
