{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
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
    <*> strOption   (long "zkhost"           <> metavar "HOST" <> showDefault <> value "0.0.0.0"                    <> help "zookeeper host value")
    <*> strOption   (long "zkport"           <> metavar "INT"  <> showDefault <> value "2181" <> short 'p'          <> help "zookeeper port value")
    <*> strOption   (long "config-path"      <> metavar "PATH" <> showDefault <> value "/data/store/logdevice.conf" <> help "logdevice config path")
    <*> option auto (long "replicate-factor" <> metavar "INT"  <> showDefault <> value 3 <> short 'f'               <> help "topic replicate factor")

app :: ServerConfig -> IO ()
app ServerConfig{..} = withResource (defaultHandle (_zkHost <> ":" <> _zkPort)) $ \zk -> do
  initZooKeeper zk
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
