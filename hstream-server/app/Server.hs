{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           Network.GRPC.HighLevel.Generated

import           Options.Applicative
import           Z.Data.CBytes                    (CBytes, toBytes)
import           Z.Foreign                        (toByteString)
import           Z.IO.Network

data ServerConfig = ServerConfig
  { _serverHost          :: CBytes
  , _serverPort          :: PortNumber
  , _logdeviceConfigPath :: CBytes
  , _topicRepFactor      :: Int
  } deriving (Show)

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> strOption   (long "host" <> metavar "HOST" <> showDefault <> value "127.0.0.1" <> help "server host value")
    <*> option auto (long "port" <> metavar "INT" <> showDefault <> value 50051 <> short 'p' <> help "server port value")
    <*> strOption (long "config-path" <> metavar "PATH" <> showDefault <> value "/data/store/logdevice.conf" <> help "logdevice config path")
    <*> option auto (long "replicate-factor" <> metavar "INT" <> showDefault <> value 3 <> short 'f' <> help "topic replicate factor")

app :: ServerConfig -> IO ()
app ServerConfig{..} = do
  let options = defaultServiceOptions
                { serverHost = Host . toByteString . toBytes $ _serverHost
                , serverPort = Port . fromIntegral $ _serverPort
                }
  api <- handlers _logdeviceConfigPath
  print _logdeviceConfigPath
  hstreamApiServer api options

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Server")
  print "HStream Server"
  app config
