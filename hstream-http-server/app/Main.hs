{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module Main where

import           Control.Monad.IO.Class           (liftIO)
import           Network.GRPC.HighLevel.Generated
import           Network.Wai.Handler.Warp         (run)
import           Options.Applicative              (Parser, auto, execParser,
                                                   fullDesc, help, helper, info,
                                                   long, metavar, option,
                                                   progDesc, short, showDefault,
                                                   strOption, value, (<**>))
import           Servant.Server                   (serve)

import           HStream.HTTP.Server.API          (ServerConfig (..), api,
                                                   apiServer)
import qualified HStream.Store                    as HS

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> strOption   (long "host"                 <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "server host value")
    <*> option auto (long "port"                 <> metavar "INT"  <> showDefault <> value 8000 <> short 'p'            <> help "server port value")
    <*> strOption   (long "config-path"          <> metavar "PATH" <> showDefault <> value "/data/store/logdevice.conf" <> help "logdevice config path")
    <*> strOption   (long "hstream-host"         <> metavar "HOST" <> showDefault <> value "127.0.0.1"                  <> help "hstream grpc server host value")
    <*> option auto (long "hstream-port"         <> metavar "INT"  <> showDefault <> value 6570                         <> help "hstream grpc server port value")

app :: ServerConfig -> IO ()
app ServerConfig{..} = do
  let clientConfig = ClientConfig { clientServerHost = Host _hstreamHost
                            , clientServerPort = Port _hstreamPort
                            , clientArgs = []
                            , clientSSLConfig = Nothing
                            , clientAuthority = Nothing
                            }
  withGRPCClient clientConfig $
    \hClient -> do
      ldClient <- liftIO (HS.newLDClient _logdeviceConfigPath)
      run _serverPort $ serve api $ apiServer ldClient hClient

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Admin-Server")
  app config
