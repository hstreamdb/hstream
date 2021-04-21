{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}

module Main where

import           Options.Applicative           (Parser, auto, execParser,
                                                fullDesc, help, helper, info,
                                                long, metavar, option, progDesc,
                                                short, showDefault, strOption,
                                                value, (<**>))
import           Text.RawString.QQ             (r)
import           Z.Data.CBytes                 (CBytes)
import           Z.IO.Network                  (PortNumber,
                                                defaultTCPServerConfig, ipv4,
                                                tcpListenAddr)

import qualified HStream.Store.RPC.MessagePack as RPC

data ServerConfig = ServerConfig
  { serverHost :: CBytes
  , serverPort :: PortNumber
  }

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> strOption (long "host" <> metavar "HOST" <> showDefault <> value "127.0.0.1" <> help "server host value")
    <*> option auto (long "port" <> metavar "INT" <> showDefault <> value 6567 <> short 'p' <> help "server port value")

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Store")
  putStrLn $ [r|
       _         _
    | |       | |
    | |__  ___| |_ ___  _ __ ___
    | '_ \/ __| __/ _ \| '__/ _ \
    | | | \__ \ || (_) | | |  __/
    |_| |_|___/\__\___/|_|  \___|

    |]
  app config

app :: ServerConfig -> IO ()
app ServerConfig{..} = do
  let listenAddr = ipv4 serverHost serverPort
  putStrLn $ "Listening " <> show listenAddr
  RPC.serve defaultTCPServerConfig{tcpListenAddr=listenAddr}
