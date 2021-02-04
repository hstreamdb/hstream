{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           HStream.Server.Handler (app)
import           HStream.Server.Type
import           Options.Applicative    (Parser, auto, execParser, fullDesc,
                                         help, helper, info, long, metavar,
                                         option, progDesc, short, showDefault,
                                         strOption, value, (<**>))

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> option auto (long "port" <> metavar "INT" <> showDefault <> value 8081 <> short 'p' <> help "server port value")
    <*> strOption (long "logdevice" <> metavar "PATH" <> showDefault <> value "/data/store/logdevice.conf" <> short 'l' <> help "logdevice path")
    <*> option auto (long "replicationFactor" <> metavar "INT" <> showDefault <> value 3 <> short 'r' <> help "topic replication factor")
    <*> option auto (long "consumerBufferSize" <> metavar "INT" <> showDefault <> value (-1) <> short 's' <> help "consumer buffer size")

main :: IO ()
main = do
  putStrLn "Start HStream-Server!"
  (execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "start hstream-server")) >>= app
