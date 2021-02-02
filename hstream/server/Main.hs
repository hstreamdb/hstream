{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           HStream.Server.Handler   (app)
import           HStream.Server.Type
import           Network.Wai.Handler.Warp (run)
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, metavar,
                                           option, progDesc, short, showDefault,
                                           strOption, value, (<**>))

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> option auto (long "port" <> metavar "INT" <> showDefault <> value 8081 <> short 'p' <> help "port value")
    <*> strOption (long "logdevice" <> metavar "Path" <> showDefault <> value "/data/store/logdevice.conf" <> short 'l' <> help "logdevice path")
    <*> option auto (long "replicationFactor" <> metavar "INT" <> showDefault <> value 3 <> short 'r' <> help "topic replication Factor :")

main :: IO ()
main = do
  putStrLn "Start Hstream-Server!"
  c@ServerConfig {..} <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "start hstream-server")
  app c >>= run serverPort
