{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Main where

import           Data.Aeson               (FromJSON, ToJSON)
import           GHC.Generics             (Generic)
import           HStream.Server.Handler   (app)
import           Network.Wai.Handler.Warp (run)
import           Options.Applicative      (Parser, auto, execParser, fullDesc,
                                           help, helper, info, long, metavar,
                                           option, progDesc, short, showDefault,
                                           strOption, value, (<**>))

data Config = Config
  { cport :: Int,
    cpath :: String
  }
  deriving (Show, Eq, Generic, FromJSON, ToJSON)

parseConfig :: Parser Config
parseConfig =
  Config
    <$> option auto (long "port" <> metavar "INT" <> showDefault <> value 8081 <> short 'p' <> help "port value")
    <*> strOption (long "logdevice" <> metavar "Path" <> showDefault <> value "/data/store/logdevice.conf" <> short 'l' <> help "logdevice path")

main :: IO ()
main = do
  putStrLn "Start Hstream-Server!"
  Config {..} <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "start hstream-server")
  app cpath >>= run cport
