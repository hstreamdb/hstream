{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Exception
import qualified Data.Map.Strict                  as Map
import           Network.GRPC.HighLevel.Generated
import           Options.Applicative
import           Text.RawString.QQ                (r)
import           Z.Data.CBytes                    (CBytes, toBytes)
import           Z.Foreign                        (toByteString)
import           Z.IO.Network
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           HStream.Server.Persistence
import           HStream.Store                    (Compression (..),
                                                   HsLogAttrs (..), LDClient,
                                                   LogAttrs (..),
                                                   initCheckpointStoreLogID,
                                                   newLDClient,
                                                   setupSigsegvHandler)

data ServerConfig = ServerConfig
  { _serverHost          :: CBytes
  , _serverPort          :: PortNumber
  , _persistent          :: Bool
  , _zkHost              :: CBytes
  , _logdeviceConfigPath :: CBytes
  , _topicRepFactor      :: Int
  , _ckpRepFactor        :: Int
  , _compression         :: Compression
  } deriving (Show)

parseConfig :: Parser ServerConfig
parseConfig =
  ServerConfig
    <$> strOption ( long "host" <> metavar "HOST"
                 <> showDefault <> value "127.0.0.1"
                 <> help "server host value"
                  )
    <*> option auto ( long "port" <> short 'p' <> metavar "INT"
                   <> showDefault <> value 6570
                   <> help "server port value"
                    )
    <*> flag False True ( long "persistent"
                       <> help "set flag to store queries in zookeeper"
                        )
    <*> strOption ( long "zkhost" <> metavar "STR"
                 <> showDefault
                 <> value "127.0.0.1:2181"
                 <> help ( "comma separated host:port pairs, each corresponding"
                      <> "to a zk zookeeper server, only meaningful when"
                      <> "persistent flag is set. "
                      <> "e.g. \"127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183\""
                         )
                  )
    <*> strOption ( long "config-path" <> metavar "PATH"
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
    <*> option auto ( long "compression" <> metavar "none|lz4|lz4hc"
                   <> showDefault <> value CompressionLZ4
                   <> help "Specify the compression policy for gdevice"
                    )

app :: ServerConfig -> IO ()
app config@ServerConfig{..} = do
  setupSigsegvHandler
  ldclient <- newLDClient _logdeviceConfigPath
  _ <- initCheckpointStoreLogID ldclient (LogAttrs $ HsLogAttrs _ckpRepFactor Map.empty)
  if _persistent then withResource (defaultHandle _zkHost) $
    \zk -> initZooKeeper zk >> app' config ldclient (Just zk)
  else app' config ldclient Nothing

app' :: ServerConfig -> LDClient -> Maybe ZHandle -> IO ()
app' ServerConfig{..} ldclient zk = do
  let options = defaultServiceOptions
                { serverHost = Host . toByteString . toBytes $ _serverHost
                , serverPort = Port . fromIntegral $ _serverPort
                }
  api <- handlers ldclient _topicRepFactor zk _compression
  print _logdeviceConfigPath
  hstreamApiServer api options

initZooKeeper :: ZHandle -> IO ()
initZooKeeper zk = catch (initializeAncestors zk) (\(_ :: ZNODEEXISTS) -> pure ())

main :: IO ()
main = do
  config <- execParser $ info (parseConfig <**> helper) (fullDesc <> progDesc "HStream-Server")
  putStrLn [r|
   _  _   __ _____ ___ ___  __  __ __
  | || |/' _/_   _| _ \ __|/  \|  V  |
  | >< |`._`. | | | v / _|| /\ | \_/ |
  |_||_||___/ |_| |_|_\___|_||_|_| |_|

  |]
  app config
