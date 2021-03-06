{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Exception
import           Data.Int                         (Int64)
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
import           HStream.Store
import qualified HStream.Store.Logger             as Log

-- TODO
-- 1. config file for the Server
-- 2. log options

data ServerOpts = ServerOpts
  { _serverHost       :: CBytes
  , _serverPort       :: PortNumber
  , _persistent       :: Bool
  , _zkUri            :: CBytes
  , _ldConfigPath     :: CBytes
  , _topicRepFactor   :: Int
  , _ckpRepFactor     :: Int
  , _heartbeatTimeout :: Int64
  , _compression      :: Compression
  } deriving (Show)

parseConfig :: Parser ServerOpts
parseConfig =
  ServerOpts
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
    <*> strOption ( long "zkuri" <> metavar "STR"
                 <> showDefault
                 <> value "127.0.0.1:2181"
                 <> help ( "comma separated host:port pairs, each corresponding"
                      <> "to a zk zookeeper server, only meaningful when"
                      <> "persistent flag is set. "
                      <> "e.g. \"127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183\""
                         )
                  )
    <*> strOption ( long "store-config" <> metavar "PATH"
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
    <*> option auto ( long "timeout" <> metavar "INT"
                   <> showDefault <> value 1000
                   <> help "the timer timeout in milliseconds"
                    )
    <*> option auto ( long "compression" <> metavar "none|lz4|lz4hc"
                   <> showDefault <> value CompressionLZ4
                   <> help "Specify the compression policy for gdevice"
                    )

app :: ServerOpts -> IO ()
app config@ServerOpts{..} = do
  setupSigsegvHandler
  ldclient <- newLDClient _ldConfigPath
  _ <- initCheckpointStoreLogID ldclient (LogAttrs $ HsLogAttrs _ckpRepFactor Map.empty)
  if _persistent
     then withResource (defaultHandle _zkUri) $ \zk -> do
       initZooKeeper zk
       serve config ldclient (Just zk)
     else serve config ldclient Nothing

serve :: ServerOpts -> LDClient -> Maybe ZHandle -> IO ()
serve ServerOpts{..} ldclient zk = do
  let options = defaultServiceOptions
                { serverHost = Host . toByteString . toBytes $ _serverHost
                , serverPort = Port . fromIntegral $ _serverPort
                }
  api <- handlers ldclient _topicRepFactor zk _heartbeatTimeout _compression
  Log.i "Server started."
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
