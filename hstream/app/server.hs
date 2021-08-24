{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

import           Control.Exception
import           Data.ByteString                  (ByteString)
import           Data.Int                         (Int64)
import qualified Data.Map.Strict                  as Map
import           Network.GRPC.HighLevel.Generated
import           Options.Applicative
import           Text.RawString.QQ                (r)
import qualified Z.Data.Builder                   as Builder
import           Z.Data.CBytes                    (CBytes, toBytes)
import qualified Z.Data.CBytes                    as CBytes
import           Z.Foreign                        (toByteString)
import           Z.IO.Network
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Types

import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.Handler
import           HStream.Server.Persistence
import           HStream.Store
import qualified HStream.Store.Admin.API          as AA
import           HStream.Utils                    (setupSigsegvHandler)

-- TODO
-- 1. config file for the Server
-- 2. log options

data ServerOpts = ServerOpts
  { _serverHost         :: CBytes
  , _serverPort         :: PortNumber
  , _persistent         :: Bool
  , _zkUri              :: CBytes
  , _ldConfigPath       :: CBytes
  , _topicRepFactor     :: Int
  , _ckpRepFactor       :: Int
  , _heartbeatTimeout   :: Int64
  , _compression        :: Compression
  , _ldAdminHost        :: ByteString
  , _ldAdminPort        :: Int
  , _ldAdminProtocolId  :: AA.ProtocolId
  , _ldAdminConnTimeout :: Int
  , _ldAdminSendTimeout :: Int
  , _ldAdminRecvTimeout :: Int
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
    <*> strOption   ( long "hadmin-host" <> metavar "HOST"
                   <> showDefault <> value "127.0.0.1" <> help "logdevice admin host"
                    )
    <*> option auto ( long "hadmin-port" <> metavar "INT"
                   <> showDefault <> value 6440 <> help "logdevice admin port"
                    )
    <*> option auto ( long "hadmin-protocol-id" <> metavar "ProtocolId"
                   <> showDefault <> value AA.binaryProtocolId <> help "logdevice admin thrift protocol id"
                    )
    <*> option auto ( long "hadmin-conn-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift connection timeout in milliseconds"
                    )
    <*> option auto ( long "hadmin-send-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift sending timeout in milliseconds"
                    )
    <*> option auto ( long "hadmin-recv-timeout" <> metavar "INT"
                   <> showDefault <> value 5000
                   <> help "logdevice admin thrift receiving timeout in milliseconds"
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
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout
  api <- handlers ldclient headerConfig _topicRepFactor zk _heartbeatTimeout _compression
  Log.i $ "Server started on "
       <> CBytes.toBuilder _serverHost <> ":" <> Builder.int _serverPort
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
  Log.setLogLevel Log.INFO
  app config
