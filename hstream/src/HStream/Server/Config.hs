{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE BangPatterns  #-}

module HStream.Server.Config
  ( getConfig
  ) where

import           Control.Exception          (throwIO)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Char8      as BSC
import           Data.Maybe                 (fromMaybe, isJust)
import qualified Data.Text                  as Text
import           Data.Word                  (Word32)
import           Data.Yaml                  as Y
import           Options.Applicative        as O
import           System.Directory           (makeAbsolute)
import           System.Environment         (getArgs, getProgName)
import           System.Exit                (exitSuccess)
import           Z.Data.CBytes              (CBytes)
import           Z.IO.Network               (PortNumber (PortNumber))

import qualified HStream.Admin.Store.API    as AA
import qualified HStream.Logger             as Log
import           HStream.Server.Persistence ()
import           HStream.Server.Types       (ServerOpts (..),
                                             TlsConfig (TlsConfig))
import           HStream.Store              (Compression (..))
import qualified HStream.Store.Logger       as Log

data CliOptions = CliOptions
  { _configPath          :: String
  , _serverHost_         :: Maybe CBytes
  , _serverPort_         :: Maybe PortNumber
  , _serverAddress_      :: Maybe String
  , _serverInternalPort_ :: Maybe PortNumber
  , _serverID_           :: Maybe Word32
  , _serverLogLevel_     :: Maybe Log.Level
  , _serverLogWithColor_ :: Bool
  , _compression_        :: Maybe Compression
  , _ldAdminHost_        :: Maybe ByteString
  , _ldAdminPort_        :: Maybe Int
  , _ldLogLevel_         :: Maybe Log.LDLogLevel
  , _zkUri_              :: Maybe CBytes
  , _storeConfigPath     :: CBytes
  , _enableTls_          :: Bool
  , _tlsKeyPath_         :: Maybe String
  , _tlsCertPath_        :: Maybe String
  , _tlsCaPath_          :: Maybe String
  }
  deriving Show

configPath :: O.Parser String
configPath = strOption
  $  long "config-path"
  <> metavar "PATH" <> value "/etc/hstream/config.yaml"
  <> help "hstream config path"

-- TODO: This option will be removed
serverHost :: O.Parser CBytes
serverHost = strOption
  $ long "host" <> metavar "HOST"
  <> showDefault
  <> help "server host value"

serverPort :: O.Parser PortNumber
serverPort = option auto
  $  long "port" <> short 'p'
  <> metavar "INT"
  <> help "server port value"

serverAddress :: O.Parser String
serverAddress = strOption
  $  long "address"
  <> metavar "ADDRESS"
  <> help "server address"

serverInternalPort :: O.Parser PortNumber
serverInternalPort = option auto
  $ long "internal-port"
  <> metavar "INT"
  <> help "server channel port value for internal communication"

serverID :: O.Parser Word32
serverID = option auto
  $ long "server-id"
  <> metavar "UINT32"
  <> help "ID of the hstream server node"

compression :: O.Parser Compression
compression = option auto
  $ long "compression"
  <> metavar "none | lz4 | lz4hc"
  <> help "Compression option when write records to store"

logLevel :: O.Parser Log.Level
logLevel = option auto
  $ long "log-level"
  <> metavar "[critical|fatal|warning|info|debug]"
  <> help "Server log level"

logWithColor :: O.Parser Bool
logWithColor = flag False True
  $  long "log-with-color"
  <> help "Server log with color"

ldAdminPort :: O.Parser Int
ldAdminPort = option auto
  $  long "store-admin-port"
  <> metavar "INT"
  <> help "Store admin port value"

ldAdminHost :: O.Parser ByteString
ldAdminHost = strOption
  $  long "store-admin-host" <> metavar "HOST"
  <> help "Store admin host"

ldLogLevel :: O.Parser Log.LDLogLevel
ldLogLevel = option auto
  $  long "store-log-level"
  <> metavar "[critical|error|warning|notify|info|debug|spew]"
  <> help "Store log level"

zkUri :: O.Parser CBytes
zkUri = strOption
  $  long "zkuri"
  <> metavar "STR"
  <> help ( "comma separated host:port pairs, each corresponding"
         <> "to a zk zookeeper server. "
         <> "e.g. \"127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183\"")

--TODO: This option will be removed once we can get config from admin server.
storeConfigPath :: O.Parser CBytes
storeConfigPath = strOption
  $  long "store-config"
  <> metavar "PATH" <> value "/data/store/logdevice.conf"
  <> help "Storage config path"

enableTls :: O.Parser Bool
enableTls = flag False True
  $  long "enable-tls"
  <> help "enable tls, require tls-key-path, tls-cert-path options"

tlsKeyPath :: O.Parser String
tlsKeyPath = strOption
  $  long "tls-key-path"
  <> metavar "PATH"
  <> help "private key path"

tlsCertPath :: O.Parser String
tlsCertPath = strOption
  $  long "tls-cert-path"
  <> metavar "PATH"
  <> help "signed certificate path"

tlsCaPath :: O.Parser String
tlsCaPath = strOption
  $  long "tls-ca-path"
  <> metavar "PATH"
  <> help "trusted CA(Certificate Authority) path"

parseWithFile :: O.Parser CliOptions
parseWithFile = do
  _configPath          <- configPath
  _serverHost_         <- optional serverHost
  _serverPort_         <- optional serverPort
  _serverAddress_      <- optional serverAddress
  _serverInternalPort_ <- optional serverInternalPort
  _serverID_           <- optional serverID
  _ldAdminPort_        <- optional ldAdminPort
  _ldAdminHost_        <- optional ldAdminHost
  _ldLogLevel_         <- optional ldLogLevel
  _zkUri_              <- optional zkUri
  _serverLogLevel_     <- optional logLevel
  _compression_        <- optional compression
  _serverLogWithColor_ <- logWithColor
  _storeConfigPath     <- storeConfigPath
  _enableTls_          <- enableTls
  _tlsKeyPath_         <- optional tlsKeyPath
  _tlsCertPath_        <- optional tlsCertPath
  _tlsCaPath_          <- optional tlsCaPath
  return CliOptions {..}

parseFileToJSON :: FilePath -> IO Y.Object
parseFileToJSON = decodeFileThrow

parseJSONToOptions :: CliOptions -> Y.Object -> Y.Parser ServerOpts
parseJSONToOptions CliOptions {..} obj = do
  nodeCfgObj  <- obj .: "hserver"
  nodeId      <- nodeCfgObj .:  "id"
  nodeAddress <- nodeCfgObj .:  "address"
  nodePort    <- nodeCfgObj .:? "port" .!= 6570
  nodeInternalPort <- nodeCfgObj .:? "internal-port" .!= 6571
  zkuri            <- nodeCfgObj .:  "zkuri"
  recordCompression   <- read <$> nodeCfgObj .:? "compression" .!= "lz4"
  nodeLogLevel     <- nodeCfgObj .:? "log-level" .!= "info"
  nodeLogWithColor <- nodeCfgObj .:? "log-with-color" .!= True

  let _serverPort    = fromMaybe (PortNumber nodePort) _serverPort_
  let _serverID      = fromMaybe nodeId _serverID_
  let _serverInternalPort = fromMaybe (PortNumber nodeInternalPort) _serverInternalPort_
  let _zkUri              = fromMaybe zkuri _zkUri_
  let _serverLogLevel     = fromMaybe (read nodeLogLevel) _serverLogLevel_
  let _serverLogWithColor = nodeLogWithColor || _serverLogWithColor_
  let _serverAddress      = fromMaybe nodeAddress _serverAddress_
  let _compression        = fromMaybe recordCompression _compression_

  storeCfgObj  <- obj .:? "hstore" .!= mempty
  storeLogLevel <- read <$> storeCfgObj .:? "log-level" .!= "info"
  sAdminCfgObj <- storeCfgObj .:? "store-admin" .!= mempty
  storeAdminHost      <- BSC.pack <$> sAdminCfgObj .:? "host" .!= "127.0.0.1"
  storeAdminPort      <- sAdminCfgObj .:? "port" .!= 6440
  _ldAdminProtocolId  <- readProtocol <$> sAdminCfgObj .:? "protocol-id" .!= "binary"
  _ldAdminConnTimeout <- sAdminCfgObj .:? "conn-timeout" .!= 5000
  _ldAdminSendTimeout <- sAdminCfgObj .:? "send-timeout" .!= 5000
  _ldAdminRecvTimeout <- sAdminCfgObj .:? "recv-timeout" .!= 5000

  let _ldAdminHost    = fromMaybe storeAdminHost _ldAdminHost_
  let _ldAdminPort    = fromMaybe storeAdminPort _ldAdminPort_
  let _ldLogLevel     = fromMaybe storeLogLevel  _ldLogLevel_
  let _topicRepFactor = 1
  let _ckpRepFactor   = 3
  -- TODO: remove the following 2 options
  let _serverHost     = fromMaybe "0.0.0.0" _serverHost_
  let _ldConfigPath   = _storeConfigPath

  -- TLS config
  nodeEnableTls <- nodeCfgObj .:? "enable-tls" .!= False
  nodeTlsKeyPath <- nodeCfgObj .:? "tls-key-path"
  nodeTlsCertPath <- nodeCfgObj .:? "tls-cert-path"
  nodeTlsCaPath <- nodeCfgObj .:? "tls-ca-path"

  let _enableTls = _enableTls_ || nodeEnableTls
  let _firstJust x y = if isJust x then x else y
  let _tlsKeyPath = _firstJust _tlsKeyPath_ nodeTlsKeyPath
  let _tlsCertPath = _firstJust _tlsCertPath_ nodeTlsCertPath
  let _tlsCaPath = _firstJust _tlsCaPath_ nodeTlsCaPath
  let !_tlsConfig = case (_enableTls, _tlsKeyPath, _tlsCertPath) of
                    (False, _, _) -> Nothing
                    (_, Nothing, _) -> error "enable-tls=true, but tls-key-path is empty"
                    (_, _, Nothing) -> error "enable-tls=true, but tls-cert-path is empty"
                    (_, Just kp, Just cp) -> Just $ TlsConfig kp cp _tlsCaPath

  return ServerOpts {..}

getConfig :: IO ServerOpts
getConfig = do
  args <- getArgs
  case parseConfig parseWithFile args of
    Success opts@CliOptions{..} -> do
      path <- makeAbsolute _configPath
      jsonCfg <- parseFileToJSON path
      case parseEither (parseJSONToOptions opts) jsonCfg of
        Left err  -> throwIO (AesonException err)
        Right cfg -> return cfg
    Failure failure -> do
      progn <- getProgName
      let (msg, _) = renderFailure failure progn
      putStrLn msg
      exitSuccess
    CompletionInvoked compl -> handleCompletion compl
  where
    handleCompletion compl = do
      progn <- getProgName
      msg <- execCompletion compl progn
      putStr msg
      exitSuccess
    parseConfig p = execParserPure defaultPrefs
      $ info (p <**> helper) (fullDesc <> progDesc "HStream-Server")

readProtocol :: Text.Text -> AA.ProtocolId
readProtocol x = case (Text.strip . Text.toUpper) x of
  "binary"  -> AA.binaryProtocolId
  "compact" -> AA.compactProtocolId
  _         -> AA.binaryProtocolId
