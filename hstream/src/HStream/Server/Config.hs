{-# LANGUAGE ApplicativeDo #-}

module HStream.Server.Config
  ( getConfig
  ) where

import           Control.Exception          (throwIO)
import           Data.ByteString            (ByteString)
import qualified Data.ByteString.Char8      as BSC
import           Data.Maybe                 (fromMaybe)
import qualified Data.Text                  as Text
import           Data.Word                  (Word32)
import           Data.Yaml                  as Y
import           Options.Applicative        as O
import           System.Directory           (makeAbsolute)
import           System.Environment         (getArgs, getProgName)
import           System.Exit                (exitSuccess)
import           Z.Data.CBytes              (CBytes)
import           Z.IO.Network               (PortNumber (PortNumber))


import qualified HStream.Logger             as Log
import           HStream.Server.Persistence ()
import           HStream.Server.Types       (ServerOpts (..))
import           HStream.Store              (Compression (CompressionLZ4))
import qualified HStream.Store.Admin.API    as AA
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
  , _ldAdminHost_        :: Maybe ByteString
  , _ldAdminPort_        :: Maybe Int
  , _ldLogLevel_         :: Maybe Log.LDLogLevel
  , _zkUri_              :: Maybe CBytes
  , _storeConfigPath     :: CBytes
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

parseWithFile :: O.Parser CliOptions
parseWithFile = do
  _configPath <- configPath
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
  _serverLogWithColor_ <- logWithColor
  _storeConfigPath     <- storeConfigPath
  return CliOptions {..}

parseFileToJSON :: FilePath -> IO Y.Object
parseFileToJSON = decodeFileThrow

parseJSONToOptions :: CliOptions -> Y.Object -> Y.Parser ServerOpts
parseJSONToOptions CliOptions {..} obj = do
  nodeCfgObj  <- obj .: "hserver"
  nodeId      <- nodeCfgObj .:  "id"
  nodeAddress <- nodeCfgObj .:  "address"
  nodePort    <- nodeCfgObj .:? "port" .!= 6570
  nodeInternalPort <- nodeCfgObj .:? "internal-port" .!= 6570
  zkuri            <- nodeCfgObj .:  "zkuri"
  nodeLogLevel     <- nodeCfgObj .:? "log-level" .!= "info"
  nodeLogWithColor <- nodeCfgObj .:? "log-with-color" .!= True

  let _serverPort    = fromMaybe (PortNumber nodePort) _serverPort_
  let _serverID      = fromMaybe nodeId _serverID_
  let _serverInternalPort = fromMaybe (PortNumber nodeInternalPort) _serverInternalPort_
  let _zkUri              = fromMaybe zkuri _zkUri_
  let _serverLogLevel     = fromMaybe (read nodeLogLevel) _serverLogLevel_
  let _serverLogWithColor = nodeLogWithColor || _serverLogWithColor_
  let _serverAddress      = fromMaybe nodeAddress _serverAddress_

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
  let _topicRepFactor = 3
  let _compression    = CompressionLZ4
  let _ckpRepFactor   = 1
  -- TODO: remove the following 2 options
  let _serverHost     = fromMaybe "0.0.0.0" _serverHost_
  let _ldConfigPath   = _storeConfigPath

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
