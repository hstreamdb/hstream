{-# LANGUAGE ApplicativeDo     #-}
{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}

module HStream.Server.Config
  ( ServerOpts (..)
  , CliOptions (..)
  , TlsConfig (..)
  , AdvertisedListeners
  , advertisedListenersToPB
  , getConfig
  , MetaStoreAddr(..)
  , parseJSONToOptions
  , readProtocol
  ) where

import           Control.Exception              (throwIO)
import           Control.Monad                  (when)
import qualified Data.Attoparsec.Text           as AP
import           Data.Bifunctor                 (second)
import           Data.ByteString                (ByteString)
import qualified Data.ByteString.Char8          as BSC
import qualified Data.HashMap.Strict            as HM
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           Data.Maybe                     (fromMaybe)
import           Data.Text                      (Text)
import qualified Data.Text                      as T
import qualified Data.Text                      as Text
import           Data.Text.Encoding             (encodeUtf8)
import           Data.Vector                    (Vector)
import           Data.Word                      (Word16, Word32)
import           Data.Yaml                      as Y (Object,
                                                      ParseException (..),
                                                      Parser, decodeFileThrow,
                                                      parseEither, (.!=), (.:),
                                                      (.:?))
import           Options.Applicative            as O (Alternative ((<|>)),
                                                      CompletionResult (execCompletion),
                                                      Parser,
                                                      ParserResult (CompletionInvoked, Failure, Success),
                                                      auto, defaultPrefs,
                                                      execParserPure, flag,
                                                      fullDesc, help, helper,
                                                      info, long, maybeReader,
                                                      metavar, option, optional,
                                                      progDesc, renderFailure,
                                                      short, showDefault,
                                                      strOption, value, (<**>))
import           System.Directory               (makeAbsolute)
import           System.Environment             (getArgs, getProgName)
import           System.Exit                    (exitSuccess)
import qualified Z.Data.CBytes                  as CB
import           Z.Data.CBytes                  (CBytes)

import qualified HStream.Admin.Store.API        as AA
import           HStream.Gossip                 (GossipOpts (..),
                                                 defaultGossipOpts)
import qualified HStream.IO.Types               as IO
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as SAI
import           HStream.Store                  (Compression (..))
import qualified HStream.Store.Logger           as Log


-------------------------------------------------------------------------------

data TlsConfig = TlsConfig
  { keyPath  :: String
  , certPath :: String
  , caPath   :: Maybe String
  } deriving (Show, Eq)

type AdvertisedListeners = Map Text (Vector SAI.Listener)

advertisedListenersToPB :: AdvertisedListeners -> Map Text (Maybe SAI.ListOfListener)
advertisedListenersToPB = Map.map $ Just . SAI.ListOfListener

data MetaStoreAddr
  = ZkAddr CBytes
  | RqAddr Text
  deriving (Eq)

data ServerOpts = ServerOpts
  { _serverHost                :: !CBytes
  , _serverPort                :: !Word16
  , _serverInternalPort        :: !Word16
  , _serverAddress             :: !String
  , _serverAdvertisedListeners :: !AdvertisedListeners
  , _serverID                  :: !Word32
  , _metaStore                 :: !MetaStoreAddr
  , _ldConfigPath              :: !CBytes
  , _topicRepFactor            :: !Int
  , _ckpRepFactor              :: !Int
  , _compression               :: !Compression
  , _maxRecordSize             :: !Int
  , _tlsConfig                 :: !(Maybe TlsConfig)
  , _serverLogLevel            :: !Log.Level
  , _serverLogWithColor        :: !Bool
  , _seedNodes                 :: ![(ByteString, Int)]
  , _ldAdminHost               :: !ByteString
  , _ldAdminPort               :: !Int
  , _ldAdminProtocolId         :: !AA.ProtocolId
  , _ldAdminConnTimeout        :: !Int
  , _ldAdminSendTimeout        :: !Int
  , _ldAdminRecvTimeout        :: !Int
  , _ldLogLevel                :: !Log.LDLogLevel

  , _gossipOpts                :: !GossipOpts
  , _ioOptions                 :: !IO.IOOptions
  , _connectorMetaStore        :: !MetaStoreAddr
  } deriving (Show, Eq)

getConfig :: IO ServerOpts
getConfig = do
  args <- getArgs
  case parseCliOptions args of
    Success opts@CliOptions{..} -> do
      path <- makeAbsolute _configPath
      jsonCfg <- decodeFileThrow path
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

-------------------------------------------------------------------------------

data CliOptions = CliOptions
  { _configPath          :: !String
  , _serverHost_         :: !(Maybe CBytes)
  , _serverPort_         :: !(Maybe Word16)
  , _serverAddress_      :: !(Maybe String)
  , _serverInternalPort_ :: !(Maybe Word16)
  , _serverID_           :: !(Maybe Word32)
  , _serverLogLevel_     :: !(Maybe Log.Level)
  , _serverLogWithColor_ :: !Bool
  , _compression_        :: !(Maybe Compression)
  , _metaStore_          :: !(Maybe MetaStoreAddr)
  , _seedNodes_          :: !(Maybe Text)

  , _enableTls_          :: !Bool
  , _tlsKeyPath_         :: !(Maybe String)
  , _tlsCertPath_        :: !(Maybe String)
  , _tlsCaPath_          :: !(Maybe String)

  , _ldAdminHost_        :: !(Maybe ByteString)
  , _ldAdminPort_        :: !(Maybe Int)
  , _ldLogLevel_         :: !(Maybe Log.LDLogLevel)
  , _storeConfigPath     :: !CBytes

  , _ioTasksPath_        :: !(Maybe Text)
  , _ioTasksNetwork_     :: !(Maybe Text)
  }
  deriving Show

parseCliOptions :: [String] -> ParserResult CliOptions
parseCliOptions = execParserPure defaultPrefs $
  info (cliOptionsParser <**> helper) (fullDesc <> progDesc "HStream-Server")

cliOptionsParser :: O.Parser CliOptions
cliOptionsParser = do
  _configPath          <- configPath
  _serverHost_         <- optional serverHost
  _serverAddress_      <- optional serverAddress
  _serverPort_         <- optional serverPort
  _serverInternalPort_ <- optional serverInternalPort
  _seedNodes_          <- optional seedNodes
  _serverID_           <- optional serverID
  _ldAdminPort_        <- optional ldAdminPort
  _ldAdminHost_        <- optional ldAdminHost
  _ldLogLevel_         <- optional ldLogLevel
  _metaStore_          <- optional metaStore
  _serverLogLevel_     <- optional logLevel
  _compression_        <- optional compression
  _serverLogWithColor_ <- logWithColor
  _storeConfigPath     <- storeConfigPath
  _enableTls_          <- enableTls
  _tlsKeyPath_         <- optional tlsKeyPath
  _tlsCertPath_        <- optional tlsCertPath
  _tlsCaPath_          <- optional tlsCaPath
  _ioTasksPath_        <- optional ioTasksPath
  _ioTasksNetwork_     <- optional ioTasksNetwork
  return CliOptions {..}

parseJSONToOptions :: CliOptions -> Y.Object -> Y.Parser ServerOpts
parseJSONToOptions CliOptions {..} obj = do
  nodeCfgObj  <- obj .: "hserver"

  nodeId              <- nodeCfgObj .:  "id"
  nodePort            <- nodeCfgObj .:? "port" .!= 6570
  nodeAddress         <- nodeCfgObj .:  "address"
  nodeInternalPort    <- nodeCfgObj .:? "internal-port" .!= 6571
  advertisedListeners <- nodeCfgObj .:? "advertised-listeners"

  nodeMetaStore     <- parseMetaStoreAddr <$> nodeCfgObj .:  "metastore-uri" :: Y.Parser MetaStoreAddr
  serverCompression <- read <$> nodeCfgObj .:? "compression" .!= "lz4"
  nodeLogLevel      <- nodeCfgObj .:? "log-level" .!= "info"
  nodeLogWithColor  <- nodeCfgObj .:? "log-with-color" .!= True
  -- TODO: For the max_record_size to work properly, we should also tell user
  -- to set payload size for gRPC and LD.
  _maxRecordSize    <- nodeCfgObj .:? "max-record-size" .!= 1048576
  when (_maxRecordSize < 0 && _maxRecordSize > 1048576)
    $ errorWithoutStackTrace "max-record-size has to be a positive number less than 1MB"

  let _serverID           = fromMaybe nodeId _serverID_
  let _serverHost         = fromMaybe "0.0.0.0" _serverHost_
  let _serverPort         = fromMaybe nodePort _serverPort_
  let _serverInternalPort = fromMaybe nodeInternalPort _serverInternalPort_
  let _serverAddress      = fromMaybe nodeAddress _serverAddress_
  let _serverAdvertisedListeners = fromMaybe Map.empty advertisedListeners

  let _metaStore          = fromMaybe nodeMetaStore _metaStore_
  let _serverLogLevel     = fromMaybe (read nodeLogLevel) _serverLogLevel_
  let _serverLogWithColor = nodeLogWithColor || _serverLogWithColor_
  let _compression        = fromMaybe serverCompression _compression_

  -- Cluster Option
  seeds <- flip fromMaybe _seedNodes_ <$> (nodeCfgObj .: "seed-nodes")
  let !_seedNodes = case parseHostPorts seeds of
        Left err -> errorWithoutStackTrace err
        Right hps -> map (second . fromMaybe $ fromIntegral _serverInternalPort) hps

  clusterCfgObj <- nodeCfgObj .:? "gossip" .!= mempty
  gossipFanout     <- clusterCfgObj .:? "gossip-fanout"     .!= gossipFanout defaultGossipOpts
  retransmitMult   <- clusterCfgObj .:? "retransmit-mult"   .!= retransmitMult defaultGossipOpts
  gossipInterval   <- clusterCfgObj .:? "gossip-interval"   .!= gossipInterval defaultGossipOpts
  probeInterval    <- clusterCfgObj .:? "probe-interval"    .!= probeInterval defaultGossipOpts
  roundtripTimeout <- clusterCfgObj .:? "roundtrip-timeout" .!= roundtripTimeout defaultGossipOpts
  joinWorkerConcurrency <- clusterCfgObj .:? "join-worker-concurrency" .!= joinWorkerConcurrency defaultGossipOpts
  let _gossipOpts = GossipOpts {..}

  -- Store Config
  storeCfgObj         <- obj .:? "hstore" .!= mempty
  storeLogLevel       <- read <$> storeCfgObj .:? "log-level" .!= "info"
  sAdminCfgObj        <- storeCfgObj .:? "store-admin" .!= mempty
  storeAdminHost      <- BSC.pack <$> sAdminCfgObj .:? "host" .!= "127.0.0.1"
  storeAdminPort      <- sAdminCfgObj .:? "port" .!= 6440
  _ldAdminProtocolId  <- readProtocol <$> sAdminCfgObj .:? "protocol-id" .!= "binary"
  _ldAdminConnTimeout <- sAdminCfgObj .:? "conn-timeout" .!= 5000
  _ldAdminSendTimeout <- sAdminCfgObj .:? "send-timeout" .!= 5000
  _ldAdminRecvTimeout <- sAdminCfgObj .:? "recv-timeout" .!= 5000

  let _ldAdminHost    = fromMaybe storeAdminHost _ldAdminHost_
  let _ldAdminPort    = fromMaybe storeAdminPort _ldAdminPort_
  let _ldConfigPath   = _storeConfigPath
  let _ldLogLevel     = fromMaybe storeLogLevel  _ldLogLevel_
  let _topicRepFactor = 1
  let _ckpRepFactor   = 3

  -- TLS config
  nodeEnableTls   <- nodeCfgObj .:? "enable-tls" .!= False
  nodeTlsKeyPath  <- nodeCfgObj .:? "tls-key-path"
  nodeTlsCertPath <- nodeCfgObj .:? "tls-cert-path"
  nodeTlsCaPath   <- nodeCfgObj .:? "tls-ca-path"
  let _enableTls   = _enableTls_ || nodeEnableTls
      _tlsKeyPath  = _tlsKeyPath_  <|> nodeTlsKeyPath
      _tlsCertPath = _tlsCertPath_ <|> nodeTlsCertPath
      _tlsCaPath   = _tlsCaPath_   <|> nodeTlsCaPath
      !_tlsConfig  = case (_enableTls, _tlsKeyPath, _tlsCertPath) of
        (False, _, _) -> Nothing
        (_, Nothing, _) -> errorWithoutStackTrace "enable-tls=true, but tls-key-path is empty"
        (_, _, Nothing) -> errorWithoutStackTrace "enable-tls=true, but tls-cert-path is empty"
        (_, Just kp, Just cp) -> Just $ TlsConfig kp cp _tlsCaPath


  -- hstream io config
  -- FIXME: connector meta store should be part of ioOpts
  _connectorMetaStore <- parseMetaStoreAddr <$> nodeCfgObj .:?  "connector-meta-store" .!= T.pack (show nodeMetaStore)
  case _connectorMetaStore of
    ZkAddr _ -> return ()
    _ -> error "Currently only support connectors with zookeeper meta store"
  nodeIOCfg <- nodeCfgObj .:? "hstream-io" .!= mempty
  nodeIOTasksPath <- nodeIOCfg .:? "tasks-path" .!= "/tmp/io/tasks"
  nodeIOTasksNetwork <- nodeIOCfg .:? "tasks-network" .!= "host"
  optSourceImages <- nodeIOCfg .:? "source-images" .!= HM.empty
  optSinkImages <- nodeIOCfg .:? "sink-images" .!= HM.empty
  let optTasksPath = fromMaybe nodeIOTasksPath _ioTasksPath_
      optTasksNetwork = fromMaybe nodeIOTasksNetwork _ioTasksNetwork_
      _ioOptions = IO.IOOptions {..}

  return ServerOpts {..}

-------------------------------------------------------------------------------

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

serverPort :: O.Parser Word16
serverPort = option auto
  $  long "port" <> short 'p'
  <> metavar "INT"
  <> help "server port value"

serverAddress :: O.Parser String
serverAddress = strOption
  $  long "address"
  <> metavar "ADDRESS"
  <> help "server address"

serverInternalPort :: O.Parser Word16
serverInternalPort = option auto
  $ long "internal-port"
  <> metavar "INT"
  <> help "server channel port value for internal communication"

serverID :: O.Parser Word32
serverID = option auto
  $ long "server-id"
  <> metavar "UINT32"
  <> help "ID of the hstream server node"

seedNodes :: O.Parser Text
seedNodes = strOption
  $  long "seed-nodes"
  <> metavar "ADDRESS"
  <> help "host:port pairs of seed nodes, separated by commas (,)"

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

metaStore :: O.Parser MetaStoreAddr
metaStore = option (O.maybeReader (Just . parseMetaStoreAddr . T.pack))
  $  long "metastore-uri"
  <> metavar "STR"
  <> help ( "Meta store address, currently support zookeeper and rqlite"
         <> "such as \"zk://127.0.0.1:2181,127.0.0.1:2182 , \"rq://127.0.0.1:4001\"")

--TODO: This option will be removed once we can get config from admin server.
storeConfigPath :: O.Parser CBytes
storeConfigPath = strOption
  $  long "store-config"
  <> metavar "PATH" <> value "/data/store/logdevice.conf"
  <> help "Storage config path"

enableTls :: O.Parser Bool
enableTls = flag False True
  $  long "enable-tls"
  <> help "Enable tls, require tls-key-path, tls-cert-path options"

tlsKeyPath :: O.Parser String
tlsKeyPath = strOption
  $  long "tls-key-path"
  <> metavar "PATH"
  <> help "TLS key path"

tlsCertPath :: O.Parser String
tlsCertPath = strOption
  $  long "tls-cert-path"
  <> metavar "PATH"
  <> help "Signed certificate path"

tlsCaPath :: O.Parser String
tlsCaPath = strOption
  $  long "tls-ca-path"
  <> metavar "PATH"
  <> help "Trusted CA(Certificate Authority) path"

ioTasksPath :: O.Parser Text
ioTasksPath = strOption
  $  long "io-tasks-path"
  <> metavar "PATH"
  <> help "io tasks path"

ioTasksNetwork :: O.Parser Text
ioTasksNetwork = strOption
  $  long "io-tasks-network"
  <> metavar "STR"
  <> help "io tasks network"

readProtocol :: Text.Text -> AA.ProtocolId
readProtocol x = case (Text.strip . Text.toUpper) x of
  "binary"  -> AA.binaryProtocolId
  "compact" -> AA.compactProtocolId
  _         -> AA.binaryProtocolId

parseHostPorts :: Text -> Either String [(ByteString, Maybe Int)]
parseHostPorts = AP.parseOnly (hostPortParser `AP.sepBy` AP.char ',')
  where
    hostPortParser = do
      AP.skipSpace
      host <- encodeUtf8 <$> AP.takeTill (`elem` [':', ','])
      port <- optional (AP.char ':' *> AP.decimal)
      return (host, port)

parseMetaStoreAddr :: Text -> MetaStoreAddr
parseMetaStoreAddr t =
  case AP.parseOnly metaStoreP t of
    Right (s, ip)
      | s == "zk" -> ZkAddr . CB.pack .T.unpack $ ip
      | s == "rq" -> RqAddr ip
      | otherwise -> errorWithoutStackTrace $ "Invalid meta store address, unsupported scheme: " <> show s
    Left eMsg -> errorWithoutStackTrace eMsg

metaStoreP :: AP.Parser (Text, Text)
metaStoreP = do
  scheme <- AP.takeTill (== ':')
  AP.string "://"
  ip <- AP.takeText
  return (scheme, ip)

-- TODO: Haskell libraries does not support the case where multiple auths exist
-- case parseURI str of
-- Just URI{..} -> case uriAuthority of
--   Just URIAuth{..}
--     | uriScheme == "zk:" -> ZkAddr . CB.pack $ uriRegName <> uriPort
--     | uriScheme == "rq:" -> RqAddr . T.pack $ uriRegName <> uriPort
--     | otherwise -> errorWithoutStackTrace $ "Invalid meta store address, unsupported scheme: " <> uriScheme
--   Nothing -> errorWithoutStackTrace $ "Invalid meta store address, no Auth: " <> str
-- Nothing  -> errorWithoutStackTrace $ "Invalid meta store address, no parse: " <> str

instance Show MetaStoreAddr where
  show (ZkAddr addr) = "zk://" <> CB.unpack addr
  show (RqAddr addr) = "rq://" <> T.unpack addr
