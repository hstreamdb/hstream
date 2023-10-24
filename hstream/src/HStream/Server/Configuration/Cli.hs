{-# LANGUAGE ApplicativeDo     #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}

module HStream.Server.Configuration.Cli
  ( ServerCli (..)
  , serverCliParser
  , runServerCli

  , CliOptions (..)
  , cliOptionsParser

  , AdvertisedListeners
  , ListenersSecurityProtocolMap
  , SecurityProtocolMap
  , MetaStoreAddr (..)
  , TlsConfig (..)
  , ExperimentalFeature (..)

    -- * Helpers
  , advertisedListenersToPB
  , defaultProtocolMap
  , parseMetaStoreAddr
  ) where

import qualified Data.Attoparsec.Text           as AP
import           Data.ByteString                (ByteString)
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           Data.Set                       (Set)
import qualified Data.Set                       as Set
import           Data.Text                      (Text)
import qualified Data.Text                      as T
import qualified Data.Vector                    as V
import           Data.Word                      (Word16, Word32)
import           Options.Applicative            as O (auto, flag, help, long,
                                                      maybeReader, metavar,
                                                      option, optional, short,
                                                      strOption, value, (<**>),
                                                      (<|>))
import qualified Options.Applicative            as O
import           System.Environment             (getProgName)
import           System.Exit                    (exitSuccess)
import qualified Z.Data.CBytes                  as CB
import           Z.Data.CBytes                  (CBytes)

import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as SAI
import           HStream.Store                  (Compression (..))
import qualified HStream.Store.Logger           as Log

-------------------------------------------------------------------------------

data ServerCli
  = Cli CliOptions
  | ShowVersion
  deriving (Show)

serverCliParser :: O.Parser ServerCli
serverCliParser = (Cli <$> cliOptionsParser) <|> showVersionParser

runServerCli :: [String] -> IO ServerCli
runServerCli args =
  case execParser args of
    O.Success serverCli -> pure serverCli
    O.Failure failure -> do
      progn <- getProgName
      let (msg, _) = O.renderFailure failure progn
      putStrLn msg
      exitSuccess
    O.CompletionInvoked compl -> handleCompletion compl
 where
   execParser = O.execParserPure O.defaultPrefs $
     O.info (serverCliParser <**> O.helper) (O.fullDesc <> O.progDesc "HStream-Server")

   handleCompletion compl = do
     progn <- getProgName
     msg <- O.execCompletion compl progn
     putStr msg
     exitSuccess

showVersionParser :: O.Parser ServerCli
showVersionParser = O.flag' ShowVersion
  ( O.long "version" <> O.short 'v' <> O.help "Show server version." )

data CliOptions = CliOptions
  { cliConfigPath                   :: !String

  , cliServerPort                   :: !(Maybe Word16)
  , cliServerBindAddress            :: !(Maybe ByteString)
  , cliServerGossipAddress          :: !(Maybe String)
  , cliServerInternalPort           :: !(Maybe Word16)

  , cliServerAdvertisedAddress      :: !(Maybe String)
  , cliServerAdvertisedListeners    :: !AdvertisedListeners
  , cliListenersSecurityProtocolMap :: !ListenersSecurityProtocolMap

  , cliServerID                     :: !(Maybe Word32)
  , cliMetaStore                    :: !(Maybe MetaStoreAddr)
  , cliSeedNodes                    :: !(Maybe Text)

  , cliServerLogLevel               :: !(Maybe Log.Level)
  , cliServerLogWithColor           :: !Bool
  , cliServerLogFlushImmediately    :: !Bool

    -- TLS config
  , cliEnableTls                    :: !Bool
  , cliTlsKeyPath                   :: !(Maybe String)
  , cliTlsCertPath                  :: !(Maybe String)
  , cliTlsCaPath                    :: !(Maybe String)

  , cliLdAdminHost                  :: !(Maybe ByteString)
  , cliLdAdminPort                  :: !(Maybe Int)
  , cliLdLogLevel                   :: !(Maybe Log.LDLogLevel)
  , cliStoreConfigPath              :: !CBytes
  , cliCkpRepFactor                 :: !(Maybe Int)

  , cliIoTasksPath                  :: !(Maybe Text)
  , cliIoTasksNetwork               :: !(Maybe Text)
  , cliIoConnectorImages            :: ![Text]

  , cliQuerySnapshotPath            :: !(Maybe FilePath)

    -- Experimental Features
  , cliExperimentalFeatures         :: ![ExperimentalFeature]

    -- Internal options
  , cliStoreCompression             :: !(Maybe Compression)
  } deriving Show

cliOptionsParser :: O.Parser CliOptions
cliOptionsParser = do
  cliConfigPath <- configPathParser

  cliServerBindAddress   <- optional bindAddressParser
  cliServerPort          <- optional serverPortParser
  cliServerGossipAddress <- optional serverGossipAddressParser
  cliServerInternalPort  <- optional serverInternalPortParser

  cliServerAdvertisedAddress      <- optional advertisedAddressParser
  cliServerAdvertisedListeners    <- advertisedListenersParser
  cliListenersSecurityProtocolMap <- listenersSecurityProtocolMapParser

  cliSeedNodes          <- optional seedNodesParser
  cliServerID           <- optional serverIDParser
  cliMetaStore          <- optional metaStoreAddrParser

  cliServerLogLevel     <- optional logLevelParser
  cliServerLogWithColor <- logWithColorParser
  cliServerLogFlushImmediately <- logFlushImmediatelyParser

  cliLdAdminPort     <- optional ldAdminPortParser
  cliLdAdminHost     <- optional ldAdminHostParser
  cliLdLogLevel      <- optional ldLogLevelParser
  cliStoreConfigPath <- storeConfigPathParser
  cliCkpRepFactor       <- optional ckpReplicaParser
  cliEnableTls          <- enableTlsParser
  cliTlsKeyPath         <- optional tlsKeyPathParser
  cliTlsCertPath        <- optional tlsCertPathParser
  cliTlsCaPath          <- optional tlsCaPathParser
  cliIoTasksPath        <- optional ioTasksPathParser
  cliIoTasksNetwork     <- optional ioTasksNetworkParser
  cliIoConnectorImages  <- ioConnectorImageParser
  cliQuerySnapshotPath  <- optional querySnapshotPathParser

  cliExperimentalFeatures <- O.many experimentalFeatureParser

  cliStoreCompression <- optional storeCompressionParser

  return CliOptions{..}

-------------------------------------------------------------------------------
-- Parser

type AdvertisedListeners = Map Text (Set SAI.Listener)

parseAdvertisedListeners :: Text -> Either String AdvertisedListeners
parseAdvertisedListeners =
  let parser = do key <- AP.takeTill (== ':')
                  AP.string ":hstream://"
                  address <- AP.takeTill (== ':')
                  AP.char ':'
                  port <- AP.decimal
                  AP.endOfInput
                  return (key, Set.singleton SAI.Listener{ listenerAddress = address, listenerPort = port})
   in (Map.fromListWith Set.union <$>) . AP.parseOnly (parser `AP.sepBy` AP.char ',')

advertisedListenersParser :: O.Parser AdvertisedListeners
advertisedListenersParser = parserOpt parseAdvertisedListeners
   $ long "advertised-listeners"
  <> value mempty
  <> metavar "LISTENERS"
  <> help ( "comma separated advertised listener pairs, in format "
         <> "<listener_key>:hstream://<address>:<port>."
         <> "e.g. public:hstream://your-pub-ip:6570,private:hstream://127.0.0.1:6580"
          )

advertisedListenersToPB :: AdvertisedListeners -> Map Text (Maybe SAI.ListOfListener)
advertisedListenersToPB = Map.map $ Just . SAI.ListOfListener . V.fromList . Set.toList

type ListenersSecurityProtocolMap = Map Text Text

parseListenersSecurityProtocolMap :: Text -> Either String ListenersSecurityProtocolMap
parseListenersSecurityProtocolMap =
  let parser = do
        AP.skipSpace
        host <- AP.takeTill (`elem` [':'])
        port <- AP.char ':' *> AP.takeTill (== ',')
        return (host, port)
   in (Map.fromList <$>) . AP.parseOnly (parser `AP.sepBy` AP.char ',')

listenersSecurityProtocolMapParser :: O.Parser ListenersSecurityProtocolMap
listenersSecurityProtocolMapParser = parserOpt parseListenersSecurityProtocolMap
   $ long "listeners-security-protocol-map"
  <> value mempty
  <> metavar "LISTENER_KEY:SECURITY_KEY"
  <> help ( "listener security, in format <listener_key>:<security_key>. "
         <> "e.g. public:tls,private:plaintext"
          )

data MetaStoreAddr
  = ZkAddr CBytes
  | RqAddr Text
  | FileAddr FilePath
  deriving (Eq)

instance Show MetaStoreAddr where
  show (ZkAddr addr)   = "zk://" <> CB.unpack addr
  show (RqAddr addr)   = "rq://" <> T.unpack addr
  show (FileAddr addr) = "file://" <> addr

metaStoreAddrParser :: O.Parser MetaStoreAddr
metaStoreAddrParser = option (O.maybeReader (Just . parseMetaStoreAddr . T.pack))
  $  long "metastore-uri"
  <> metavar "STR"
  <> help ( "Meta store address, currently support zookeeper and rqlite"
         <> "such as \"zk://127.0.0.1:2181,127.0.0.1:2182 , \"rq://127.0.0.1:4001\"")

-- FIXME: Haskell libraries does not support the case where multiple auths exist
-- case parseURI str of
-- Just URI{..} -> case uriAuthority of
--   Just URIAuth{..}
--     | uriScheme == "zk:" -> ZkAddr . CB.pack $ uriRegName <> uriPort
--     | uriScheme == "rq:" -> RqAddr . T.pack $ uriRegName <> uriPort
--     | otherwise -> errorWithoutStackTrace $ "Invalid meta store address, unsupported scheme: " <> uriScheme
--   Nothing -> errorWithoutStackTrace $ "Invalid meta store address, no Auth: " <> str
-- Nothing  -> errorWithoutStackTrace $ "Invalid meta store address, no parse: " <> str
parseMetaStoreAddr :: Text -> MetaStoreAddr
parseMetaStoreAddr t =
  let parser = do scheme <- AP.takeTill (== ':')
                  AP.string "://"
                  ip <- AP.takeText
                  return (scheme, ip)
   in case AP.parseOnly parser t of
        Right (s, ip)
          | s == "zk" -> ZkAddr . CB.pack .T.unpack $ ip
          | s == "rq" -> RqAddr ip
          | s == "file" -> FileAddr . T.unpack $ ip
          | otherwise -> errorWithoutStackTrace $ "Invalid meta store address, unsupported scheme: " <> show s
        Left eMsg -> errorWithoutStackTrace eMsg

data TlsConfig = TlsConfig
  { keyPath  :: String
  , certPath :: String
  , caPath   :: Maybe String
  } deriving (Show, Eq)

type SecurityProtocolMap = Map Text (Maybe TlsConfig)

defaultProtocolMap :: Maybe TlsConfig -> SecurityProtocolMap
defaultProtocolMap tlsConfig =
  Map.fromList [("plaintext", Nothing), ("tls", tlsConfig)]

enableTlsParser :: O.Parser Bool
enableTlsParser = flag False True
  $  long "enable-tls"
  <> help "Enable tls, require tls-key-path, tls-cert-path options"

tlsKeyPathParser :: O.Parser String
tlsKeyPathParser = strOption
  $  long "tls-key-path"
  <> metavar "PATH"
  <> help "TLS key path"

tlsCertPathParser :: O.Parser String
tlsCertPathParser = strOption
  $  long "tls-cert-path"
  <> metavar "PATH"
  <> help "Signed certificate path"

tlsCaPathParser :: O.Parser String
tlsCaPathParser = strOption
  $  long "tls-ca-path"
  <> metavar "PATH"
  <> help "Trusted CA(Certificate Authority) path"

configPathParser :: O.Parser String
configPathParser = strOption
  $  long "config-path"
  <> metavar "PATH" <> value "/etc/hstream/config.yaml"
  <> help "hstream config path"

bindAddressParser :: O.Parser ByteString
bindAddressParser = strOption
  $ long "bind-address" <> metavar "ADDRESS"
  <> help "the address the server will bind to"

serverPortParser :: O.Parser Word16
serverPortParser = option auto
  $  long "port" <> short 'p'
  <> metavar "INT"
  <> help "server port value"

serverGossipAddressParser :: O.Parser String
serverGossipAddressParser = strOption
  $  long "gossip-address"
  <> metavar "ADDRESS"
  <> help "server gossip address, if not given will use advertised-address"

advertisedAddressParser :: O.Parser String
advertisedAddressParser = strOption
   $ long "advertised-address"
  <> metavar "ADDRESS"
  <> help "server advertised address, e.g. 127.0.0.1"

serverInternalPortParser :: O.Parser Word16
serverInternalPortParser = option auto
  $ long "internal-port"
  <> metavar "INT"
  <> help "server channel port value for internal communication"

serverIDParser :: O.Parser Word32
serverIDParser = option auto
  $ long "server-id"
  <> metavar "UINT32"
  <> help "ID of the hstream server node"

seedNodesParser :: O.Parser Text
seedNodesParser = strOption
  $  long "seed-nodes"
  <> metavar "ADDRESS"
  <> help "host:port pairs of seed nodes, separated by commas (,)"

storeCompressionParser :: O.Parser Compression
storeCompressionParser = option auto
  $ long "store-compression"
  <> metavar "none | lz4 | lz4hc"
  <> help "For debug only, compression option when write records to store."

logLevelParser :: O.Parser Log.Level
logLevelParser = option auto
  $ long "log-level"
  <> metavar "[critical|fatal|warning|info|debug]"
  <> help "Server log level"

logWithColorParser :: O.Parser Bool
logWithColorParser = flag False True
  $  long "log-with-color"
  <> help "Server log with color"

logFlushImmediatelyParser :: O.Parser Bool
logFlushImmediatelyParser = O.switch
   $ long "log-flush-immediately"
  <> help "Flush immediately after logging, this may help debugging"

ldAdminPortParser :: O.Parser Int
ldAdminPortParser = option auto
  $  long "store-admin-port"
  <> metavar "INT"
  <> help "Store admin port value"

ldAdminHostParser :: O.Parser ByteString
ldAdminHostParser = strOption
  $  long "store-admin-host" <> metavar "HOST"
  <> help "Store admin host"

ldLogLevelParser :: O.Parser Log.LDLogLevel
ldLogLevelParser = option auto
  $  long "store-log-level"
  <> metavar "[critical|error|warning|notify|info|debug|spew]"
  <> help "Store log level"

ckpReplicaParser :: O.Parser Int
ckpReplicaParser = option auto
  $ long "checkpoint-replica"
  <> metavar "INT"
  <> help "check point replication factor"

--TODO: This option will be removed once we can get config from admin server.
storeConfigPathParser :: O.Parser CBytes
storeConfigPathParser = strOption
  $  long "store-config"
  <> metavar "PATH" <> value "/data/store/logdevice.conf"
  <> help "Storage config path"

ioTasksPathParser :: O.Parser Text
ioTasksPathParser = strOption
  $  long "io-tasks-path"
  <> metavar "PATH"
  <> help "io tasks path"

ioTasksNetworkParser :: O.Parser Text
ioTasksNetworkParser = strOption
  $  long "io-tasks-network"
  <> metavar "STR"
  <> help "io tasks network"

ioConnectorImageParser :: O.Parser [Text]
ioConnectorImageParser = O.many . strOption $
  long "io-connector-image"
  <> metavar "<source | sink> <target connector> <docker image>"
  <> help "update connector image, e.g. \"source mysql hsteramdb/source-mysql:latest\""

querySnapshotPathParser :: O.Parser FilePath
querySnapshotPathParser = strOption
  $  long "query-snapshot-path"
  <> metavar "PATH" <> value "/data/query_snapshots"
  <> help "hstream query snapshot store path"

data ExperimentalFeature
  = ExperimentalStreamV2
  | ExperimentalKafka
  deriving (Show, Eq)

parseExperimentalFeature :: O.ReadM ExperimentalFeature
parseExperimentalFeature = O.eitherReader $ \case
  "stream-v2" -> Right ExperimentalStreamV2
  "kafka"     -> Right ExperimentalKafka
  x           -> Left $ "cannot parse experimental feature: " <> x

experimentalFeatureParser :: O.Parser ExperimentalFeature
experimentalFeatureParser = option parseExperimentalFeature $
  long "experimental" <> metavar "ExperimentalFeature"

-------------------------------------------------------------------------------

parserOpt :: (Text -> Either String a) -> O.Mod O.OptionFields a -> O.Parser a
parserOpt parse = option $ O.eitherReader (parse . T.pack)
