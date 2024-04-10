{-# LANGUAGE ApplicativeDo     #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}

module HStream.Kafka.Server.Config.FromCli
  ( serverCliParser
  , runServerCli

  , CliOptions (..)
  , cliOptionsParser

    -- * Helpers
  , parseMetaStoreAddr
  ) where

import qualified Data.Attoparsec.Text              as AP
import           Data.ByteString                   (ByteString)
import qualified Data.Map.Strict                   as Map
import qualified Data.Set                          as Set
import           Data.Text                         (Text)
import qualified Data.Text                         as T
import           Data.Word                         (Word16, Word32)
import           Options.Applicative               as O (auto, flag, help, long,
                                                         maybeReader, metavar,
                                                         option, optional,
                                                         short, strOption,
                                                         value, (<**>), (<|>))
import qualified Options.Applicative               as O
import           System.Environment                (getProgName)
import           System.Exit                       (exitSuccess)
import           Z.Data.CBytes                     (CBytes)

import           HStream.Kafka.Server.Config.Types
import qualified HStream.Logger                    as Log
import           HStream.Store                     (Compression (..))
import           HStream.Store.Logger              (LDLogLevel (..))

-------------------------------------------------------------------------------

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

cliOptionsParser :: O.Parser CliOptions
cliOptionsParser = do
  cliConfigPath <- configPathParser

  cliServerBindAddress   <- optional bindAddressParser
  cliServerPort          <- optional serverPortParser
  cliMetricsPort         <- optional metricsPortParser

  cliServerID           <- optional serverIDParser
  cliMetaStore          <- optional metaStoreAddrParser

  cliServerLogLevel     <- optional logLevelParser
  cliServerLogWithColor <- logWithColorParser
  cliServerLogFlushImmediately <- logFlushImmediatelyParser
  cliServerFileLog <- optional fileLoggerSettingsParser

  cliServerGossipAddress <- optional serverGossipAddressParser
  cliServerGossipPort    <- optional serverGossipPortParser
  cliSeedNodes          <- optional seedNodesParser

  cliServerAdvertisedAddress      <- optional advertisedAddressParser
  cliServerAdvertisedListeners    <- advertisedListenersParser
  cliListenersSecurityProtocolMap <- listenersSecurityProtocolMapParser

  cliLdLogLevel      <- optional ldLogLevelParser
  cliStoreConfigPath <- storeConfigPathParser
  cliEnableTls          <- enableTlsParser
  cliTlsKeyPath         <- optional tlsKeyPathParser
  cliTlsCertPath        <- optional tlsCertPathParser
  cliTlsCaPath          <- optional tlsCaPathParser

  cliStoreCompression <- optional storeCompressionParser

  cliEnableSaslAuth <- enableSaslAuthParser
  cliEnableAcl      <- enableAclParser

  cliDisableAutoCreateTopic <- disableAutoCreateTopicParser

  cliExperimentalFeatures <- O.many experimentalFeatureParser

  return CliOptions{..}

-------------------------------------------------------------------------------
-- Parser

parseAdvertisedListeners :: Text -> Either String AdvertisedListeners
parseAdvertisedListeners =
  let parser = do key <- AP.takeTill (== ':')
                  AP.string ":hstream://"
                  address <- AP.takeTill (== ':')
                  AP.char ':'
                  port <- AP.decimal
                  return (key, Set.singleton Listener{ listenerAddress = address, listenerPort = port})
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

fileLoggerSettingsParser :: O.Parser FileLoggerSettings
fileLoggerSettingsParser = do
  logpath <- strOption
     $ long "file-log-path"
    <> metavar "PATH"
    <> help "File logger path"
  logsize <- option auto
     $ long "file-log-size"
    <> metavar "INT" <> value 20971520
    <> help "The maximum size of the log before it's rolled, in bytes. Default is 20MB"
  lognum <- option auto
     $ long "file-log-num"
    <> metavar "INT" <> value 10
    <> help "The maximum number of rolled log files to keep. Default is 10"
  return FileLoggerSettings{..}

metaStoreAddrParser :: O.Parser MetaStoreAddr
metaStoreAddrParser = option (O.maybeReader (Just . parseMetaStoreAddr . T.pack))
  $  long "metastore-uri"
  <> metavar "STR"
  <> help ( "Meta store address, currently support zookeeper and rqlite"
         <> "such as \"zk://127.0.0.1:2181,127.0.0.1:2182 , \"rq://127.0.0.1:4001\"")

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

metricsPortParser :: O.Parser Word16
metricsPortParser = option auto
  $  long "metrics-port"
  <> metavar "INT"
  <> help "metrics port value"

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

serverGossipPortParser :: O.Parser Word16
serverGossipPortParser = option auto
  $ long "gossip-port"
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
logWithColorParser = O.switch
   $ long "log-with-color"
  <> help "Server log with color"

logFlushImmediatelyParser :: O.Parser Bool
logFlushImmediatelyParser = O.switch
   $ long "log-flush-immediately"
  <> help "Flush immediately after logging, this may help debugging"

ldLogLevelParser :: O.Parser LDLogLevel
ldLogLevelParser = option auto
  $  long "store-log-level"
  <> metavar "[critical|error|warning|notify|info|debug|spew]"
  <> help "Store log level"

storeConfigPathParser :: O.Parser CBytes
storeConfigPathParser = strOption
  $  long "store-config"
  <> metavar "PATH" <> value "/data/store/logdevice.conf"
  <> help "Storage config path"

enableSaslAuthParser :: O.Parser Bool
enableSaslAuthParser = flag False True
  $  long "enable-sasl"
  <> help "Enable SASL authentication"

enableAclParser :: O.Parser Bool
enableAclParser = flag False True
  $  long "enable-acl"
  <> help "Enable ACL authorization"

disableAutoCreateTopicParser :: O.Parser Bool
disableAutoCreateTopicParser = flag False True
  $  long "disable-auto-create-topic"
  <> help "Disable auto create topic"

experimentalFeatureParser :: O.Parser ExperimentalFeature
experimentalFeatureParser = option parseExperimentalFeature $
  long "experimental" <> metavar "ExperimentalFeature"

-------------------------------------------------------------------------------

parserOpt :: (Text -> Either String a) -> O.Mod O.OptionFields a -> O.Parser a
parserOpt parse = option $ O.eitherReader (parse . T.pack)
