module HStream.Kafka.Server.Config.FromJson
  ( parseJSONToOptions
  ) where

import           Control.Applicative               (optional, (<|>))
import           Control.Monad                     (when)
import qualified Data.Attoparsec.Text              as AP
import           Data.Bifunctor                    (second)
import           Data.ByteString                   (ByteString)
import qualified Data.Map.Strict                   as Map
import           Data.Maybe                        (fromMaybe, isNothing)
import           Data.String                       (IsString (..))
import           Data.Text                         (Text)
import           Data.Text.Encoding                (encodeUtf8)
import           Data.Yaml                         ((.!=), (.:), (.:?))
import qualified Data.Yaml                         as Y
import           Text.Read                         (readEither)

import           HStream.Kafka.Server.Config.Types
import           HStream.Store                     (Compression (..))

-------------------------------------------------------------------------------

parseJSONToOptions :: CliOptions -> Y.Object -> Y.Parser ServerOpts
parseJSONToOptions CliOptions{..} obj = do
  nodeCfgObj          <- obj .: "kafka"
  nodeId              <- nodeCfgObj .:  "id"
  nodeHost            <- fromString <$> nodeCfgObj .:? "bind-address" .!= "0.0.0.0"
  nodePort            <- nodeCfgObj .:? "port" .!= 6570
  nodeGossipAddress   <- nodeCfgObj .:?  "gossip-address"
  nodeGossipPort      <- nodeCfgObj .:? "gossip-port" .!= 6571
  nodeAdvertisedListeners <- nodeCfgObj .:? "advertised-listeners" .!= mempty
  nodeAdvertisedAddress   <- nodeCfgObj .:  "advertised-address"
  nodeListenersSecurityProtocolMap <- nodeCfgObj .:? "listeners-security-protocol-map" .!= mempty
  nodeMetaStore     <- parseMetaStoreAddr <$> nodeCfgObj .:  "metastore-uri" :: Y.Parser MetaStoreAddr
  nodeLogLevel      <- nodeCfgObj .:? "log-level" .!= "info"
  nodeLogWithColor  <- nodeCfgObj .:? "log-with-color" .!= True

  -- Kafka config
  let !_disableAutoCreateTopic = cliDisableAutoCreateTopic
  kafkaCfgObj     <- nodeCfgObj .:? "kafka" .!= mempty
  numPartitions   <- kafkaCfgObj .:? "num-partitions" .!= 1
  defaultReplica  <- kafkaCfgObj .:? "default-replication-factor" .!= 1
  let !_topicRepFactor = numPartitions
  let !_partitionNums  = defaultReplica

  -- TODO: For the max_record_size to work properly, we should also tell user
  -- to set payload size for gRPC and LD.
  _maxRecordSize    <- nodeCfgObj .:? "max-record-size" .!= 1048576
  when (_maxRecordSize < 0 && _maxRecordSize > 1048576)
    $ errorWithoutStackTrace "max-record-size has to be a positive number less than 1MB"

  let !_serverID           = fromMaybe nodeId cliServerID
  let !_serverHost         = fromMaybe nodeHost cliServerBindAddress
  let !_serverPort         = fromMaybe nodePort cliServerPort
  let !_serverGossipPort   = fromMaybe nodeGossipPort cliServerGossipPort
  let !_advertisedAddress  = fromMaybe nodeAdvertisedAddress cliServerAdvertisedAddress
  let !_serverAdvertisedListeners = Map.union cliServerAdvertisedListeners nodeAdvertisedListeners
  let !_serverGossipAddress = fromMaybe _advertisedAddress (cliServerGossipAddress <|> nodeGossipAddress)

  let !_metaStore          = fromMaybe nodeMetaStore cliMetaStore
  let !_compression        = fromMaybe CompressionNone cliStoreCompression

  let !_serverLogLevel     = fromMaybe (readWithErrLog "log-level" nodeLogLevel) cliServerLogLevel
  let !_serverLogWithColor = nodeLogWithColor || cliServerLogWithColor
  let !_serverLogFlushImmediately = cliServerLogFlushImmediately

  -- Cluster Option
  seeds <- flip fromMaybe cliSeedNodes <$> (nodeCfgObj .: "seed-nodes")
  let !_seedNodes = case parseHostPorts seeds of
        Left err -> errorWithoutStackTrace err
        Right hps -> map (second . fromMaybe $ fromIntegral _serverGossipPort) hps

  clusterCfgObj    <- nodeCfgObj .:? "gossip" .!= mempty
  gossipFanout     <- clusterCfgObj .:? "gossip-fanout"     .!= gossipFanout defaultGossipOpts
  retransmitMult   <- clusterCfgObj .:? "retransmit-mult"   .!= retransmitMult defaultGossipOpts
  gossipInterval   <- clusterCfgObj .:? "gossip-interval"   .!= gossipInterval defaultGossipOpts
  probeInterval    <- clusterCfgObj .:? "probe-interval"    .!= probeInterval defaultGossipOpts
  roundtripTimeout <- clusterCfgObj .:? "roundtrip-timeout" .!= roundtripTimeout defaultGossipOpts
  joinWorkerConcurrency <- clusterCfgObj .:? "join-worker-concurrency" .!= joinWorkerConcurrency defaultGossipOpts
  let _gossipOpts = GossipOpts {..}

  -- Store Config
  storeCfgObj         <- obj .:? "hstore" .!= mempty
  storeLogLevel       <- readWithErrLog "store log-level" <$> storeCfgObj .:? "log-level" .!= "info"

  let !_ldConfigPath   = cliStoreConfigPath
  let !_ldLogLevel     = fromMaybe storeLogLevel  cliLdLogLevel

  -- TLS config
  nodeEnableTls   <- nodeCfgObj .:? "enable-tls" .!= False
  nodeTlsKeyPath  <- nodeCfgObj .:? "tls-key-path"
  nodeTlsCertPath <- nodeCfgObj .:? "tls-cert-path"
  nodeTlsCaPath   <- nodeCfgObj .:? "tls-ca-path"
  let !enableTls = cliEnableTls || nodeEnableTls
      !tlsConfig = do key <- cliTlsKeyPath  <|> nodeTlsKeyPath
                      cert <- cliTlsCertPath <|> nodeTlsCertPath
                      pure $ TlsConfig key cert (cliTlsCaPath <|> nodeTlsCaPath)
  when (enableTls && isNothing tlsConfig) $
    errorWithoutStackTrace "enable-tls=true, but tls-config is empty"

  let !_tlsConfig = if enableTls then tlsConfig else Nothing
  -- FIXME: This should be more flexible
  let !_securityProtocolMap = defaultProtocolMap tlsConfig
  let !_listenersSecurityProtocolMap = Map.union cliListenersSecurityProtocolMap nodeListenersSecurityProtocolMap

  -- SASL config
  nodeEnableSaslAuth <- nodeCfgObj .:? "enable-sasl" .!= False
  let !_enableSaslAuth = cliEnableSaslAuth || nodeEnableSaslAuth

  return ServerOpts {..}

-------------------------------------------------------------------------------

parseHostPorts :: Text -> Either String [(ByteString, Maybe Int)]
parseHostPorts = AP.parseOnly (hostPortParser `AP.sepBy` AP.char ',')
  where
    hostPortParser = do
      AP.skipSpace
      host <- encodeUtf8 <$> AP.takeTill (`elem` [':', ','])
      port <- optional (AP.char ':' *> AP.decimal)
      return (host, port)

readWithErrLog :: Read a => String -> String -> a
readWithErrLog opt v = case readEither v of
  Right x -> x
  Left _err -> errorWithoutStackTrace $ "Failed to parse value " <> show v <> " for option " <> opt
