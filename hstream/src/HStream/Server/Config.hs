{-# LANGUAGE ApplicativeDo     #-}
{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE CPP               #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -pgmP "hpp --cpp -P" #-}

module HStream.Server.Config
  ( ServerOpts (..)
  , getConfig
    -- * Cli
  , ServerCli (..)
  , runServerCli
  , CliOptions (..)

    -- *
  , TlsConfig (..)
  , FileLoggerSettings (..)
  , AdvertisedListeners
  , advertisedListenersToPB
  , ListenersSecurityProtocolMap
  , SecurityProtocolMap
  , MetaStoreAddr(..)
  , parseJSONToOptions
  , ExperimentalFeature (..)

#if __GLASGOW_HASKELL__ < 902
  , readProtocol
#endif
  , parseHostPorts
  ) where

import           Control.Exception                (throwIO)
import           Control.Monad                    (when)
import qualified Data.Aeson                       as Aeson
import qualified Data.Aeson.Types                 as Aeson
import qualified Data.Attoparsec.Text             as AP
import           Data.Bifunctor                   (second)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Char8            as BSC
import           Data.Foldable                    (foldrM)
import qualified Data.HashMap.Strict              as HM
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, fromMaybe,
                                                   isNothing)
import           Data.String                      (IsString (..))
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import           Data.Text.Encoding               (encodeUtf8)
import           Data.Vector                      (Vector)
import qualified Data.Vector                      as V
import           Data.Word                        (Word16, Word32)
import           Data.Yaml                        as Y (Object,
                                                        ParseException (..),
                                                        Parser, decodeFileThrow,
                                                        parseEither, (.!=),
                                                        (.:), (.:?))
import           Options.Applicative              as O (Alternative (many, (<|>)),
                                                        Parser, auto, flag,
                                                        help, long, maybeReader,
                                                        metavar, option,
                                                        optional, short,
                                                        strOption, value)
import qualified Options.Applicative              as O
import           System.Directory                 (makeAbsolute)
import           Text.Read                        (readEither)
import qualified Z.Data.CBytes                    as CB
import           Z.Data.CBytes                    (CBytes)

import           HStream.Gossip                   (GossipOpts (..),
                                                   defaultGossipOpts)
import qualified HStream.IO.Types                 as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Configuration.Cli
import qualified HStream.Server.HStreamInternal   as SAI
import           HStream.Store                    (Compression (..))
import qualified HStream.Store.Logger             as Log

#ifndef HStreamUseGrpcHaskell
import qualified HsGrpc.Server.Types              as HsGrpc
#endif

-- FIXME: hsthrift only support ghc < 9.x
#if __GLASGOW_HASKELL__ < 902
import qualified HStream.Admin.Store.API          as AA
#endif

-------------------------------------------------------------------------------

data ServerOpts = ServerOpts
  { _serverHost                   :: !ByteString
  , _serverPort                   :: !Word16
  , _serverInternalPort           :: !Word16
  , _serverGossipAddress          :: !String
  , _serverAddress                :: !String
  , _serverAdvertisedListeners    :: !AdvertisedListeners
  , _serverID                     :: !Word32
  , _listenersSecurityProtocolMap :: !ListenersSecurityProtocolMap
  , _securityProtocolMap          :: !SecurityProtocolMap
  , _metaStore                    :: !MetaStoreAddr
  , _ldConfigPath                 :: !CBytes
  , _topicRepFactor               :: !Int
  , _ckpRepFactor                 :: !Int
  , _compression                  :: !Compression
  , _maxRecordSize                :: !Int
  , _tlsConfig                    :: !(Maybe TlsConfig)
  , _serverLogLevel               :: !Log.Level
  , _serverLogWithColor           :: !Bool
  , _serverLogFlushImmediately    :: !Bool
  , serverFileLog                 :: !(Maybe FileLoggerSettings)
  , _seedNodes                    :: ![(ByteString, Int)]
  , _ldAdminHost                  :: !ByteString
  , _ldAdminPort                  :: !Int
#if __GLASGOW_HASKELL__ < 902
  , _ldAdminProtocolId            :: !AA.ProtocolId
#endif
  , _ldAdminConnTimeout           :: !Int
  , _ldAdminSendTimeout           :: !Int
  , _ldAdminRecvTimeout           :: !Int
  , _ldLogLevel                   :: !Log.LDLogLevel

  , _gossipOpts                   :: !GossipOpts
  , _ioOptions                    :: !IO.IOOptions

  , _querySnapshotPath            :: !FilePath
  , experimentalFeatures          :: ![ExperimentalFeature]

  , _enableServerCache            :: !Bool
  , _cacheStorePath               :: !FilePath

#ifndef HStreamUseGrpcHaskell
  , grpcChannelArgs               :: ![HsGrpc.ChannelArg]
#endif
  , serverTokens                  :: ![ByteString]
  } deriving (Show, Eq)

getConfig :: CliOptions -> IO ServerOpts
getConfig opts@CliOptions{..} = do
  path <- makeAbsolute cliConfigPath
  jsonCfg <- decodeFileThrow path
  case parseEither (parseJSONToOptions opts) jsonCfg of
    Left err  -> throwIO (AesonException err)
    Right cfg -> return cfg

-------------------------------------------------------------------------------

parseJSONToOptions :: CliOptions -> Y.Object -> Y.Parser ServerOpts
parseJSONToOptions CliOptions{..} obj = do
  nodeCfgObj  <- obj .: "hserver"
  nodeId              <- nodeCfgObj .:  "id"
  nodeHost            <- fromString <$> nodeCfgObj .:? "bind-address" .!= "0.0.0.0"
  nodePort            <- nodeCfgObj .:? "port" .!= 6570
  nodeGossipAddress   <- nodeCfgObj .:?  "gossip-address"
  nodeInternalPort    <- nodeCfgObj .:? "internal-port" .!= 6571
  nodeAdvertisedListeners <- nodeCfgObj .:? "advertised-listeners" .!= mempty
  nodeAddress         <- nodeCfgObj .:  "advertised-address"
  nodeListenersSecurityProtocolMap <- nodeCfgObj .:? "listeners-security-protocol-map" .!= mempty
  nodeMetaStore     <- parseMetaStoreAddr <$> nodeCfgObj .:  "metastore-uri" :: Y.Parser MetaStoreAddr
  nodeLogLevel      <- nodeCfgObj .:? "log-level" .!= "info"
  nodeLogWithColor  <- nodeCfgObj .:? "log-with-color" .!= True
  -- TODO: For the max_record_size to work properly, we should also tell user
  -- to set payload size for gRPC and LD.
  _maxRecordSize    <- nodeCfgObj .:? "max-record-size" .!= 1048576
  when (_maxRecordSize < 0 && _maxRecordSize > 1048576)
    $ errorWithoutStackTrace "max-record-size has to be a positive number less than 1MB"

  let !_serverID           = fromMaybe nodeId cliServerID
  let !_serverHost         = fromMaybe nodeHost cliServerBindAddress
  let !_serverPort         = fromMaybe nodePort cliServerPort
  let !_serverInternalPort = fromMaybe nodeInternalPort cliServerInternalPort
  let !_serverAddress      = fromMaybe nodeAddress cliServerAdvertisedAddress
  let !_serverAdvertisedListeners = Map.union cliServerAdvertisedListeners nodeAdvertisedListeners
  let !_serverGossipAddress = fromMaybe _serverAddress (cliServerGossipAddress <|> nodeGossipAddress)

  let !_metaStore          = fromMaybe nodeMetaStore cliMetaStore
  let !_compression        = fromMaybe CompressionNone cliStoreCompression

  let !_serverLogLevel     = fromMaybe (readWithErrLog "log-level" nodeLogLevel) cliServerLogLevel
  let !_serverLogWithColor = nodeLogWithColor || cliServerLogWithColor
  let !_serverLogFlushImmediately = cliServerLogFlushImmediately
  let !serverFileLog = cliServerFileLog

  -- Cluster Option
  seeds <- flip fromMaybe cliSeedNodes <$> (nodeCfgObj .: "seed-nodes")
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
  storeLogLevel       <- readWithErrLog "store log-level" <$> storeCfgObj .:? "log-level" .!= "info"
  storeCkpReplica     <- storeCfgObj .:? "checkpoint-replication-factor" .!= 1
  sAdminCfgObj        <- storeCfgObj .:? "store-admin" .!= mempty
  storeAdminHost      <- BSC.pack <$> sAdminCfgObj .:? "host" .!= "127.0.0.1"
  storeAdminPort      <- sAdminCfgObj .:? "port" .!= 6440
#if __GLASGOW_HASKELL__ < 902
  _ldAdminProtocolId  <- readProtocol <$> sAdminCfgObj .:? "protocol-id" .!= "binary"
#endif
  _ldAdminConnTimeout <- sAdminCfgObj .:? "conn-timeout" .!= 5000
  _ldAdminSendTimeout <- sAdminCfgObj .:? "send-timeout" .!= 5000
  _ldAdminRecvTimeout <- sAdminCfgObj .:? "recv-timeout" .!= 5000

  let !_ldAdminHost    = fromMaybe storeAdminHost cliLdAdminHost
  let !_ldAdminPort    = fromMaybe storeAdminPort cliLdAdminPort
  let !_ldConfigPath   = cliStoreConfigPath
  let !_ldLogLevel     = fromMaybe storeLogLevel  cliLdLogLevel
  let !_topicRepFactor = 1
  let !_ckpRepFactor   = fromMaybe storeCkpReplica cliCkpRepFactor

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

  -- hstream io config
  nodeIOCfg <- nodeCfgObj .:? "hstream-io" .!= mempty
  nodeIOTasksPath <- nodeIOCfg .:? "tasks-path" .!= "/tmp/io/tasks"
  nodeIOTasksNetwork <- nodeIOCfg .:? "tasks-network" .!= "host"
  nodeSourceImages <- nodeIOCfg .:? "source-images" .!= HM.empty
  nodeSinkImages <- nodeIOCfg .:? "sink-images" .!= HM.empty
  optExtraDockerArgs <- nodeIOCfg .:? "extra-docker-args" .!= ""
  optFixedConnectorImage <- nodeIOCfg .:? "fixed-connector-image" .!= True
  (optSourceImages, optSinkImages) <- foldrM
        (\img (ss, sk) -> do
          -- "source mysql IMAGE" -> ("source" "mysq" "IMAGE")
          let parseImage = toThreeTuple . T.words
              toThreeTuple [a, b, c] = pure (a, b, c)
              toThreeTuple _         = fail "incorrect image"
          (typ, ct, di) <- parseImage img
          case T.toLower typ of
            "source" -> return (HM.insert ct di ss, sk)
            "sink"   -> return (ss, HM.insert ct di sk)
            _        -> fail "incorrect connector type"
        )
        (nodeSourceImages, nodeSinkImages) cliIoConnectorImages
  let optTasksPath = fromMaybe nodeIOTasksPath cliIoTasksPath
      optTasksNetwork = fromMaybe nodeIOTasksNetwork cliIoTasksNetwork
      !_ioOptions = IO.IOOptions {..}

  -- processing config
  processingCfg <- nodeCfgObj .:? "hstream-processing" .!= mempty
  snapshotPath <- processingCfg .:? "query-snapshot-path" .!= "/data/query_snapshots"
  let !_querySnapshotPath = fromMaybe snapshotPath cliQuerySnapshotPath

  let experimentalFeatures = cliExperimentalFeatures

  cacheStoreCfg <- nodeCfgObj .:? "cache-store" .!= mempty
  serverCache <- cacheStoreCfg .:? "enable-server-cache" .!= False
  let _enableServerCache = cliEnableServerCache || serverCache
  cacheStorePath <- cacheStoreCfg .:? "cache-store-path" .!= "/data/cache_store"
  let _cacheStorePath = fromMaybe cacheStorePath cliCacheStorePath

#ifndef HStreamUseGrpcHaskell
  grpcCfg <- nodeCfgObj .:? "grpc" .!= mempty
  grpcChanArgsCfg <- grpcCfg .:? "channel-args" .!= mempty
  let grpcChannelArgs = getGrpcChannelArgs grpcChanArgsCfg
#endif

  tokensCfg <- nodeCfgObj .:? "tokens" .!= mempty
  let serverTokens = map encodeUtf8 tokensCfg

  return ServerOpts {..}

-------------------------------------------------------------------------------

#ifndef HStreamUseGrpcHaskell

#define MkIntArg(x) \
  (fmap . fmap $ HsGrpc.mk_##x . fromIntegral @Int) . (.:? #x)

getGrpcChannelArgs :: Aeson.Object -> [HsGrpc.ChannelArg]
getGrpcChannelArgs obj = parse id args obj
  where
    -- FIXME: ChanArgValueInt requires a CInt, so there is a potential overflow
    -- :: Aeson.Object -> Aeson.Parser (Maybe Int)
    args :: [Aeson.Object -> Aeson.Parser (Maybe HsGrpc.ChannelArg)]
    args =
      [ MkIntArg(GRPC_ARG_KEEPALIVE_TIME_MS)
      , MkIntArg(GRPC_ARG_KEEPALIVE_TIMEOUT_MS)
      , MkIntArg(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS)
      , MkIntArg(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA)
      , MkIntArg(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS)
      , MkIntArg(GRPC_ARG_HTTP2_MAX_PING_STRIKES)
      ]
    parse f parsers o =
        map (f . fromJust . fromJust)
      . filter (\case Just (Just _) -> True; otherwise -> False)
      . map (flip Aeson.parseMaybe o)
      $ parsers
#endif

-------------------------------------------------------------------------------

#if __GLASGOW_HASKELL__ < 902
readProtocol :: Text.Text -> AA.ProtocolId
readProtocol x = case (Text.strip . Text.toUpper) x of
  "binary"  -> AA.binaryProtocolId
  "compact" -> AA.compactProtocolId
  _         -> AA.binaryProtocolId
#endif

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
