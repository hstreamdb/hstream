{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE CPP #-}

module HStream.ConfigSpec where

import           Control.Applicative            ((<|>))
import           Control.Exception              (SomeException, evaluate, try)
import           Data.Bifunctor                 (second)
import           Data.ByteString                (ByteString)
import qualified Data.HashMap.Strict            as HM
import qualified Data.Map.Strict                as M
import qualified Data.Map.Strict                as Map
import           Data.Maybe                     (fromMaybe, isJust)
import           Data.Text                      (Text)
import qualified Data.Text                      as T
import           Data.Text.Encoding             (decodeUtf8, encodeUtf8)
import           Data.Yaml                      (ToJSON (..), Value (..),
                                                 decodeThrow, encode, object,
                                                 parseEither, (.=))
import           Test.Hspec                     (Spec, anyException, describe,
                                                 it, shouldBe, shouldThrow)
import           Test.Hspec.QuickCheck          (prop)
import           Test.QuickCheck                (Arbitrary, Gen, choose,
                                                 elements, listOf, listOf1)
import           Test.QuickCheck.Arbitrary      (Arbitrary (arbitrary))
import           Test.QuickCheck.Gen            (oneof, resize)
import qualified Z.Data.CBytes                  as CB

import           HStream.Gossip                 (GossipOpts (..),
                                                 defaultGossipOpts)
import           HStream.IO.Types               (IOOptions (..))
import           HStream.Server.Config          (CliOptions (..),
                                                 MetaStoreAddr (..),
                                                 ServerOpts (..),
                                                 TlsConfig (..), parseHostPorts,
                                                 parseJSONToOptions)
import           HStream.Server.HStreamInternal
import           HStream.Store                  (Compression (..))

#if __GLASGOW_HASKELL__ < 902
import           HStream.Admin.Store.API        (ProtocolId, binaryProtocolId,
                                                 compactProtocolId)
import           HStream.Server.Config          (readProtocol)
#endif

spec :: Spec
spec = describe "HStream.ConfigSpec" $ do
  describe "parseConfig" $ do
    it "basic config test" $ do
      let yaml = encode $ toJSON defaultConfig
      -- putStrLn $ T.unpack $ decodeUtf8 yaml
      obj <- decodeThrow yaml
      -- putStrLn $ T.unpack $ decodeUtf8 $ BSL.toStrict $ encodePretty obj
      parseEither (parseJSONToOptions emptyCliOptions) obj `shouldBe` Right defaultConfig

    prop "prop test" $ \x -> do
      -- putStrLn $ T.unpack $ decodeUtf8 $ BSL.toStrict $ encodePretty x
      let yaml = encode $ toJSON x
      -- putStrLn $ T.unpack $ decodeUtf8 yaml
      obj <- decodeThrow yaml
      -- putStrLn $ T.unpack $ decodeUtf8 $ BSL.toStrict $ encodePretty obj
      parseEither (parseJSONToOptions emptyCliOptions) obj `shouldBe` Right x

    prop "Cli options override configuration files" $ \(serverOpts, cliOptions) -> do
      let yaml = encode $ toJSON serverOpts
      -- putStrLn $ T.unpack $ decodeUtf8 yaml
      obj <- decodeThrow yaml
      -- putStrLn $ T.unpack $ decodeUtf8 $ BSL.toStrict $ encodePretty obj
      -- print cliOptions
      try (evaluate $ updateServerOptsWithCliOpts cliOptions serverOpts) >>= \case
        Left (_ :: SomeException) -> do
          evaluate (parseEither (parseJSONToOptions cliOptions) obj) `shouldThrow` anyException
        Right success -> parseEither (parseJSONToOptions cliOptions) obj `shouldBe` Right success


defaultConfig :: ServerOpts
defaultConfig = ServerOpts
  { _serverHost                = "0.0.0.0"
  , _serverPort                = 6570
  , _serverInternalPort        = 6571
  , _serverAddress             = "127.0.0.1"
  , _serverGossipAddress       = "127.0.0.1"
  , _serverAdvertisedListeners = mempty
  , _listenersSecurityProtocolMap = mempty
  , _serverID                  = 1
  , _metaStore                 = ZkAddr "127.0.0.1:2181"
  , _ldConfigPath              = "/data/store/logdevice.conf"
  , _topicRepFactor            = 1
  , _ckpRepFactor              = 1
  , _compression               = CompressionNone
  , _maxRecordSize             = 1024 * 1024 * 1024
  , _tlsConfig                 = Nothing
  , _serverLogLevel            = read "info"
  , _serverLogWithColor        = True
  , _seedNodes                 = [("127.0.0.1", 6571)]
  , _ldAdminHost               = "127.0.0.1"
  , _ldAdminPort               = 6440
#if __GLASGOW_HASKELL__ < 902
  , _ldAdminProtocolId         = binaryProtocolId
#endif
  , _ldAdminConnTimeout        = 5000
  , _ldAdminSendTimeout        = 5000
  , _ldAdminRecvTimeout        = 5000
  , _ldLogLevel                = read "info"
  , _securityProtocolMap = M.fromList [("plaintext", Nothing), ("tls", Nothing)]

  , _gossipOpts                = defaultGossipOpts
  , _ioOptions                 = defaultIOOptions
  , _querySnapshotPath         = "/data/query_snapshots"
  , experimentalFeatures       = []
  }

defaultIOOptions :: IOOptions
defaultIOOptions = IOOptions {
    optTasksNetwork = "host"
  , optTasksPath    = "/tmp/io/tasks"
  , optSourceImages = mempty
  , optSinkImages   = mempty
  }

instance ToJSON ServerOpts where
  toJSON ServerOpts{..} = object [
      "hserver" .= object ([
          "id"      .= _serverID
        , "port"    .= _serverPort
        , "bind-address" .= decodeUtf8 _serverHost
        , "advertised-address" .= _serverAddress
        , "gossip-address" .= _serverGossipAddress
        , "internal-port"   .= _serverInternalPort
        , "seed-nodes"      .= showSeedNodes _seedNodes
        , "metastore-uri"   .= show _metaStore          --TODO
        , "log-level"       .= show _serverLogLevel  --TODO
        , "log-with-color"  .= _serverLogWithColor
        , "max-record-size" .= _maxRecordSize
        , "gossip"          .= _gossipOpts
        , "hstream-io"      .= _ioOptions      --TODO
        , "listeners-security-protocol-map" .= _listenersSecurityProtocolMap
        ]
      ++ ["advertised-listeners" .= _serverAdvertisedListeners       --TODO
         | not $ null _serverAdvertisedListeners]
      ++ case _tlsConfig of
        Nothing -> []
        Just TlsConfig {..} ->
          [ "enable-tls" .= True , "tls-cert-path" .= certPath , "tls-key-path"  .= keyPath ]
          ++ maybe [] (\x -> ["tls-ca-path" .= x]) caPath
      )
    , "hstore" .= object [
          "log-level"   .= show _ldLogLevel --TODO
        , "store-admin" .= object [
              "host" .= decodeUtf8 _ldAdminHost
            , "port" .= _ldAdminPort
#if __GLASGOW_HASKELL__ < 902
            , "protocol-id"  .= showProtocol _ldAdminProtocolId --TODO
#endif
            , "conn-timeout" .= _ldAdminConnTimeout
            , "send-timeout" .= _ldAdminSendTimeout
            , "recv-timeout" .= _ldAdminRecvTimeout
            ]
    ]
    ]

instance ToJSON GossipOpts where
  toJSON GossipOpts{..} = object [
      "gossip-fanout"     .= Number (fromIntegral gossipFanout)
    , "retransmit-mult"   .= Number (fromIntegral retransmitMult)
    , "gossip-interval"   .= Number (fromIntegral gossipInterval)
    , "probe-interval"    .= Number (fromIntegral probeInterval)
    , "roundtrip-timeout" .= Number (fromIntegral roundtripTimeout)
    ]

instance ToJSON IOOptions where
  toJSON IOOptions{..} = object $ [
      "tasks-path"    .= optTasksPath
    , "tasks-network" .= optTasksNetwork]
    ++ ["source-images" .= toJSON optSourceImages | not (HM.null optSourceImages)]
    ++ ["sink-images"   .= toJSON optSinkImages   | not (HM.null optSinkImages)]

showCompression ::Compression -> Text
showCompression CompressionNone     = "none"
showCompression CompressionLZ4      = "lz4"
showCompression CompressionLZ4HC    = "lz4hc"
showCompression (CompressionZSTD x) = "zstd" <> ":" <> T.pack (show x)

showSeedNodes :: [(ByteString, Int)] -> Text
showSeedNodes = T.intercalate "," . map (\(h,p) -> decodeUtf8 h <> ":" <> T.pack (show p))

#if __GLASGOW_HASKELL__ < 902
showProtocol :: ProtocolId -> Text
showProtocol pid | pid == binaryProtocolId  = "binary"
                 | pid == compactProtocolId = "compact"
                 | otherwise                = "binary"
#endif

emptyCliOptions :: CliOptions
emptyCliOptions = CliOptions
  { cliConfigPath          = ""
  , cliServerPort         = Nothing
  , cliServerBindAddress      = Nothing
  , cliServerGossipAddress      = Nothing
  , cliServerAdvertisedAddress   = Nothing
  , cliServerAdvertisedListeners = mempty
  , cliListenersSecurityProtocolMap = mempty
  , cliServerInternalPort = Nothing
  , cliServerID           = Nothing
  , cliServerLogLevel     = Nothing
  , cliServerLogWithColor = False
  , cliStoreCompression   = Nothing
  , cliMetaStore          = Nothing
  , cliSeedNodes          = Nothing

  , cliEnableTls          = False
  , cliTlsKeyPath         = Nothing
  , cliTlsCertPath        = Nothing
  , cliTlsCaPath          = Nothing

  , cliLdAdminHost        = Nothing
  , cliLdAdminPort        = Nothing
  , cliLdLogLevel         = Nothing
  , cliStoreConfigPath    = "/data/store/logdevice.conf"
  , cliCkpRepFactor       = Nothing

  , cliIoTasksPath        = Nothing
  , cliIoTasksNetwork     = Nothing
  , cliIoConnectorImages  = []

  , cliQuerySnapshotPath  = Nothing

  , cliExperimentalFeatures = []
  }


--------------------------------------------------------------------------------
-- Utils

instance Arbitrary ServerOpts where
  arbitrary = do
    _serverHost                <- encodeUtf8 . T.pack <$> addressGen
    _serverID                  <- arbitrary
    _serverPort                <- fromIntegral <$> portGen
    _serverAddress             <- addressGen
    _serverGossipAddress       <- addressGen
    _serverInternalPort        <- fromIntegral <$> portGen
    listenersKeys              <- listOf5' (T.pack <$> nameGen)
    _serverAdvertisedListeners <- M.fromList . zip listenersKeys <$> arbitrary
    _metaStore                 <- arbitrary
    let _ldConfigPath   = "/data/store/logdevice.conf"
    let _topicRepFactor = 1
    let _ckpRepFactor   = 1
    let _compression = CompressionNone
    _maxRecordSize             <- arbitrary
    _tlsConfig                 <- arbitrary
    _serverLogLevel            <- read <$> logLevelGen
    _serverLogWithColor        <- arbitrary
    _seedNodes                 <- listOf5' ((,) <$> (encodeUtf8 . T.pack <$> addressGen) <*> portGen)
    _ldAdminHost               <- encodeUtf8 . T.pack <$> addressGen
    _ldAdminPort               <- portGen
#if __GLASGOW_HASKELL__ < 902
    _ldAdminProtocolId         <- readProtocol <$> elements ["binary", "compact"]
#endif
    _ldAdminConnTimeout        <- arbitrary
    _ldAdminSendTimeout        <- arbitrary
    _ldAdminRecvTimeout        <- arbitrary
    _ldLogLevel                <- read <$> ldLogLevelGen
    _gossipOpts                <- arbitrary
    _ioOptions                 <- arbitrary
    let _querySnapshotPath = "/data/query_snapshots"
    _listenersSecurityProtocolMap <- M.fromList . zip listenersKeys . repeat <$> elements ["plaintext", "tls"]
    let _securityProtocolMap = M.fromList [("plaintext", Nothing), ("tls", _tlsConfig)]
    let experimentalFeatures = []
    pure ServerOpts{..}

instance Arbitrary CliOptions where
  arbitrary = do
    let cliConfigPath = ""
    let cliCkpRepFactor = Nothing
    cliServerPort         <- genMaybe $ fromIntegral <$> portGen
    cliServerBindAddress  <- genMaybe $ encodeUtf8 . T.pack <$> addressGen
    cliServerGossipAddress     <- genMaybe addressGen
    cliServerAdvertisedAddress <- genMaybe addressGen
    cliServerAdvertisedListeners <- arbitrary
    cliListenersSecurityProtocolMap <- M.fromList . zip (Map.keys cliServerAdvertisedListeners) . repeat <$> elements ["plaintext", "tls"]
    cliServerInternalPort <- genMaybe $ fromIntegral <$> portGen
    cliServerID           <- arbitrary
    cliServerLogLevel     <- genMaybe $ read <$> logLevelGen
    cliServerLogWithColor <- arbitrary
    cliStoreCompression   <- arbitrary
    cliMetaStore          <- arbitrary
    cliSeedNodes          <- genMaybe seedNodesStringGen
    cliEnableTls          <- arbitrary
    cliTlsKeyPath         <- genMaybe pathGen
    cliTlsCertPath        <- genMaybe pathGen
    cliTlsCaPath          <- genMaybe pathGen
    cliLdAdminHost        <- genMaybe $ encodeUtf8 . T.pack <$> addressGen
    cliLdAdminPort        <- genMaybe portGen
    cliLdLogLevel         <- genMaybe $ read <$> ldLogLevelGen
    cliStoreConfigPath    <- CB.pack <$> pathGen
    cliIoTasksPath        <- genMaybe $ T.pack <$> pathGen
    cliIoTasksNetwork     <- genMaybe $ T.pack <$> nameGen
    cliIoConnectorImages  <- listOf5' $ T.pack <$> connectorImageCliOptGen
    let cliQuerySnapshotPath = Just "/data/query_snapshots"
    let cliExperimentalFeatures = []
    pure CliOptions{..}

instance Arbitrary Listener where
  arbitrary = do
    listenerAddress <- T.pack <$> addressGen
    listenerPort   <- fromIntegral <$> portGen
    pure Listener{..}

instance Arbitrary IOOptions where
  arbitrary = do
    optTasksNetwork <- T.pack <$> nameGen
    optTasksPath    <- T.pack <$> pathGen
    optSourceImages <- HM.fromList <$> listOf5 ((,) <$> (T.pack <$> nameGen) <*> (T.pack <$> pathGen))
    optSinkImages   <- HM.fromList <$> listOf5 ((,) <$> (T.pack <$> nameGen) <*> (T.pack <$> pathGen))
    pure IOOptions{..}

instance Arbitrary GossipOpts where
  arbitrary = do
    gossipFanout     <- arbitrary
    retransmitMult   <- arbitrary
    gossipInterval   <- arbitrary
    probeInterval    <- arbitrary
    roundtripTimeout <- arbitrary
    let joinWorkerConcurrency = 10
    pure GossipOpts{..}

instance Arbitrary TlsConfig where
  arbitrary = do
    keyPath  <- pathGen
    certPath <- pathGen
    caPath   <- Just <$> pathGen
    pure TlsConfig{..}

instance Arbitrary Compression where
  arbitrary = do
    level <- choose (0,10)
    elements [ CompressionNone
             , CompressionLZ4
             , CompressionLZ4HC
             , CompressionZSTD level]

instance Arbitrary MetaStoreAddr where
  arbitrary = do
    uri <- uriGen
    elements [ ZkAddr (CB.pack uri), RqAddr (T.pack uri)]

aByteGen :: Gen String
aByteGen = do
  x <- choose (0, 255) :: Gen Int
  pure $ show x

addressGen :: Gen String
addressGen = do
  x <- aByteGen
  y <- aByteGen
  z <- aByteGen
  w <- aByteGen
  domain <- nameGen
  elements [x <> "." <> y <> "." <> z <> "." <> w, domain]

portGen :: Gen Int
portGen = choose (0, 65535)

uriGen :: Gen String
uriGen = do
  x <- addressGen
  y <- portGen
  pure $ x <> ":" <> show y

pathGen :: Gen String
pathGen = do
  names <- listOf5' nameGen
  pure $ foldr ((<>) .  ("/" <>)) mempty names

alphabet :: Gen Char
alphabet = elements $ ['a'..'z'] ++ ['A'..'Z']

alphaNum :: Gen Char
alphaNum = oneof [alphabet, elements ['0'..'9']]

nameGen :: Gen String
nameGen = do
  a <- listOf5' alphabet
  b <- listOf5 alphaNum
  return $ a <> b

logLevelGen :: Gen String
logLevelGen = elements ["debug", "info", "warning", "fatal", "critical"]

ldLogLevelGen :: Gen String
ldLogLevelGen = elements ["critical", "error", "warning", "notify", "info", "debug", "spew"]

listOf5 :: Gen a -> Gen [a]
listOf5 x = resize 5 (listOf x)

listOf5' :: Gen a -> Gen [a]
listOf5' x = resize 5 (listOf1 x)

connectorImageCliOptGen :: Gen String
connectorImageCliOptGen = do
  cType <- elements ["source", "sink"]
  cName <- nameGen
  iName <- nameGen
  pure $ unwords [cType, cName, iName]

genMaybe :: Gen a -> Gen (Maybe a)
genMaybe gen = oneof [pure Nothing, Just <$> gen]

seedNodesStringGen :: Gen Text
seedNodesStringGen = T.intercalate ", " <$> listOf5' (T.pack <$> uriGen)

updateServerOptsWithCliOpts :: CliOptions -> ServerOpts -> ServerOpts
updateServerOptsWithCliOpts CliOptions{..} x@ServerOpts{..} = x {
    _serverHost = fromMaybe _serverHost cliServerBindAddress
  , _serverPort = port
  , _serverInternalPort = fromMaybe _serverInternalPort cliServerInternalPort
  , _serverAddress = fromMaybe _serverAddress cliServerAdvertisedAddress
  , _serverGossipAddress = fromMaybe _serverGossipAddress cliServerGossipAddress
  , _serverAdvertisedListeners = Map.union cliServerAdvertisedListeners _serverAdvertisedListeners
  , _listenersSecurityProtocolMap = Map.union cliListenersSecurityProtocolMap _listenersSecurityProtocolMap
  , _serverID = fromMaybe _serverID cliServerID
  , _metaStore = fromMaybe _metaStore cliMetaStore
  , _ldConfigPath = cliStoreConfigPath
  , _compression = fromMaybe _compression cliStoreCompression
  , _tlsConfig = tlsConfig
  , _serverLogLevel = fromMaybe _serverLogLevel cliServerLogLevel
  , _serverLogWithColor = _serverLogWithColor || cliServerLogWithColor
  , _seedNodes = fromMaybe _seedNodes $ parseCliSeeds =<< cliSeedNodes
  , _ldAdminHost = fromMaybe _ldAdminHost cliLdAdminHost
  , _ldAdminPort = fromMaybe _ldAdminPort cliLdAdminPort
  , _ldLogLevel = fromMaybe _ldLogLevel cliLdLogLevel
  , _ckpRepFactor = fromMaybe _ckpRepFactor cliCkpRepFactor
  , _ioOptions = cliIoOptions
  , _securityProtocolMap = Map.insert "tls" tlsConfig _securityProtocolMap}
  where
    port = fromMaybe _serverPort cliServerPort
    updateSeedsPort = second $ fromMaybe (fromIntegral port)
    parseCliSeeds = either (const Nothing) (Just . (updateSeedsPort <$>)) . parseHostPorts

    tlsConfig  = case (cliEnableTls || isJust _tlsConfig, cliTlsKeyPath <|> keyPath <$> _tlsConfig,  cliTlsCertPath <|> certPath <$> _tlsConfig ) of
        (False, _, _) -> Nothing
        (_, Nothing, _) -> errorWithoutStackTrace "enable-tls=true, but tls-key-path is empty"
        (_, _, Nothing) -> errorWithoutStackTrace "enable-tls=true, but tls-cert-path is empty"
        (_, Just kp, Just cp) -> Just $ TlsConfig kp cp (cliTlsCaPath <|> (caPath =<< _tlsConfig) )

    (ssi,ski) = foldr
        (\img (ss, sk) -> do
          -- "source mysql IMAGE" -> ("source" "mysq" "IMAGE")
          let parseImage = toThreeTuple . T.words
              toThreeTuple [a, b, c] = pure (a, b, c)
          (typ, ct, di) <- parseImage img
          case T.toLower typ of
            "source" -> (HM.insert ct di ss, sk)
            "sink"   -> (ss, HM.insert ct di sk)
            _        -> (ss, sk)
        )
        (optSourceImages _ioOptions, optSinkImages _ioOptions) cliIoConnectorImages
    cliIoOptions = IOOptions {
            optTasksNetwork = fromMaybe (optTasksNetwork _ioOptions) cliIoTasksNetwork
          , optTasksPath = fromMaybe (optTasksPath _ioOptions) cliIoTasksPath
          , optSourceImages = ssi
          , optSinkImages = ski
          }
