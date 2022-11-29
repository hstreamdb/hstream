{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.ConfigSpec where

import           Control.Applicative            ((<|>))
import           Control.Exception              (SomeException (SomeException),
                                                 evaluate, try)
import           Data.Aeson.Encode.Pretty       (encodePretty)
import           Data.Bifunctor                 (second)
import           Data.ByteString                (ByteString)
import qualified Data.ByteString.Lazy           as BSL
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

import           HStream.Admin.Store.API        (ProtocolId, binaryProtocolId,
                                                 compactProtocolId)
import           HStream.Gossip                 (GossipOpts (..),
                                                 defaultGossipOpts)
import           HStream.IO.Types               (IOOptions (..))
import           HStream.Server.Config          (CliOptions (..),
                                                 MetaStoreAddr (..),
                                                 ServerOpts (..),
                                                 TlsConfig (..), parseHostPorts,
                                                 parseJSONToOptions,
                                                 readProtocol)
import           HStream.Server.HStreamInternal
import           HStream.Store                  (Compression (..))

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
  , _serverID                  = "server-1"
  , _metaStore                 = ZkAddr "127.0.0.1:2181"
  , _ldConfigPath              = "/data/store/logdevice.conf"
  , _topicRepFactor            = 1
  , _ckpRepFactor              = 3
  , _compression               = CompressionNone
  , _maxRecordSize             = 1024 * 1024 * 1024
  , _tlsConfig                 = Nothing
  , _serverLogLevel            = read "info"
  , _serverLogWithColor        = True
  , _seedNodes                 = [("127.0.0.1", 6571)]
  , _ldAdminHost               = "127.0.0.1"
  , _ldAdminPort               = 6440
  , _ldAdminProtocolId         = binaryProtocolId
  , _ldAdminConnTimeout        = 5000
  , _ldAdminSendTimeout        = 5000
  , _ldAdminRecvTimeout        = 5000
  , _ldLogLevel                = read "info"

  , _gossipOpts                = defaultGossipOpts
  , _ioOptions                 = defaultIOOptions
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
            , "protocol-id"  .= showProtocol _ldAdminProtocolId --TODO
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

showProtocol :: ProtocolId -> Text
showProtocol pid | pid == binaryProtocolId  = "binary"
                 | pid == compactProtocolId = "compact"
                 | otherwise                = "binary"

emptyCliOptions :: CliOptions
emptyCliOptions = CliOptions {
    _configPath          = ""
  , _serverPort_         = Nothing
  , _serverBindAddress_      = Nothing
  , _serverGossipAddress_      = Nothing
  , _serverAdvertisedAddress_   = Nothing
  , _serverAdvertisedListeners_ = mempty
  , _serverInternalPort_ = Nothing
  , _serverID_           = Nothing
  , _serverLogLevel_     = Nothing
  , _serverLogWithColor_ = False
  , _compression_        = Nothing
  , _metaStore_          = Nothing
  , _seedNodes_          = Nothing

  , _enableTls_          = False
  , _tlsKeyPath_         = Nothing
  , _tlsCertPath_        = Nothing
  , _tlsCaPath_          = Nothing

  , _ldAdminHost_        = Nothing
  , _ldAdminPort_        = Nothing
  , _ldLogLevel_         = Nothing
  , _storeConfigPath     = "/data/store/logdevice.conf"

  , _ioTasksPath_        = Nothing
  , _ioTasksNetwork_     = Nothing
  , _ioConnectorImages_  = []
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
    _serverAdvertisedListeners <- M.fromList <$> listOf5' ((,) <$> (T.pack <$> nameGen) <*> arbitrary)
    _metaStore                 <- arbitrary
    let _ldConfigPath   = "/data/store/logdevice.conf"
    let _topicRepFactor = 1
    let _ckpRepFactor   = 3
    let _compression = CompressionNone
    _maxRecordSize             <- arbitrary
    _tlsConfig                 <- arbitrary
    _serverLogLevel            <- read <$> logLevelGen
    _serverLogWithColor        <- arbitrary
    _seedNodes                 <- listOf5' ((,) <$> (encodeUtf8 . T.pack <$> addressGen) <*> portGen)
    _ldAdminHost               <- encodeUtf8 . T.pack <$> addressGen
    _ldAdminPort               <- portGen
    _ldAdminProtocolId         <- readProtocol <$> elements ["binary", "compact"]
    _ldAdminConnTimeout        <- arbitrary
    _ldAdminSendTimeout        <- arbitrary
    _ldAdminRecvTimeout        <- arbitrary
    _ldLogLevel                <- read <$> ldLogLevelGen
    _gossipOpts                <- arbitrary
    _ioOptions                 <- arbitrary
    pure ServerOpts{..}

instance Arbitrary CliOptions where
  arbitrary = do
    let _configPath = ""
    _serverPort_         <- genMaybe $ fromIntegral <$> portGen
    _serverBindAddress_  <- genMaybe $ encodeUtf8 . T.pack <$> addressGen
    _serverGossipAddress_     <- genMaybe addressGen
    _serverAdvertisedAddress_ <- genMaybe addressGen
    _serverAdvertisedListeners_ <- arbitrary
    _serverInternalPort_ <- genMaybe $ fromIntegral <$> portGen
    _serverID_           <- arbitrary
    _serverLogLevel_     <- genMaybe $ read <$> logLevelGen
    _serverLogWithColor_ <- arbitrary
    _compression_        <- arbitrary
    _metaStore_          <- arbitrary
    _seedNodes_          <- genMaybe seedNodesStringGen
    _enableTls_          <- arbitrary
    _tlsKeyPath_         <- genMaybe pathGen
    _tlsCertPath_        <- genMaybe pathGen
    _tlsCaPath_          <- genMaybe pathGen
    _ldAdminHost_        <- genMaybe $ encodeUtf8 . T.pack <$> addressGen
    _ldAdminPort_        <- genMaybe portGen
    _ldLogLevel_         <- genMaybe $ read <$> ldLogLevelGen
    _storeConfigPath     <- CB.pack <$> pathGen
    _ioTasksPath_        <- genMaybe $ T.pack <$> pathGen
    _ioTasksNetwork_     <- genMaybe $ T.pack <$> nameGen
    _ioConnectorImages_  <- listOf5' $ T.pack <$> connectorImageCliOptGen
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
updateServerOptsWithCliOpts CliOptions {..} x@ServerOpts{..} = x {
    _serverHost = fromMaybe _serverHost _serverBindAddress_
  , _serverPort = port
  , _serverInternalPort = fromMaybe _serverInternalPort _serverInternalPort_
  , _serverAddress = fromMaybe _serverAddress _serverAdvertisedAddress_
  , _serverGossipAddress = fromMaybe _serverGossipAddress _serverGossipAddress_
  , _serverAdvertisedListeners = Map.union _serverAdvertisedListeners_ _serverAdvertisedListeners
  , _serverID = fromMaybe _serverID _serverID_
  , _metaStore = fromMaybe _metaStore _metaStore_
  , _ldConfigPath = _storeConfigPath
  , _compression = fromMaybe _compression _compression_
  , _tlsConfig = _tlsConfig_
  , _serverLogLevel = fromMaybe _serverLogLevel _serverLogLevel_
  , _serverLogWithColor = _serverLogWithColor || _serverLogWithColor_
  , _seedNodes = fromMaybe _seedNodes $ parseCliSeeds =<< _seedNodes_
  , _ldAdminHost = fromMaybe _ldAdminHost _ldAdminHost_
  , _ldAdminPort = fromMaybe _ldAdminPort _ldAdminPort_
  , _ldLogLevel = fromMaybe _ldLogLevel _ldLogLevel_
  , _ioOptions = _ioOptions_}
  where
    port = fromMaybe _serverPort _serverPort_
    updateSeedsPort = second $ fromMaybe (fromIntegral port)
    parseCliSeeds = either (const Nothing) (Just . (updateSeedsPort <$>)) . parseHostPorts

    _tlsConfig_  = case (_enableTls_ || isJust _tlsConfig, _tlsKeyPath_ <|> keyPath <$> _tlsConfig,  _tlsCertPath_ <|> certPath <$> _tlsConfig ) of
        (False, _, _) -> Nothing
        (_, Nothing, _) -> errorWithoutStackTrace "enable-tls=true, but tls-key-path is empty"
        (_, _, Nothing) -> errorWithoutStackTrace "enable-tls=true, but tls-cert-path is empty"
        (_, Just kp, Just cp) -> Just $ TlsConfig kp cp (_tlsCaPath_ <|> (caPath =<< _tlsConfig) )

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
        (optSourceImages _ioOptions, optSinkImages _ioOptions) _ioConnectorImages_
    _ioOptions_ = IOOptions {
            optTasksNetwork = fromMaybe (optTasksNetwork _ioOptions) _ioTasksNetwork_
          , optTasksPath = fromMaybe (optTasksPath _ioOptions) _ioTasksPath_
          , optSourceImages = ssi
          , optSinkImages = ski
          }
