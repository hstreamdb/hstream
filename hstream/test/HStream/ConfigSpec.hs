{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.ConfigSpec where

import           Data.ByteString                (ByteString)
import qualified Data.HashMap.Strict            as HM
import qualified Data.Map.Strict                as M
import           Data.Text                      (Text)
import qualified Data.Text                      as T
import           Data.Text.Encoding             (decodeUtf8, encodeUtf8)
import           Data.Yaml                      (ToJSON (..), Value (..),
                                                 decodeThrow, encode, object,
                                                 parseEither, (.=))
import           Test.Hspec                     (Spec, describe, it, shouldBe)
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
                                                 TlsConfig (..),
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

defaultConfig :: ServerOpts
defaultConfig = ServerOpts
  { _serverHost                = "0.0.0.0"
  , _serverPort                = 6570
  , _serverInternalPort        = 6571
  , _serverAddress             = "127.0.0.1"
  , _serverAdvertisedListeners = mempty
  , _serverID                  = 1
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
        , "host"    .= _serverHost
        , "port"    .= _serverPort
        , "address" .= _serverAddress
        , "internal-port"   .= _serverInternalPort
        , "seed-nodes"      .= showSeedNodes _seedNodes
        , "metastore-uri"   .= show _metaStore          --TODO
        , "compression"     .= showCompression _compression --TODO
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
  , _serverHost_         = Nothing
  , _serverPort_         = Nothing
  , _serverAddress_      = Nothing
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
    let _serverHost = "0.0.0.0"
    _serverID                  <- arbitrary
    _serverPort                <- fromIntegral <$> portGen
    _serverAddress             <- addressGen
    _serverInternalPort        <- fromIntegral <$> portGen
    _serverAdvertisedListeners <- M.fromList <$> listOf5 ((,) <$> (T.pack <$> nameGen) <*> arbitrary)
    _metaStore                 <- arbitrary
    let _ldConfigPath   = "/data/store/logdevice.conf"
    let _topicRepFactor = 1
    let _ckpRepFactor   = 3
    _compression               <- arbitrary
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
