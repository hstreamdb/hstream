{-# LANGUAGE DuplicateRecordFields #-}

module HStream.Kafka.Server.Config.Types
  ( ServerOpts (..)
  , ServerCli (..), CliOptions (..)

  , MetaStoreAddr (..)
  , AdvertisedListeners
  , ListenersSecurityProtocolMap
  , TlsConfig (..)
  , SecurityProtocolMap, defaultProtocolMap

    -- * Helpers
  , advertisedListenersToPB
  , parseMetaStoreAddr

    -- * Re-exports
  , GossipOpts (..), defaultGossipOpts
  , SAI.Listener (..)
  , SAI.ListOfListener (..)
  ) where

import qualified Data.Attoparsec.Text           as AP
import           Data.ByteString                (ByteString)
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           Data.Set                       (Set)
import qualified Data.Set                       as Set
import           Data.Text                      (Text)
import qualified Data.Text                      as Text
import qualified Data.Vector                    as V
import           Data.Word
import           HStream.Gossip                 (GossipOpts (..),
                                                 defaultGossipOpts)
import qualified Z.Data.CBytes                  as CBytes
import           Z.Data.CBytes                  (CBytes)

import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as SAI
import           HStream.Store                  (Compression (..))
import           HStream.Store.Logger           (LDLogLevel)

-------------------------------------------------------------------------------

data ServerOpts = ServerOpts
  { _serverHost                   :: !ByteString
  , _serverPort                   :: !Word16

  , _advertisedAddress            :: !String
  , _serverAdvertisedListeners    :: !AdvertisedListeners
  , _securityProtocolMap          :: !SecurityProtocolMap
  , _listenersSecurityProtocolMap :: !ListenersSecurityProtocolMap

  , _serverID                     :: !Word32
  , _metaStore                    :: !MetaStoreAddr
  , _serverLogLevel               :: !Log.Level
  , _serverLogWithColor           :: !Bool
  , _tlsConfig                    :: !(Maybe TlsConfig)

  , _serverGossipAddress          :: !String
  , _serverGossipPort             :: !Word16
  , _gossipOpts                   :: !GossipOpts

  , _topicRepFactor               :: !Int
  , _maxRecordSize                :: !Int
  , _seedNodes                    :: ![(ByteString, Int)]

  , _ldLogLevel                   :: !LDLogLevel
  , _ldConfigPath                 :: !CBytes

  , _compression                  :: !Compression
  } deriving (Show, Eq)

-------------------------------------------------------------------------------
-- Command Line

data ServerCli
  = Cli CliOptions
  | ShowVersion
  deriving (Show)

data CliOptions = CliOptions
  { cliConfigPath                   :: !String

  , cliServerPort                   :: !(Maybe Word16)
  , cliServerBindAddress            :: !(Maybe ByteString)

  , cliServerID                     :: !(Maybe Word32)
  , cliServerLogLevel               :: !(Maybe Log.Level)
  , cliServerLogWithColor           :: !Bool
  , cliMetaStore                    :: !(Maybe MetaStoreAddr)

    -- Gossip
  , cliServerGossipAddress          :: !(Maybe String)
  , cliServerGossipPort             :: !(Maybe Word16)
  , cliSeedNodes                    :: !(Maybe Text)

    -- AdvertisedListeners
  , cliServerAdvertisedAddress      :: !(Maybe String)
  , cliServerAdvertisedListeners    :: !AdvertisedListeners
  , cliListenersSecurityProtocolMap :: !ListenersSecurityProtocolMap

    -- TLS config
  , cliEnableTls                    :: !Bool
  , cliTlsKeyPath                   :: !(Maybe String)
  , cliTlsCertPath                  :: !(Maybe String)
  , cliTlsCaPath                    :: !(Maybe String)

    -- Store config
  , cliStoreConfigPath              :: !CBytes
  , cliLdLogLevel                   :: !(Maybe LDLogLevel)

    -- Internal options
  , cliStoreCompression             :: !(Maybe Compression)
  } deriving Show

-------------------------------------------------------------------------------

data MetaStoreAddr
  = ZkAddr CBytes
  | RqAddr Text
  | FileAddr FilePath
  deriving (Eq)

instance Show MetaStoreAddr where
  show (ZkAddr addr)   = "zk://" <> CBytes.unpack addr
  show (RqAddr addr)   = "rq://" <> Text.unpack addr
  show (FileAddr addr) = "file://" <> addr

type AdvertisedListeners = Map Text (Set SAI.Listener)

type ListenersSecurityProtocolMap = Map Text Text

data TlsConfig = TlsConfig
  { keyPath  :: String
  , certPath :: String
  , caPath   :: Maybe String
  } deriving (Show, Eq)

type SecurityProtocolMap = Map Text (Maybe TlsConfig)

defaultProtocolMap :: Maybe TlsConfig -> SecurityProtocolMap
defaultProtocolMap tlsConfig =
  Map.fromList [("plaintext", Nothing), ("tls", tlsConfig)]

advertisedListenersToPB :: AdvertisedListeners -> Map Text (Maybe SAI.ListOfListener)
advertisedListenersToPB =
  Map.map $ Just . SAI.ListOfListener . V.fromList . Set.toList

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
                  _ <- AP.string "://"
                  ip <- AP.takeText
                  return (scheme, ip)
   in case AP.parseOnly parser t of
        Right (s, ip)
          | s == "zk" -> ZkAddr . CBytes.pack . Text.unpack $ ip
          | s == "rq" -> RqAddr ip
          | s == "file" -> FileAddr . Text.unpack $ ip
          | otherwise -> errorWithoutStackTrace $ "Invalid meta store address, unsupported scheme: " <> show s
        Left eMsg -> errorWithoutStackTrace eMsg