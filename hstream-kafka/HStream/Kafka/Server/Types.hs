module HStream.Kafka.Server.Types
  ( ServerContext (..)
  , initServerContext
  , initConnectionContext
  ) where

import           Data.Text                               (Text)
import           Data.Word
import           Foreign.ForeignPtr                      (newForeignPtr_)
import           Foreign.Ptr                             (nullPtr)

import           HStream.Common.Server.HashRing          (LoadBalanceHashRing,
                                                          initializeHashRing)
import           HStream.Gossip.Types                    (GossipContext)
import           HStream.Kafka.Common.OffsetManager      (OffsetManager,
                                                          initOffsetReader,
                                                          newOffsetManager)
import           HStream.Kafka.Group.GroupCoordinator    (GroupCoordinator,
                                                          mkGroupCoordinator)
import           HStream.Kafka.Server.Config             (ServerOpts (..))
import qualified HStream.Kafka.Server.Config.KafkaConfig as KC
import           HStream.MetaStore.Types                 (MetaHandle)
import           HStream.Stats                           (newServerStatsHolder)
import qualified HStream.Stats                           as Stats
import qualified HStream.Store                           as S

data ServerContext = ServerContext
  { serverID                 :: !Word32
  , serverOpts               :: !ServerOpts
  , scAdvertisedListenersKey :: !(Maybe Text)
  , scMaxRecordSize          :: !Int
  , metaHandle               :: !MetaHandle
  , scStatsHolder            :: !Stats.StatsHolder
  , scLDClient               :: !S.LDClient
  , cmpStrategy              :: !S.Compression
  , loadBalanceHashRing      :: !LoadBalanceHashRing
  , gossipContext            :: !GossipContext
  , scGroupCoordinator       :: !GroupCoordinator
  , kafkaBrokerConfigs       :: !KC.KafkaBrokerConfigs
    -- { per connection, see 'initConnectionContext'
  , scOffsetManager          :: !OffsetManager
  , fetchReader              :: !S.LDReader
    -- } per connection end
  }

initServerContext
  :: ServerOpts
  -> GossipContext
  -> MetaHandle
  -> IO ServerContext
initServerContext opts@ServerOpts{..} gossipContext mh = do
  ldclient <- S.newLDClient _ldConfigPath
  -- Disable logdeivce crc checksum for kafka server, since we have checksum
  -- in kafka message header.
  S.setClientSetting ldclient "checksum-bits" "0"

  -- XXX: Should we add a server option to toggle Stats?
  statsHolder <- newServerStatsHolder
  epochHashRing <- initializeHashRing gossipContext
  scGroupCoordinator <- mkGroupCoordinator mh ldclient _serverID

  offsetManager <- newOffsetManager ldclient
  -- Trick to avoid use maybe, must be initialized later
  fetchReader <- newForeignPtr_ nullPtr

  return
    ServerContext
      { serverID                 = _serverID
      , serverOpts               = opts
      , scAdvertisedListenersKey = Nothing
      , scMaxRecordSize          = _maxRecordSize
      , metaHandle               = mh
      , scStatsHolder            = statsHolder
      , scLDClient               = ldclient
      , cmpStrategy              = _compression
      , loadBalanceHashRing      = epochHashRing
      , gossipContext            = gossipContext
      , scGroupCoordinator       = scGroupCoordinator
      , kafkaBrokerConfigs       = _kafkaBrokerConfigs
      , scOffsetManager          = offsetManager
      , fetchReader              = fetchReader
      }

initConnectionContext :: ServerContext -> IO ServerContext
initConnectionContext sc = do
  -- Since the Reader inside OffsetManger is thread-unsafe, for each connection
  -- we create a new Reader.
  !om <- initOffsetReader $ scOffsetManager sc

  -- Reader used for fetch.
  --
  -- Currently, we only need one reader per connection because there will be
  -- only one thread to fetch data.
  --
  -- TODO: also considering the following:
  --
  -- - use a pool of readers.
  -- - create a reader(or pool of readers) for each consumer group.
  --
  -- NOTE: the maxLogs is set to 1000, which means the reader will fetch at most
  -- 1000 logs.
  -- TODO: maybe we should set maxLogs dynamically according to the max number
  -- of all fetch requests in this connection.
  !reader <- S.newLDReader (scLDClient sc) 1000{-maxLogs-} (Just 10){-bufferSize-}

  pure sc{scOffsetManager = om, fetchReader = reader}
