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
import           HStream.Kafka.Common.FetchManager       (FetchContext,
                                                          fakeFetchContext,
                                                          initFetchContext)
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
  , fetchCtx                 :: !FetchContext
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

  -- must be initialized later
  offsetManager <- newOffsetManager ldclient
  -- Trick to avoid use maybe, must be initialized later
  fetchCtx <- fakeFetchContext

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
      , fetchCtx                 = fetchCtx
      }

initConnectionContext :: ServerContext -> IO ServerContext
initConnectionContext sc = do
  -- Since the Reader inside OffsetManger is thread-unsafe, for each connection
  -- we create a new Reader.
  !om <- initOffsetReader $ scOffsetManager sc
  !fc <- initFetchContext (scLDClient sc)

  pure sc{scOffsetManager = om, fetchCtx = fc}
