module HStream.Kafka.Server.Types
  ( ServerContext (..)
  , initServerContext
  ) where

import           Data.Text                               (Text)
import           Data.Word

import           HStream.Common.Server.HashRing          (LoadBalanceHashRing,
                                                          initializeHashRing)
import           HStream.Gossip.Types                    (GossipContext)
import           HStream.Kafka.Common.OffsetManager      (OffsetManager,
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
  , scDefaultTopicRepFactor  :: !Int
  , scDefaultPartitionNum    :: !Int
  , scMaxRecordSize          :: !Int
  , metaHandle               :: !MetaHandle
  , scStatsHolder            :: !Stats.StatsHolder
  , scLDClient               :: !S.LDClient
  , cmpStrategy              :: !S.Compression
  , loadBalanceHashRing      :: !LoadBalanceHashRing
  , gossipContext            :: !GossipContext
  , scOffsetManager          :: !OffsetManager
  , scGroupCoordinator       :: GroupCoordinator
  , kafkaBrokerConfigs       :: !KC.KafkaBrokerConfigs
}

initServerContext
  :: ServerOpts
  -> GossipContext
  -> MetaHandle
  -> IO ServerContext
initServerContext opts@ServerOpts{..} gossipContext mh = do
  ldclient <- S.newLDClient _ldConfigPath
  -- XXX: Should we add a server option to toggle Stats?
  statsHolder <- newServerStatsHolder
  epochHashRing <- initializeHashRing gossipContext
  offsetManager <- newOffsetManager ldclient
  scGroupCoordinator <- mkGroupCoordinator mh ldclient _serverID

  return
    ServerContext
      { serverID                 = _serverID
      , serverOpts               = opts
      , scAdvertisedListenersKey = Nothing
      , scDefaultTopicRepFactor  = _topicRepFactor
      , scDefaultPartitionNum    = _partitionNums
      , scMaxRecordSize          = _maxRecordSize
      , metaHandle               = mh
      , scStatsHolder            = statsHolder
      , scLDClient               = ldclient
      , cmpStrategy              = _compression
      , loadBalanceHashRing      = epochHashRing
      , gossipContext            = gossipContext
      , scOffsetManager          = offsetManager
      , scGroupCoordinator       = scGroupCoordinator
      , kafkaBrokerConfigs = _kafkaBrokerConfigs
      }
