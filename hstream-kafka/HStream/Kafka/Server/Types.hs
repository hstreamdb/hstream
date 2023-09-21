module HStream.Kafka.Server.Types
  ( ServerContext (..)
  , initServerContext

  , transToStreamName
  ) where

import           Control.Concurrent.STM
import           Data.Text                          (Text)
import           Data.Word

import           HStream.Common.ConsistentHashing   (HashRing)
import           HStream.Common.Server.HashRing     (LoadBalanceHashRing,
                                                     initializeHashRing)
import           HStream.Gossip.Types               (Epoch, GossipContext)
import           HStream.Kafka.Common.OffsetManager (OffsetManager,
                                                     newOffsetManager)
import           HStream.Kafka.Server.Config        (ServerOpts (..))
import qualified HStream.Logger                     as Log
import           HStream.MetaStore.Types            (MetaHandle)
import           HStream.Stats                      (newServerStatsHolder)
import qualified HStream.Stats                      as Stats
import qualified HStream.Store                      as S
import           HStream.Utils                      (textToCBytes)

data ServerContext = ServerContext
  { serverID                 :: !Word32
  , serverOpts               :: !ServerOpts
  , scAdvertisedListenersKey :: !(Maybe Text)
  , scDefaultStreamRepFactor :: !Int
  , scMaxRecordSize          :: !Int
  , metaHandle               :: !MetaHandle
  , scStatsHolder            :: !Stats.StatsHolder
  , scLDClient               :: !S.LDClient
  , cmpStrategy              :: !S.Compression
  , loadBalanceHashRing      :: !LoadBalanceHashRing
  , gossipContext            :: !GossipContext
  , scOffsetManager          :: !OffsetManager
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
  offsetManager <- newOffsetManager ldclient 1000{- TODO: maxLogs -}

  return
    ServerContext
      { serverID                 = _serverID
      , serverOpts               = opts
      , scAdvertisedListenersKey = Nothing
      , scDefaultStreamRepFactor = _topicRepFactor
      , scMaxRecordSize          = _maxRecordSize
      , metaHandle               = mh
      , scStatsHolder            = statsHolder
      , scLDClient               = ldclient
      , cmpStrategy              = _compression
      , loadBalanceHashRing      = epochHashRing
      , gossipContext            = gossipContext
      , scOffsetManager          = offsetManager
      }

transToStreamName :: Text -> S.StreamId
transToStreamName = S.mkStreamId S.StreamTypeStream . textToCBytes
