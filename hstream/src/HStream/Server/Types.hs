{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId)
import           Control.Concurrent.STM
import           Data.Hashable                    (hash)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Set                         as Set
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel           (StreamSend)
import qualified Proto3.Suite                     as PB
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Types                  (ZHandle)

import qualified HStream.Admin.Store.API          as AA
import           HStream.Common.ConsistentHashing (HashRing)
import           HStream.Gossip.Types             (GossipContext)
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (NodeState,
                                                   StreamingFetchResponse)
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as HS
import HStream.Connector.HStore (transToStreamName)
import           HStream.Utils                    (textToCBytes)

protocolVersion :: Text
protocolVersion = "0.1.0"

serverVersion :: Text
serverVersion = "0.8.0"

type Timestamp = Int64
type ServerID = Word32
type ServerState = PB.Enumerated NodeState

data ServerContext = ServerContext
  { scLDClient               :: HS.LDClient
  , serverID                 :: Word32
  , scAdvertisedListenersKey :: Maybe Text
  , scDefaultStreamRepFactor :: Int
  , scMaxRecordSize          :: Int
  , zkHandle                 :: ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
  , runningConnectors        :: MVar (HM.HashMap CB.CBytes ThreadId)
  , scSubscribeContexts      :: TVar (HM.HashMap SubscriptionId SubscribeContextNewWrapper)
  , cmpStrategy              :: HS.Compression
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
  , scStatsHolder            :: Stats.StatsHolder
  , loadBalanceHashRing      :: TVar HashRing
  , scServerState            :: MVar ServerState
  , gossipContext            :: GossipContext
}

data SubscribeContextNewWrapper = SubscribeContextNewWrapper
  { scnwState   :: TVar SubscribeState,
    scnwContext :: TMVar SubscribeContext
  }

data SubscribeContextWrapper = SubscribeContextWrapper
  { scwState   :: TVar SubscribeState,
    scwContext :: SubscribeContext
  }

data SubscribeState
  = SubscribeStateNew
  | SubscribeStateRunning
  | SubscribeStateStopping
  | SubscribeStateStopped
  deriving (Eq, Show)

data SubscribeContext = SubscribeContext
  { subSubscriptionId    :: T.Text,
    subStreamName        :: T.Text,
    subAckTimeoutSeconds :: Int32,
    subMaxUnackedRecords :: Word32,
    subLdCkpReader       :: HS.LDSyncCkpReader,
    subLdReader          :: MVar HS.LDReader,
    subUnackedRecords    :: TVar Word32,
    subConsumerContexts  :: TVar (HM.HashMap ConsumerName ConsumerContext),
    subShardContexts     :: TVar (HM.HashMap HS.C_LogID SubscribeShardContext),
    subAssignment        :: Assignment,
    subCurrentTime ::  TVar Word64,
    subWaitingCheckedRecordIds :: TVar [CheckedRecordIds],
    subWaitingCheckedRecordIdsIndex :: TVar (Map.Map CheckedRecordIdsKey CheckedRecordIds)
  }

data CheckedRecordIds = CheckedRecordIds {
  crDeadline     :: Word64,
  crLogId        :: HS.C_LogID,
  crBatchId      :: Word64,
  crBatchIndexes :: TVar (Set.Set Word32)
}

data CheckedRecordIdsKey = CheckedRecordIdsKey {
  crkLogId   :: HS.C_LogID,
  crkBatchId :: Word64
} deriving (Eq, Ord)

data ConsumerContext = ConsumerContext
  { ccConsumerName :: ConsumerName,
    ccIsValid      :: TVar Bool,
    -- use MVar for streamSend because only on thread can use streamSend at the
    -- same time
    ccStreamSend   :: MVar (StreamSend StreamingFetchResponse)
  }

data SubscribeShardContext = SubscribeShardContext
  { sscAckWindow :: AckWindow,
    sscLogId     :: HS.C_LogID
  }

data AckWindow = AckWindow
  { awWindowLowerBound :: TVar ShardRecordId,
    awWindowUpperBound :: TVar ShardRecordId,
    awAckedRanges      :: TVar (Map.Map ShardRecordId ShardRecordIdRange),
    awBatchNumMap      :: TVar (Map.Map Word64 Word32)
  }

data Assignment = Assignment
  { totalShards :: TVar (Set.Set HS.C_LogID),
    unassignedShards :: TVar [HS.C_LogID],
    waitingReadShards :: TVar [HS.C_LogID],
    waitingReassignedShards :: TVar [HS.C_LogID],
    waitingConsumers :: TVar [ConsumerName],
    shard2Consumer :: TVar (HM.HashMap HS.C_LogID ConsumerName),
    consumer2Shards :: TVar (HM.HashMap ConsumerName (TVar (Set.Set HS.C_LogID))),
    consumerWorkloads :: TVar (Set.Set ConsumerWorkload)
  }

data ConsumerWorkload = ConsumerWorkload
  { cwConsumerName :: ConsumerName,
    cwShardCount   :: Int
  } deriving (Show)
instance Eq ConsumerWorkload where
  (==) w1 w2 = cwConsumerName w1 == cwConsumerName w2 && cwShardCount w1 == cwShardCount w2
instance Ord ConsumerWorkload where
  (<=) w1 w2 = (cwShardCount w1, cwConsumerName w1) <= (cwShardCount w2, cwConsumerName w2)

type SubscriptionId = T.Text
type OrderingKey = T.Text

data ShardRecordId = ShardRecordId {
  sriBatchId    :: Word64,
  sriBatchIndex :: Word32
} deriving (Eq, Ord, Show)

instance Bounded ShardRecordId where
  minBound = ShardRecordId minBound minBound
  maxBound = ShardRecordId maxBound maxBound

data ShardRecordIdRange = ShardRecordIdRange
  { startRecordId :: ShardRecordId,
    endRecordId   :: ShardRecordId
  } deriving (Eq, Show)

printAckedRanges :: Map.Map ShardRecordId ShardRecordIdRange -> String
printAckedRanges mp = show (Map.elems mp)

type ConsumerName = T.Text

--------------------------------------------------------------------------------
-- shard

getShardName :: Int -> CB.CBytes
getShardName idx = textToCBytes $ "shard" <> T.pack (show idx)

getShard :: HS.LDClient -> HS.StreamId -> Maybe T.Text -> IO HS.C_LogID
getShard client streamId key = do
  partitions <- HS.listStreamPartitions client streamId
  let size = length partitions - 1
  let shard = getShardName . getShardIdx size <$> key
  Log.debug $ "assign key " <> Log.buildString' (show key) <> " to shard " <> Log.buildString' (getShardIdx size <$> key)
  HS.getUnderlyingLogId client streamId shard

getShardIdx :: Int -> T.Text -> Int
getShardIdx size key = let hashValue = hash key
                        in hashValue `mod` size

