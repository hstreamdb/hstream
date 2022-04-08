{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId)
import           Control.Concurrent.STM
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel           (StreamSend)
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Types                  (ZHandle)

import qualified HStream.Admin.Store.API          as AA
import           HStream.Common.ConsistentHashing (HashRing)
import           HStream.Server.HStreamApi        (NodeState,
                                                   StreamingFetchResponse)
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as HS
import qualified Proto3.Suite                     as PB

protocolVersion :: T.Text
protocolVersion = "0.1.0"

serverVersion :: T.Text
serverVersion = "0.7.0"

type Timestamp = Int64
type ServerID = Word32
type ServerState = PB.Enumerated NodeState

data ServerContext = ServerContext {
    scLDClient               :: HS.LDClient
  , serverID                 :: Word32
  , scDefaultStreamRepFactor :: Int
  , scMaxRecordSize          :: Int
  , zkHandle                 :: ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
  , runningConnectors        :: MVar (HM.HashMap CB.CBytes ThreadId)
  , scSubscribeContexts      :: TVar (HM.HashMap SubscriptionId SubscribeContextNewWrapper)
  , cmpStrategy              :: HS.Compression
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
  , scStatsHolder            :: Stats.StatsHolder
  , loadBalanceHashRing      :: MVar HashRing
  , scServerState            :: MVar ServerState
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
    subAssignment        :: Assignment
  }

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
  }
instance Eq ConsumerWorkload where
  (==) w1 w2 = cwConsumerName w1 == cwConsumerName w2 && cwShardCount w1 == cwShardCount w2
instance Ord ConsumerWorkload where
  (<=) w1 w2 = w1 == w2 || cwShardCount w1 <= cwShardCount w2

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
