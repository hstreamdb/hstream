{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId)
import           Control.Concurrent.STM
import           Data.Aeson                       (FromJSON (..), ToJSON (..))
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Map.Strict                  as M
import qualified Data.Set                         as Set
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import           Data.Word                        (Word32, Word64)
import           Network.GRPC.HighLevel           (StreamSend)
import qualified Proto3.Suite                     as PB
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper.Types                  (ZHandle)

import           GHC.Generics                     (Generic)
import qualified HStream.Admin.Store.API          as AA
import           HStream.Common.ConsistentHashing (HashRing)
import           HStream.Connector.Type           as HCT
import           HStream.Gossip.Types             (GossipContext)
import qualified HStream.IO.Worker                as IO
import           HStream.Server.Config
import           HStream.Server.HStreamApi        (NodeState,
                                                   StreamingFetchResponse,
                                                   Subscription)
import           HStream.Server.Shard             (ShardKey, SharedShardMap)
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as HS
import qualified HStream.Store                    as S
import           HStream.Utils                    (textToCBytes)

protocolVersion :: Text
protocolVersion = "0.1.0"

serverVersion :: Text
serverVersion = "0.9.0"

data SubscriptionWrap = SubscriptionWrap
  { originSub  :: Subscription
  , subOffsets :: HM.HashMap S.C_LogID S.LSN
  } deriving (Generic, Show, FromJSON, ToJSON)

type Timestamp = Int64
type ServerID = Word32
type ServerState = PB.Enumerated NodeState
type ShardDict = M.Map ShardKey HS.C_LogID

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
  , scIOWorker               :: IO.Worker
  , gossipContext            :: GossipContext
  , serverOpts               :: ServerOpts
  , shardInfo                :: MVar (HM.HashMap Text SharedShardMap)
    -- ^ streamName -> ShardMap, use to manipulate shards
  , shardTable               :: MVar (HM.HashMap Text ShardDict)
    -- ^ streamName -> Map startKey shardId, use to find target shard quickly when append
  , shardReaderMap           :: MVar (HM.HashMap Text (MVar S.LDReader))
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
    subWaitingCheckedRecordIdsIndex :: TVar (Map.Map CheckedRecordIdsKey CheckedRecordIds),
    subStartOffset       :: HM.HashMap S.C_LogID S.LSN
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

transToStreamName :: HCT.StreamName -> S.StreamId
transToStreamName = S.mkStreamId S.StreamTypeStream . textToCBytes

transToTempStreamName :: HCT.StreamName -> S.StreamId
transToTempStreamName = S.mkStreamId S.StreamTypeTemp . textToCBytes

transToViewStreamName :: HCT.StreamName -> S.StreamId
transToViewStreamName = S.mkStreamId S.StreamTypeView . textToCBytes
