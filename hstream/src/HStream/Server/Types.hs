{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE CPP              #-}
{-# LANGUAGE DeriveAnyClass   #-}
{-# LANGUAGE DeriveGeneric    #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs            #-}
{-# LANGUAGE RankNTypes       #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId)
import           Control.Concurrent.STM
import           Data.Aeson                       (FromJSON (..), ToJSON (..))
import qualified Data.Aeson                       as Aeson
import           Data.HashMap.Strict              (HashMap)
import qualified Data.HashMap.Strict              as HM
import qualified Data.Heap                        as Heap
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Map.Strict                  as M
import qualified Data.Set                         as Set
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import           Data.Word                        (Word32, Word64)
import qualified Database.RocksDB                 as RocksDB
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel           (StreamSend)
import qualified Proto3.Suite                     as PB

#if __GLASGOW_HASKELL__ < 902
import qualified HStream.Admin.Store.API          as AA
#endif
import           Data.IORef                       (IORef)
import           HStream.Base.Timer               (CompactedWorker)
import           HStream.Common.ConsistentHashing (HashRing)
import           HStream.Gossip.Types             (Epoch, GossipContext)
import qualified HStream.IO.Types                 as IO
import qualified HStream.IO.Worker                as IO
import           HStream.MetaStore.Types          (MetaHandle)
import           HStream.Server.Config
import           HStream.Server.ConnectorTypes    as HCT
import           HStream.Server.HStreamApi        (NodeState, ResourceType,
                                                   SpecialOffset (..),
                                                   StreamingFetchResponse,
                                                   Subscription (..))
import           HStream.Server.Shard             (ShardKey, SharedShardMap)
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as HS
import qualified HStream.Store                    as S
import           HStream.Utils                    (ResourceType (ResConnector),
                                                   textToCBytes,
                                                   timestampToMsTimestamp)

protocolVersion :: Text
protocolVersion = "0.1.0"

serverVersion :: Text
serverVersion = "0.9.1"

data SubscriptionWrap = SubscriptionWrap
  { originSub  :: Subscription
  , subOffsets :: HM.HashMap S.C_LogID S.LSN
  } deriving (Generic, Show, FromJSON, ToJSON)

renderSubscriptionWrapToTable :: [SubscriptionWrap] -> Aeson.Value
renderSubscriptionWrapToTable subs =
  let headers = ["Sub ID" :: Text, "StreamName", "AckTimeout", "Max Unacked Records", "CreatedTime", "OffsetType", "Offsets"]
      rows = map formatSubscriptionWrap subs
   in Aeson.object ["headers" Aeson..= headers, "rows" Aeson..= rows]
 where
   formatSubscriptionWrap SubscriptionWrap{originSub=Subscription{..}, ..} =
     let offset = case subscriptionOffset of
                    (PB.Enumerated (Right SpecialOffsetEARLIEST)) -> "EARLIEST"
                    (PB.Enumerated (Right SpecialOffsetLATEST))   -> "LATEST"
                    _                                             -> "UNKNOWN"
      in [ subscriptionSubscriptionId
         , subscriptionStreamName
         , T.pack . show $ subscriptionAckTimeoutSeconds
         , T.pack . show $ subscriptionMaxUnackedRecords
         , maybe "" (T.pack . show . timestampToMsTimestamp) subscriptionCreationTime
         , offset
         , T.pack . show . HM.toList $ subOffsets
         ]

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
  , metaHandle               :: MetaHandle
  , runningQueries           :: MVar (HM.HashMap Text (ThreadId, TVar Bool))
  , scSubscribeContexts      :: TVar (HM.HashMap SubscriptionId SubscribeContextNewWrapper)
  , cmpStrategy              :: HS.Compression
#if __GLASGOW_HASKELL__ < 902
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
#endif
  , scStatsHolder            :: Stats.StatsHolder
  , loadBalanceHashRing      :: TVar (Epoch, HashRing)
  , scIOWorker               :: IO.Worker
  , gossipContext            :: GossipContext
  , serverOpts               :: ServerOpts
  , shardReaderMap           :: MVar (HM.HashMap Text (MVar ShardReader))
  , querySnapshotPath        :: FilePath
  , querySnapshotter         :: Maybe RocksDB.DB
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
  | SubscribeStateFailed
  deriving (Eq, Show)

data SubscribeContext = SubscribeContext
  { subSubscriptionId    :: !T.Text
  , subStreamName        :: !T.Text
  , subAckTimeoutSeconds :: !Int32
  , subMaxUnackedRecords :: !Word32
  , subStartOffset       :: !(PB.Enumerated SpecialOffset)
  , subLdCkpReader       :: !HS.LDSyncCkpReader
  , subLdTrimCkpWorker   :: !CompactedWorker
  , subLdReader          :: !(MVar HS.LDReader)
  , subUnackedRecords    :: !(TVar Word32)
  , subConsumerContexts  :: !(TVar (HM.HashMap ConsumerName ConsumerContext))
  , subShardContexts     :: !(TVar (HM.HashMap HS.C_LogID SubscribeShardContext))
  , subAssignment        :: !Assignment
  , subCurrentTime       :: !(TVar Word64) -- unit: ms
  , subWaitingCheckedRecordIds      :: !(TVar (Heap.Heap CheckedRecordIds))
  , subStartOffsets      :: !(HM.HashMap S.C_LogID S.LSN)
  }

data CheckedRecordIds = CheckedRecordIds {
  crDeadline     :: Word64,
  crLogId        :: HS.C_LogID,
  crBatchId      :: Word64,
  crBatchIndexes :: TVar (Set.Set Word32)
}

instance Show CheckedRecordIds where
  show CheckedRecordIds{..} = "{ logId=" <> show crLogId
                           <> ", batchId=" <> show crBatchId
                           <> ", deadline=" <> show crDeadline
                           <> "}"

instance Eq CheckedRecordIds where
  (==) cr1 cr2 = crDeadline cr1 == crDeadline cr2
instance Ord CheckedRecordIds where
  (<=) cr1 cr2 | crDeadline cr1 /= crDeadline cr2 = crDeadline cr1 < crDeadline cr2
               | otherwise = crBatchId cr1 <= crBatchId cr2

data CheckedRecordIdsKey = CheckedRecordIdsKey {
  crkLogId   :: HS.C_LogID,
  crkBatchId :: Word64
} deriving (Eq, Ord)

instance Show CheckedRecordIdsKey where
  show CheckedRecordIdsKey{..} = show crkLogId <> "-" <> show crkBatchId

data ConsumerContext = ConsumerContext
  { ccConsumerName  :: ConsumerName,
    ccConsumerUri   :: Maybe Text,
    ccConsumerAgent :: Maybe Text,
    ccIsValid       :: TVar Bool,
    -- use MVar for streamSend because only on thread can use streamSend at the
    -- same time
    ccStreamSend    :: MVar (StreamSend StreamingFetchResponse)
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
} deriving (Eq, Ord)

instance Show ShardRecordId where
  show ShardRecordId{..} = show sriBatchId <> "-" <> show sriBatchIndex

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

-- Task Manager
class TaskManager tm where
  resourceType :: tm -> ResourceType
  listResources :: tm -> IO [T.Text]
  recoverTask :: tm -> T.Text -> IO ()

instance TaskManager IO.Worker where
  resourceType = const ResConnector
  listResources = IO.listResources
  recoverTask = IO.recoverTask

data ShardReader = ShardReader
  { shardReader             :: S.LDReader
  , shardReaderTotalBatches :: Maybe (IORef Word64)
  , shardReaderStartTs      :: Maybe Int64
  , shardReaderEndTs        :: Maybe Int64
  , targetShard             :: S.C_LogID
  }

mkShardReader :: S.LDReader -> S.C_LogID -> Maybe (IORef Word64) -> Maybe Int64 -> Maybe Int64 -> ShardReader
mkShardReader shardReader targetShard shardReaderTotalBatches shardReaderStartTs shardReaderEndTs = ShardReader {..}

data StreamReader = StreamReader
  { streamReader             :: S.LDReader
  , streamReaderTotalBatches :: Maybe (IORef Word64)
  , streamReaderTsLimits     :: HashMap S.C_LogID (Maybe Int64, Maybe Int64)
    -- ^ shardId -> (startTs, endTs)
  }

mkStreamReader :: S.LDReader ->  Maybe (IORef Word64) -> HashMap S.C_LogID (Maybe Int64, Maybe Int64) -> StreamReader
mkStreamReader streamReader streamReaderTotalBatches streamReaderTsLimits = StreamReader {..}

--------------------------------------------------------------------------------

transToStreamName :: HCT.StreamName -> S.StreamId
transToStreamName = S.mkStreamId S.StreamTypeStream . textToCBytes

transToTempStreamName :: HCT.StreamName -> S.StreamId
transToTempStreamName = S.mkStreamId S.StreamTypeTemp . textToCBytes

transToViewStreamName :: HCT.StreamName -> S.StreamId
transToViewStreamName = S.mkStreamId S.StreamTypeView . textToCBytes
