{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId, newEmptyMVar)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.ByteString                  (ByteString)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import           Data.Word                        (Word32, Word64)
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel           (StreamSend)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB
import           Z.IO.Network                     (PortNumber)
import           ZooKeeper.Types                  (ZHandle)

import qualified HStream.Admin.Store.API          as AA
import           HStream.Common.ConsistentHashing (HashRing)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (RecordId (..),
                                                   StreamingFetchResponse (..),
                                                   WatchSubscriptionResponse (..))
import qualified HStream.Stats                    as Stats
import           HStream.Store                    (Compression)
import qualified HStream.Store                    as HS
import qualified HStream.Store.Logger             as Log

protocolVersion :: T.Text
protocolVersion = "0.1.0"

serverVersion :: T.Text
serverVersion = "0.7.0"

data ServerOpts = ServerOpts
  { _serverHost         :: CBytes
  , _serverPort         :: PortNumber
  , _serverAddress      :: String
  , _serverInternalPort :: PortNumber
  , _serverID           :: Word32
  , _zkUri              :: CBytes
  , _ldConfigPath       :: CBytes
  , _topicRepFactor     :: Int
  , _ckpRepFactor       :: Int
  , _compression        :: Compression
  , _ldAdminHost        :: ByteString
  , _ldAdminPort        :: Int
  , _ldAdminProtocolId  :: AA.ProtocolId
  , _ldAdminConnTimeout :: Int
  , _ldAdminSendTimeout :: Int
  , _ldAdminRecvTimeout :: Int
  , _serverLogLevel     :: Log.Level
  , _serverLogWithColor :: Bool
  , _ldLogLevel         :: Log.LDLogLevel
  } deriving (Show)

type Timestamp = Int64

type ServerID = Word32
type ServerRanking = [ServerID]

data ServerContext = ServerContext {
    scLDClient               :: HS.LDClient
  , serverID                 :: Word32
  , scDefaultStreamRepFactor :: Int
  , zkHandle                 :: ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
  , runningConnectors        :: MVar (HM.HashMap CB.CBytes ThreadId)
  , scSubscribeContexts      :: TVar (HM.HashMap SubscriptionId SubscribeContextNewWrapper)
  , cmpStrategy              :: HS.Compression
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
  , scStatsHolder            :: Stats.StatsHolder
  , loadBalanceHashRing      :: MVar HashRing
}

data SubscribeContextNewWrapper = SubscribeContextNewWrapper
  { scnwState :: TVar SubscribeState,
    scnwContext :: TVar (Maybe SubscribeContext)
  }

data SubscribeContextWrapper = SubscribeContextWrapper
  { scwState :: TVar SubscribeState,
    scwContext :: SubscribeContext
  }

data SubscribeState
  = SubscribeStateNew
  | SubscribeStateRunning
  | SubscribeStateStopping
  | SubscribeStateStopped
  deriving (Eq, Show)

data SubscribeContext = SubscribeContext
  { subSubscriptionId :: T.Text,
    subStreamName :: T.Text,
    subAckTimeoutSeconds :: Int32,
    subLdCkpReader :: HS.LdCkpReader,
    subLdReader :: HS.LdReader,
    subConsumerContexts :: TVar (HM.HashMap ConsumerName ConsumerContext),
    subShardContexts :: TVar (HM.HashMap HS.C_LogID SubscribeShardContext),
    subAssignment :: Assignment
  }

data ConsumerContext = ConsumerContext
  { ccConsumerName :: ConsumerName,
    ccIsValid :: TVar Bool,
    -- use MVar for streamSend because only on thread can use streamSend at the
    -- same time
    ccStreamSend :: MVar (StreamSend StreamingFetchResponse)
  }

data SubscribeShardContext = SubscribeShardContext
  { sscAckWindow :: AckWindow,
    sscLogId :: HS.C_LogID
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
    cwShardCount :: Int
  }
instance Eq ConsumerWorkload where
  (==) w1 w2 = cwConsumerName == cwConsumerName && cwShardCount w1 == cwShardCount w2 
instance Ord ConsumerWorkload where
  (<=) w1 w2 = w1 == w2 || cwShardCount w1 < cwShardCount w2 

type SubscriptionId = T.Text
type OrderingKey = T.Text

instance Bounded RecordId where
  minBound = RecordId minBound minBound
  maxBound = RecordId maxBound maxBound

data RecordIdRange = RecordIdRange
  { startRecordId :: RecordId,
    endRecordId   :: RecordId
  } deriving (Eq)

instance Show RecordIdRange where
  show RecordIdRange{..} = "{(" <> show (recordIdBatchId startRecordId) <> ","
                                <> show (recordIdBatchIndex startRecordId) <> "), ("
                                <> show (recordIdBatchId endRecordId) <> ","
                                <> show (recordIdBatchIndex endRecordId) <> ")}"

printAckedRanges :: Map.Map RecordId RecordIdRange -> String
printAckedRanges mp = show (Map.elems mp)

type ConsumerName = T.Text
