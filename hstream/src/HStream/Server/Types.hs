{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DeriveAnyClass    #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.ByteString                  (ByteString)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Set                         as Set 
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel           (StreamSend)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB
import           Z.IO.Network                     (PortNumber)
import           ZooKeeper.Types                  (ZHandle)

import           HStream.Common.ConsistentHashing (HashRing)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (RecordId (..),
                                                   StreamingFetchResponse (..),
                                                   WatchSubscriptionResponse (..))
import qualified HStream.Stats                    as Stats
import           HStream.Store                    (Compression)
import qualified HStream.Store                    as HS
import qualified HStream.Store.Admin.API          as AA
import qualified HStream.Store.Logger             as Log

protocolVersion :: T.Text
protocolVersion = "0.1.0"

serverVersion :: T.Text
serverVersion = "0.6.0"

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
  --, subscribeRuntimeInfo     :: MVar (HM.HashMap SubscriptionId (MVar SubscribeRuntimeInfo))
  , scSubscribeRuntimeInfo   :: MVar (HM.HashMap SubscriptionId (MVar SubscribeRuntimeInfo))
  , cmpStrategy              :: HS.Compression
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
  , scStatsHolder            :: Stats.StatsHolder
  , loadBalanceHashRing      :: MVar HashRing
}

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

-- data SubscribeRuntimeInfo = SubscribeRuntimeInfo {
--     sriStreamName        :: T.Text
--   , sriLogId             :: HS.C_LogID
--   , sriAckTimeoutSeconds :: Int32
--   , sriLdCkpReader       :: HS.LDSyncCkpReader
--   , sriLdReader          :: Maybe HS.LDReader
--   , sriWindowLowerBound  :: RecordId
--   , sriWindowUpperBound  :: RecordId
--   , sriAckedRanges       :: Map.Map RecordId RecordIdRange
--   , sriBatchNumMap       :: Map.Map Word64 Word32
--   , sriStreamSends       :: HM.HashMap ConsumerName (StreamSend StreamingFetchResponse)
--   , sriValid             :: Bool
--   , sriSignals           :: V.Vector (MVar ())
-- }

data SubscribeRuntimeInfo = SubscribeRuntimeInfo {
    sriSubscriptionId :: SubscriptionId 
  , sriWaitingConsumers    :: [ConsumerWatch]
  , sriWorkingConsumers    :: Set.Set ConsumerWorkload 
  , sriAssignments ::  HM.HashMap OrderingKey ConsumerWatch  
  , sriShardRuntimeInfo :: HM.HashMap OrderingKey (MVar ShardSubscribeRuntimeInfo)
}

data ConsumerWatch = ConsumerWatch {
  cwConsumerName :: ConsumerName,
  cwWatchStream :: StreamSend WatchSubscriptionResponse 
}

data ConsumerWorkload = ConsumerWorkload {
    cwConsumerWatch :: ConsumerWatch 
  , cwShardCount :: Int
}

instance Eq ConsumerWorkload where   
  (==) w1 w2 = cwShardCount w1 == cwShardCount w2 
instance Ord ConsumerWorkload where  
  (<=) w1 w2 = cwShardCount w1 <= cwShardCount w2 

data ShardSubscribeRuntimeInfo = ShardSubscribeRuntimeInfo {
    ssriStreamName        :: T.Text
  , ssriLogId             :: HS.C_LogID
  , ssriAckTimeoutSeconds :: Int32
  , ssriLdCkpReader       :: HS.LDSyncCkpReader
  , ssriLdReader          :: HS.LDReader
  , ssriWindowLowerBound  :: RecordId
  , ssriWindowUpperBound  :: RecordId
  , ssriAckedRanges       :: Map.Map RecordId RecordIdRange
  , ssriBatchNumMap       :: Map.Map Word64 Word32
  , ssriConsumerName      :: ConsumerName 
  , ssriStreamSend        :: StreamSend StreamingFetchResponse 
  , ssriValid             :: Bool
}

data ShardSubscriptionInfo = ShardSubscriptionInfo {
    ssiStreamName        :: T.Text
  , ssiLogId             :: HS.C_LogID
  , ssiAckTimeoutSeconds :: Int32
  , ssiLdCkpReader       :: HS.LDSyncCkpReader
  , ssiLdReader          :: Maybe HS.LDReader
}

data LoadReport = LoadReport {
    sysResUsage    :: SystemResourceUsage
  , sysResPctUsage :: SystemResourcePercentageUsage
  , isUnderloaded  :: Bool
  , isOverloaded   :: Bool
  } deriving (Eq, Generic, Show)
instance FromJSON LoadReport
instance ToJSON LoadReport

data SystemResourceUsage
  = SystemResourceUsage {
    cpuUsage      :: (Integer, Integer)
  , txTotal       :: Integer
  , rxTotal       :: Integer
  , collectedTime :: Integer
  } deriving (Eq, Generic, Show, FromJSON, ToJSON)

data SystemResourcePercentageUsage =
  SystemResourcePercentageUsage {
    cpuPctUsage       :: Double
  , memoryPctUsage    :: Double
  , bandwidthInUsage  :: Double
  , bandwidthOutUsage :: Double
  } deriving (Eq, Generic, Show)
instance FromJSON SystemResourcePercentageUsage
instance ToJSON SystemResourcePercentageUsage
