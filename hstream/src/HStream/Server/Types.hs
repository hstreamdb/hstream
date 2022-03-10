{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Types where

import           Control.Concurrent               (MVar, ThreadId, newEmptyMVar)
import           Data.ByteString                  (ByteString)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32, Int64)
import qualified Data.Map                         as Map
import qualified Data.Set                         as Set
import qualified Data.Text                        as T
import           Data.Word                        (Word32, Word64)
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
  , _tlsConfig          :: Maybe TlsConfig
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
  , scSubscribeRuntimeInfo   :: MVar (HM.HashMap SubscriptionId SubscribeRuntimeInfo)
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
  , sriStreamName :: T.Text
  , sriWatchContext :: MVar WatchContext
  , sriShardRuntimeInfo :: MVar (HM.HashMap OrderingKey (MVar ShardSubscribeRuntimeInfo))
}

data WatchContext = WatchContext {
    wcWaitingConsumers :: [ConsumerWatch]
  , wcWorkingConsumers :: Set.Set ConsumerWorkload
  , wcWatchStopSignals :: HM.HashMap ConsumerName (MVar ())
}

addNewConsumerToCtx :: WatchContext -> ConsumerName -> StreamSend WatchSubscriptionResponse -> IO(WatchContext, MVar ())
addNewConsumerToCtx ctx@WatchContext{..} name streamSend = do
  stopSignal <- newEmptyMVar
  let signals = HM.insert name stopSignal wcWatchStopSignals
  let consumerWatch = mkConsumerWatch name streamSend
  let newCtx = ctx
        { wcWaitingConsumers = wcWaitingConsumers ++ [consumerWatch]
        , wcWatchStopSignals = signals
        }
  return (newCtx, stopSignal)

removeConsumerFromCtx :: WatchContext -> ConsumerName -> IO WatchContext
removeConsumerFromCtx ctx@WatchContext{..} name =
  return ctx {wcWatchStopSignals = HM.delete name wcWatchStopSignals}

data ConsumerWatch = ConsumerWatch {
    cwConsumerName :: ConsumerName
  , cwWatchStream  :: StreamSend WatchSubscriptionResponse
}

mkConsumerWatch :: ConsumerName -> StreamSend WatchSubscriptionResponse -> ConsumerWatch
mkConsumerWatch = ConsumerWatch

data ConsumerWorkload = ConsumerWorkload {
    cwConsumerWatch :: ConsumerWatch
  , cwShards        :: Set.Set OrderingKey
}

mkConsumerWorkload :: ConsumerWatch -> Set.Set OrderingKey -> ConsumerWorkload
mkConsumerWorkload = ConsumerWorkload

instance Eq ConsumerWorkload where
  (==) w1 w2 = Set.size (cwShards w1) == Set.size (cwShards w2)
instance Ord ConsumerWorkload where
  (<=) w1 w2 = Set.size (cwShards w1) <= Set.size (cwShards w2)

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
  , ssriSendStatus        :: SendStatus
}

data SendStatus = SendStopping
                | SendStopped
                | SendRunning

data TlsConfig
  = TlsConfig {
    keyPath  :: String
  , certPath :: String
  , caPath   :: Maybe String
  } deriving (Show)
