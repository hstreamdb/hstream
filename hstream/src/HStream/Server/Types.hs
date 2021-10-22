{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric  #-}

module HStream.Server.Types where

import           Control.Concurrent        (MVar, ThreadId)
import           Data.Aeson                (FromJSON, ToJSON)
import           Data.ByteString           (ByteString)
import qualified Data.HashMap.Strict       as HM
import           Data.Int                  (Int32, Int64)
import           Data.Map                  (Map)
import qualified Data.Map                  as Map
import           Data.Set                  (Set)
import qualified Data.Text                 as T
import qualified Data.Text.Lazy            as TL
import qualified Data.Vector               as V
import           Data.Word                 (Word32, Word64)
import           GHC.Generics              (Generic)
import           Network.GRPC.HighLevel    (StreamSend)
import           Z.Data.CBytes             (CBytes)
import qualified Z.Data.CBytes             as CB
import           Z.IO.Network              (PortNumber)
import           ZooKeeper.Types           (ZHandle)

import qualified HStream.Logger            as Log
import           HStream.Server.HStreamApi (RecordId (..),
                                            StreamingFetchResponse (..))
import qualified HStream.Stats             as Stats
import           HStream.Store             (Compression)
import qualified HStream.Store             as HS
import qualified HStream.Store.Admin.API   as AA

data ServerOpts = ServerOpts
  { _serverHost         :: CBytes
  , _serverPort         :: PortNumber
  , _serverAddress      :: String
  , _serverInternalPort :: PortNumber
  , _serverName         :: CBytes
  , _zkUri              :: CBytes
  , _ldConfigPath       :: CBytes
  , _topicRepFactor     :: Int
  , _ckpRepFactor       :: Int
  , _heartbeatTimeout   :: Int64
  , _compression        :: Compression
  , _ldAdminHost        :: ByteString
  , _ldAdminPort        :: Int
  , _ldAdminProtocolId  :: AA.ProtocolId
  , _ldAdminConnTimeout :: Int
  , _ldAdminSendTimeout :: Int
  , _ldAdminRecvTimeout :: Int
  , _serverLogLevel     :: Log.Level
  , _serverLogWithColor :: Bool
  } deriving (Show)

type Timestamp = Int64

type ServerName = CBytes
type ServerRanking = [ServerName]

data ServerContext = ServerContext {
    scLDClient               :: HS.LDClient
  , serverName               :: CBytes
  , scDefaultStreamRepFactor :: Int
  , minServers               :: Int
  , leaderName               :: MVar CBytes
  , zkHandle                 :: ZHandle
  , runningQueries           :: MVar (HM.HashMap CB.CBytes ThreadId)
  , runningConnectors        :: MVar (HM.HashMap CB.CBytes ThreadId)
  , subscribeRuntimeInfo     :: MVar (HM.HashMap SubscriptionId (MVar SubscribeRuntimeInfo))
  , subscriptionCtx          :: MVar (Map String SubscriptionContext)
  , cmpStrategy              :: HS.Compression
  , headerConfig             :: AA.HeaderConfig AA.AdminAPI
  , scStatsHolder            :: Stats.StatsHolder
}

type SubscriptionId = TL.Text

instance Bounded RecordId where
  minBound = RecordId minBound minBound
  maxBound = RecordId maxBound maxBound

data RecordIdRange = RecordIdRange
  { startRecordId :: RecordId,
    endRecordId   :: RecordId
  } deriving (Show, Eq)

type ConsumerName = TL.Text

data SubscribeRuntimeInfo = SubscribeRuntimeInfo {
    sriStreamName        :: T.Text
  , sriLogId             :: HS.C_LogID
  , sriAckTimeoutSeconds :: Int32
  , sriLdCkpReader       :: HS.LDSyncCkpReader
  , sriLdReader          :: Maybe HS.LDReader
  , sriWindowLowerBound  :: RecordId
  , sriWindowUpperBound  :: RecordId
  , sriAckedRanges       :: Map.Map RecordId RecordIdRange
  , sriBatchNumMap       :: Map.Map Word64 Word32
  , sriStreamSends       :: HM.HashMap ConsumerName (StreamSend StreamingFetchResponse)
  , sriValid             :: Bool
  , sriSignals           :: V.Vector (MVar ())
}

type ServerLoadReports = HM.HashMap ServerName LoadReport

data LoadManager = LoadManager {
    sName           :: ServerName
  , lastSysResUsage :: MVar SystemResourceUsage
  , loadReport      :: MVar LoadReport
  , loadReports     :: MVar ServerLoadReports
}

data LoadReport = LoadReport {
    systemResourceUsage :: SystemResourcePercentageUsage
  , isUnderloaded       :: Bool
  , isOverloaded        :: Bool
  } deriving (Eq, Generic, Show)
instance FromJSON LoadReport
instance ToJSON LoadReport

data SystemResourceUsage
  = SystemResourceUsage {
    cpuUsage      :: (Integer, Integer)
  , txTotal       :: Integer
  , rxTotal       :: Integer
  , collectedTime :: Integer
  } deriving (Eq, Generic, Show)

data SystemResourcePercentageUsage =
  SystemResourcePercentageUsage {
    cpuPctUsage       :: Double
  , memoryPctUsage    :: Double
  , bandwidthInUsage  :: Double
  , bandwidthOutUsage :: Double
  } deriving (Eq, Generic, Show)
instance FromJSON SystemResourcePercentageUsage
instance ToJSON SystemResourcePercentageUsage

data SubscriptionContext = SubscriptionContext
  { _subctxSubId     :: String
  , _subctxStream    :: String
  , _subctxOffset    :: Int64
  , _subctxNode      :: String
  , _subctxCurOffset :: Int64
  , _subctxClients   :: Set String
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

data ProducerContext = ProducerContext
  { _prdctxStream :: String
  , _prdctxNode   :: String
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)
