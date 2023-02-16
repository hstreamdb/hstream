{-# LANGUAGE PatternSynonyms #-}

module HStream.Gossip.Types
  ( module HStream.Gossip.Types
  , module G
  ) where

import           Control.Concurrent             (MVar, ThreadId)
import           Control.Concurrent.STM         (TChan, TMVar, TQueue, TVar)
import           Data.ByteString                (ByteString)
import qualified Data.IntMap.Strict             as IM
import qualified Data.Map                       as Map
import           Data.Map.Strict                (Map)
import           Data.Text                      (Text)
import           Data.Word                      (Word32, Word64)
import qualified Options.Applicative            as O
import           Options.Applicative.Builder    (auto, help, long, metavar,
                                                 option, short, showDefault,
                                                 strOption)
import           System.Random                  (StdGen)

import           HStream.ThirdParty.Protobuf    (Timestamp (..))

import           HStream.Gossip.HStreamGossip   as G (EventMessage (..),
                                                      Message (..),
                                                      MessageContent (..),
                                                      StateMessage (..),
                                                      StateMessageContent (..),
                                                      StateReport (..),
                                                      UserEvent (..))
import qualified HStream.Server.HStreamInternal as I

type ServerId  = Word32
type ServerUrl = Text
type Epoch     = Word32
type Incarnation = Word32
type LamportTime = Word64

-- The Server list for the library user
type ServerList  = (Epoch, Map ServerId ServerStatus)
type DeadServers = Map ServerId I.ServerNode
data ServerStatus = ServerStatus
  { serverInfo       :: I.ServerNode
  , serverState      :: TVar ServerState
  , latestMessage    :: TVar G.StateMessage
  , stateIncarnation :: TVar Incarnation
  }
data ServerState = ServerAlive | ServerSuspicious | ServerDead | ServerLeft
  deriving (Show, Eq)
-- data ServerState
--   = ServerNone
--   | ServerAlive
--   | ServerLeaving -- Not yet needed
--   | ServerLeft    -- Not yet needed
--   | ServerFailed
--   deriving (Show, Eq)

data InitType = User | Gossip | Self
type Workers       = Map ServerId ThreadId
type BroadcastPool = [(G.Message, Word32)]

data GossipContext = GossipContext
  { -- Server Context
    serverSelf    :: I.ServerNode
  , incarnation   :: TVar Word32
  , eventLpTime   :: TVar Word32
  , randomGen     :: StdGen
  , gossipOpts    :: GossipOpts

  -- Server List and Probe Context
  , serverList    :: TVar ServerList
  , workers       :: TVar Workers
  , deadServers   :: TVar DeadServers
  , actionChan    :: TChan RequestAction

  -- Event Context
  , eventHandlers :: EventHandlers
  , broadcastPool :: TVar BroadcastPool
  , seenEvents    :: TVar SeenEvents
  , eventPool     :: TQueue G.EventMessage
  , statePool     :: TQueue G.StateMessage

  -- Bootstrap Context
  , clusterReady  :: MVar ()
  , clusterInited :: MVar InitType
  , seedsInfo     :: MVar (Bool, [(ByteString, Int)], Bool)
  , seeds         :: [(ByteString, Int)]
  , numInited     :: MVar (Maybe (TVar Int))
  }

data StateAction = Join ServerId | Leave ServerId
type StateDelta  = Map ServerId (G.StateMessage, Bool)
type Messages = [G.Message]

type EventName = Text
type EventPayload = ByteString
type EventHandler = EventPayload -> IO ()
type EventHandlers = Map.Map Text EventHandler
type SeenEvents    = IM.IntMap [(EventName, EventPayload)]

data RequestAction
  = DoPing ServerId Messages
  | DoPingReq [ServerId] ServerStatus (TMVar ()) Messages
  | DoPingReqPing ServerId  (TMVar Messages) Messages
  | DoGossip [ServerId] Messages
instance Show RequestAction where
  show (DoPing        x y)       = "Send ping to"     <> show x <> " with message: " <> show y
  show (DoPingReq     ids x _ y) = "Send PingReq to " <> show ids <> " for " <> show (serverInfo x)
                                 <> " with message: " <> show y
  show (DoPingReqPing x _ y)     = "Received PingReq request to " <> show x
                                 <> " with message: " <> show y
  show (DoGossip      x y)       = "Received gossip request to " <> show x
                                 <> " with message: " <> show y

newtype TempCompare = TC {unTC :: G.StateMessage}
instance Eq TempCompare where
  TC (GAlive   i1 node1 _) == TC (GAlive   i2 node2 _) = i1 == i2 && node1 == node2
  TC (GSuspect i1 node1 _) == TC (GSuspect i2 node2 _) = i1 == i2 && node1 == node2
  TC (GConfirm i1 node1 _) == TC (GConfirm i2 node2 _) = i1 == i2 && node1 == node2
  _ == _                                     = False

instance Ord TempCompare where
  compare (TC x) (TC y) =
    if getMsgNode x /= getMsgNode y
    then error "You cannot compare two different node state"
    else case (x, y) of
      (GConfirm i1 _ _ , GConfirm i2 _ _ ) -> compare i1 i2
      (GConfirm{}, _)                     -> GT
      (GSuspect i1 _ _, GSuspect i2 _ _)   -> if i1 >  i2 then GT else LT
      (GAlive   i1 _ _, GAlive   i2 _ _)   -> if i1 >  i2 then GT else LT
      (GSuspect i1 _ _, GAlive   i2 _ _)   -> if i1 >= i2 then GT else LT
      (_, _)                             -> case compare (TC y) (TC x) of GT -> LT; LT -> GT; EQ -> EQ

getMsgNode :: G.StateMessage -> I.ServerNode
getMsgNode (GSuspect _ node _) = node
getMsgNode (GAlive   _ node _) = node
getMsgNode (GConfirm _ node _) = node
getMsgNode _                   = error "illegal state message"

pattern GEvent :: G.EventMessage -> G.Message
pattern GEvent x = G.Message (Just (G.MessageContentEvent x))

pattern GState :: G.StateMessage -> G.Message
pattern GState x = G.Message (Just (G.MessageContentState x))

pattern GSuspect, GAlive, GConfirm :: Word32 -> I.ServerNode -> I.ServerNode -> G.StateMessage
pattern GSuspect x y z = G.StateMessage (Just (G.StateMessageContentSuspect (GSM x y z)))
pattern GAlive   x y z = G.StateMessage (Just (G.StateMessageContentAlive   (GSM x y z)))
pattern GConfirm x y z = G.StateMessage (Just (G.StateMessageContentConfirm (GSM x y z)))

pattern GSM :: Word32 -> I.ServerNode -> I.ServerNode -> G.StateReport
pattern GSM x y z = G.StateReport
  { stateReportIncarnation = x
  , stateReportReportee = Just y
  , stateReportReporter = Just z
  }

data GossipOpts = GossipOpts
  { gossipFanout          :: Int
  , retransmitMult        :: Int
  , gossipInterval        :: Int
  , probeInterval         :: Int
  , roundtripTimeout      :: Int
  , joinWorkerConcurrency :: Int
  } deriving (Show, Eq)

defaultGossipOpts :: GossipOpts
defaultGossipOpts = GossipOpts
  { gossipFanout          = 3
  , retransmitMult        = 4
  , gossipInterval        = 1 * 1000 * 1000
  , probeInterval         = 2 * 1000 * 1000
  , roundtripTimeout      = 500 * 1000
  , joinWorkerConcurrency = 10
  }

-------------------------------------------------------------------------------

data CliOptions = CliOptions {
    _serverHost       :: !ByteString
  , _serverPort       :: !Word32
  , _serverGossipPort :: !Word32
  , _joinHost         :: !(Maybe ByteString)
  , _joinPort         :: !(Maybe Int)
  , _serverId         :: !ServerId
  }
  deriving Show

cliOpts :: O.Parser CliOptions
cliOpts = CliOptions
  <$> serverHost
  <*> serverPort
  <*> serverGossipPort
  <*> O.optional joinHost
  <*> O.optional joinPort
  <*> serverID

serverHost :: O.Parser ByteString
serverHost = strOption
  $  long "host" <> metavar "HOST" <> short 'h'
  <> showDefault
  <> help "server host value"

serverPort :: O.Parser Word32
serverPort = option auto
  $  long "port" <> short 'p'
  <> metavar "INT"
  <> help "server port value"

serverGossipPort :: O.Parser Word32
serverGossipPort = option auto
  $  long "gossip-port" <> short 'g'
  <> metavar "INT"
  <> help "server gossip port value"

joinHost :: O.Parser ByteString
joinHost = strOption
  $  long "target-host" <> metavar "HOST"
  <> showDefault
  <> help "host value to join"

joinPort :: O.Parser Int
joinPort = option auto
  $  long "target-port"
  <> metavar "INT"
  <> help "port value to join"

serverAddress :: O.Parser String
serverAddress = strOption
  $  long "address"
  <> metavar "ADDRESS"
  <> help "server address"

serverID :: O.Parser Word32
serverID = option auto
  $  long "server-id"
  <> metavar "UINT32"
  <> help "ID of the hstream server node"
