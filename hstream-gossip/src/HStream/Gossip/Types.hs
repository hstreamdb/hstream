{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE StandaloneDeriving #-}

module HStream.Gossip.Types where


import           Control.Concurrent.Async     (Async)
import           Control.Concurrent.STM       (TChan, TMVar, TQueue, TVar)
import           Data.ByteString              (ByteString)
import qualified Data.IntMap.Strict           as IM
import qualified Data.Map                     as Map
import           Data.Map.Strict              (Map)
import           Data.Serialize               (Serialize, decode)
import           Data.Word                    (Word32, Word64)
import           GHC.Generics                 (Generic)
import qualified Options.Applicative          as O
import           Options.Applicative.Builder  (auto, help, long, metavar,
                                               option, short, showDefault,
                                               strOption)
import           System.Random                (StdGen)

import           Data.Text                    (Text)
import           HStream.Gossip.HStreamGossip (ServerNodeInternal)

type ServerId  = Word32
type ServerUrl = Text
data ServerStatus = ServerStatus {
    serverInfo    :: ServerNodeInternal
  , serverState   :: TVar ServerState
  , latestMessage :: TVar StateMessage
  }

type ServerList    = Map ServerId ServerStatus
type Workers       = Map ServerId (Async ())
type BroadcastPool = [(Message, Word32)]
type StateDelta    = Map ServerId (StateMessage, Bool)
type EventHandlers = Map.Map EventName EventHandler
type SeenEvents    = IM.IntMap [(ByteString, ByteString)]

data GossipOpts = GossipOpts {
    gossipFanout     :: Int -- default: 3
  , retransmitMult   :: Int -- default: 4
  , gossipInterval   :: Int -- default: 500 * 1000
  , probeInterval    :: Int -- default: 1 * 1000 * 1000
  , roundtripTimeout :: Int -- default: 500 * 1000
  } deriving (Show, Eq)

data ServerState = OK | Suspicious
  deriving (Show, Eq)

data GossipContext = GossipContext {
    serverSelf    :: ServerNodeInternal
  , eventHandlers :: EventHandlers
  , serverList    :: TVar ServerList
  , actionChan    :: TChan RequestAction
  , statePool     :: TQueue StateMessage
  , eventPool     :: TQueue EventMessage
  , seenEvents    :: TVar SeenEvents
  , broadcastPool :: TVar BroadcastPool
  , workers       :: TVar Workers
  , incarnation   :: TVar Word32
  , eventLpTime   :: TVar LamportTime
  , randomGen     :: StdGen
  , gossipOpts    :: GossipOpts
  }

data CliOptions = CliOptions {
    _serverHost       :: !ByteString
  , _serverPort       :: !Word32
  , _serverGossipPort :: !Word32
  , _joinHost         :: !(Maybe ByteString)
  , _joinPort         :: !(Maybe Int)
  , _serverId         :: !ServerId
  }
  deriving Show

data RequestAction
  = DoPing ServerId ByteString
  | DoPingReq [ServerId] ServerStatus (TMVar ()) ByteString
  | DoPingReqPing ServerId  (TMVar ByteString) ByteString
  | DoGossip [ServerId] ByteString
instance Show RequestAction where
  show (DoPing        x y)       = "Send ping to"     <> show x <> " with message: " <> show (decode y :: Either String Message)
  show (DoPingReq     ids x _ y) = "Send PingReq to " <> show ids <> " for " <> show (serverInfo x)
                                 <> " with message: " <> show (decode y :: Either String Message)
  show (DoPingReqPing x _ y)     = "Received PingReq request to " <> show x
                                 <> " with message: " <> show (decode y :: Either String Message)
  show (DoGossip      x y)       = "Received gossip request to " <> show x
                                 <> " with message: " <> show (decode y :: Either String Message)

-- instance Serialize Text where
--   put = put . encodeUtf8
--   get = fmap decodeUtf8 get

deriving instance Serialize ServerNodeInternal

data Message
  = EventMessage { eventMessage :: EventMessage }
  | StateMessage { stateMessage :: StateMessage }
  deriving (Show, Eq, Generic, Serialize)

type LamportTime = Word64

type EventName = ByteString
type EventPayload = ByteString
type EventHandler = EventPayload -> IO ()
data EventMessage = Event EventName LamportTime EventPayload
  deriving (Show, Eq, Generic, Serialize)

data StateMessage
  = Suspect Word32 ServerNodeInternal ServerNodeInternal
  | Alive   Word32 ServerNodeInternal ServerNodeInternal
  | Confirm Word32 ServerNodeInternal ServerNodeInternal
  | Join    ServerNodeInternal
  deriving (Show, Generic, Serialize)

instance Eq StateMessage where
  Join node1         == Join node2         = node1 == node2
  Suspect i1 node1 _ == Suspect i2 node2 _ = i1 == i2 && node1 == node2
  Alive   i1 node1 _ == Alive   i2 node2 _ = i1 == i2 && node1 == node2
  Confirm i1 node1 _ == Confirm i2 node2 _ = i1 == i2 && node1 == node2
  _ == _                                   = False

instance Ord StateMessage where
  compare x y =
    if getMsgNode x /= getMsgNode y
    then error "You cannot compare two different node state"
    else case (x, y) of
      (Join    _, _)                     -> LT
      (Confirm i1 _ _ , Confirm i2 _ _ ) -> compare i1 i2
      (Confirm{}, _)                     -> GT
      (Suspect i1 _ _, Suspect i2 _ _)   -> if i1 >  i2 then GT else LT
      (Alive   i1 _ _, Alive   i2 _ _)   -> if i1 >  i2 then GT else LT
      (Suspect i1 _ _, Alive   i2 _ _)   -> if i1 >= i2 then GT else LT
      (_, _)                             -> case compare y x of GT -> LT; LT -> GT; EQ -> EQ

getMsgNode :: StateMessage -> ServerNodeInternal
getMsgNode (Join node)        = node
getMsgNode (Suspect _ node _) = node
getMsgNode (Alive   _ node _) = node
getMsgNode (Confirm _ node _) = node

-------------------------------------------------------------------------------

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
