{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Utils where

import           Control.Concurrent
import           Control.Concurrent.STM           (STM, TQueue, TVar,
                                                   atomically, newTVar,
                                                   readTVar, readTVarIO,
                                                   stateTVar, writeTQueue,
                                                   writeTVar)
import           Control.Exception                (Handler (..))
import           Control.Exception.Base
import           Control.Monad                    (filterM, unless)
import           Data.ByteString                  (ByteString)
import           Data.Foldable                    (foldl')
import           Data.Functor
import qualified Data.HashMap.Strict              as HM
import qualified Data.Map.Strict                  as Map
import           Data.String                      (IsString (fromString))
import           Data.Text                        (Text)
import           Data.Time.Clock.System
import           Data.Word                        (Word32)
import qualified HsGrpc.Server.Types              as HsGrpc
import           Network.GRPC.HighLevel.Generated (ClientConfig (..),
                                                   ClientRequest (..),
                                                   GRPCMethodType (..),
                                                   Host (..), Port (..),
                                                   ServerResponse (..),
                                                   StatusCode (..),
                                                   StatusDetails (..))

import           HStream.Common.Types             (fromInternalServerNode)
import qualified HStream.Exception                as HE
import           HStream.Gossip.Types             (BroadcastPool, EventHandler,
                                                   EventMessage (..), EventName,
                                                   GossipContext (..),
                                                   InitType (..), Message (..),
                                                   Messages, SeenEvents,
                                                   ServerState (..),
                                                   ServerStatus (..),
                                                   StateDelta,
                                                   StateMessage (..),
                                                   TempCompare (TC), getMsgNode)
import qualified HStream.Gossip.Types             as T
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (NodeState (..),
                                                   ServerNode (..),
                                                   ServerNodeStatus (..))
import qualified HStream.Server.HStreamInternal   as I
import           HStream.Utils                    (pattern EnumPB)

initServerStatus :: I.ServerNode -> SystemTime -> STM ServerStatus
initServerStatus serverInfo now = do
  serverState      <- newTVar ServerDead
  stateIncarnation <- newTVar 0
  stateChange      <- newTVar now
  return $ ServerStatus {..}

returnResp :: Monad m => a -> m (ServerResponse 'Normal a)
returnResp resp = return (ServerNormalResponse (Just resp) mempty StatusOk "")

mkServerErrResp :: StatusCode -> StatusDetails -> ServerResponse 'Normal a
mkServerErrResp = ServerNormalResponse Nothing mempty

returnErrResp :: Monad m
  => StatusCode -> StatusDetails -> m (ServerResponse 'Normal a)
returnErrResp = (return .) . mkServerErrResp

mkGRPCClientConf :: I.ServerNode -> ClientConfig
mkGRPCClientConf I.ServerNode{..} =
  mkGRPCClientConf' serverNodeGossipAddress (fromIntegral serverNodeGossipPort)

mkGRPCClientConf' :: ByteString -> Int -> ClientConfig
mkGRPCClientConf' host port =
    ClientConfig
    { clientServerHost = Host host
    , clientServerPort = Port port
    , clientArgs = []
    , clientSSLConfig = Nothing
    , clientAuthority = Nothing
    }

mkClientNormalRequest :: req -> ClientRequest 'Normal req resp
mkClientNormalRequest x = ClientNormalRequest x requestTimeout mempty

mkClientNormalRequest' :: req -> Int -> ClientRequest 'Normal req resp
mkClientNormalRequest' x tout = ClientNormalRequest x tout mempty

requestTimeout :: Int
requestTimeout = 100

getMsgInc :: StateMessage -> Word32
getMsgInc (T.GSuspect inc _ _) = inc
getMsgInc (T.GAlive   inc _ _) = inc
getMsgInc (T.GConfirm inc _ _) = inc
getMsgInc _                    = error "illegal state message"

broadcast :: Messages -> TQueue StateMessage -> TQueue EventMessage ->  STM ()
broadcast msgs statePool eventPool = unless (null msgs) $
  sequence_ $ (\case
    T.GState msg -> writeTQueue statePool msg
    T.GEvent msg -> writeTQueue eventPool msg
    _            -> error "illegal message") <$> msgs

isStateMessage :: Message -> Bool
isStateMessage (T.GState _) = True
isStateMessage _            = False

-- TODO: add max resend
getMessagesToSend :: Word32 -> BroadcastPool -> ([Message], BroadcastPool)
getMessagesToSend l = foldl' f ([], mempty)
  where
    l' = 4 * ceiling (log $ fromIntegral l + 2 :: Double)
    f (msgs, new)  (msg, i) = if l' >= succ i
      then (msg : msgs, (msg, succ i) : new)
      else (msgs,       (msg, i) : new)

getStateMessagesToHandle :: StateDelta -> ([StateMessage], StateDelta)
getStateMessagesToHandle = Map.mapAccum f []
  where
    f xs (msg, handled) = (if handled then xs else msg:xs, (msg, True))

insertStateMessage :: (StateMessage, Bool) -> StateDelta -> (Maybe (StateMessage, Bool), StateDelta)
insertStateMessage msg@(x, _) = Map.insertLookupWithKey f (I.serverNodeId $ getMsgNode x) msg
  where
    f _key (v', p') (v, p) = if TC v' > TC v then (v', p') else (v, p)

cleanStateMessages :: [StateMessage] -> [StateMessage]
cleanStateMessages = Map.elems . foldl' (flip insertMsg) mempty
  where
    insertMsg x = Map.insertWith (\a b -> if TC a > TC b then a else b) (I.serverNodeId $ getMsgNode x) x

broadcastMessage :: Message -> BroadcastPool -> BroadcastPool
broadcastMessage msg xs = (msg, 0) : xs

updateLamportTime :: TVar Word32 -> Word32 -> STM Word32
updateLamportTime localClock eventTime = do
  localTime <- readTVar localClock
  if localTime < eventTime
    then do
      writeTVar localClock (eventTime + 1)
      return (eventTime + 1)
    else
      return localTime

incrementTVar :: Enum a => TVar a -> STM a
incrementTVar localClock = stateTVar localClock (\x -> let y = succ x in (y, y))

maxRetryTimeInterval :: Int
maxRetryTimeInterval = 10 * 1000 * 1000

eventNameINIT :: Text
eventNameINIT = "INIT_INTERNAL_USE_ONLY"

eventNameINITED :: Text
eventNameINITED = "INITED_INTERNAL_USE_ONLY"

clusterInitedErr :: StatusDetails
clusterInitedErr = "Cluster is already initialized"

clusterReadyErr  :: StatusDetails
clusterReadyErr  = "Cluster is ready"

clusterNotReadyErr  :: StatusDetails
clusterNotReadyErr  = "Node / Cluster is not ready"

data ClusterInitedErr = ClusterInitedErr
  deriving (Show, Eq)
instance Exception ClusterInitedErr

data ClusterReadyErr = ClusterReadyErr
  deriving (Show, Eq)
instance Exception ClusterReadyErr

data ClusterNotReadyErr = ClusterNotReadyErr
  deriving (Show, Eq)
instance Exception ClusterNotReadyErr

data FailedToStart = FailedToStart
  deriving (Show, Eq)
instance Exception FailedToStart

data EmptyPingRequest = EmptyPingRequest
  deriving (Show, Eq)
instance Exception EmptyPingRequest

data EmptyJoinRequest = EmptyJoinRequest
  deriving (Show, Eq)
instance Exception EmptyJoinRequest

data DuplicateNodeId = DuplicateNodeId
  deriving (Show, Eq)
instance Exception DuplicateNodeId

exHandlers :: [Handler a]
exHandlers =
  [ Handler $ \(err :: ClusterInitedErr) -> do
      Log.debug $ Log.buildString' err
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusFailedPrecondition (Just $ unStatusDetails clusterInitedErr) Nothing

  , Handler $ \(err :: ClusterReadyErr) -> do
      Log.debug $ Log.buildString' err
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusFailedPrecondition (Just $ unStatusDetails clusterReadyErr) Nothing

  , Handler $ \(err :: ClusterNotReadyErr) -> do
      Log.debug $ Log.buildString' err
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusFailedPrecondition (Just $ unStatusDetails clusterNotReadyErr) Nothing

  , Handler $ \(err :: DuplicateNodeId) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusAlreadyExists (Just "Duplicate node id join not allowed") Nothing

  , Handler $ \(err :: FailedToStart) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusFailedPrecondition

  , Handler $ \(err :: EmptyPingRequest) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInvalidArgument

  , Handler $ \(err :: EmptyJoinRequest) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInvalidArgument

  , Handler $ \(err :: IOException) -> do
      Log.fatal $ Log.buildString' err
      HsGrpc.throwGrpcError $ HE.mkGrpcStatus err HsGrpc.StatusInternal

  , Handler $ \(err :: SomeException) -> do
      Log.fatal $ Log.buildString' err
      let x = ("UnKnown exception: " <>) <$> HE.mkStatusMsg err
      HsGrpc.throwGrpcError $ HsGrpc.GrpcStatus HsGrpc.StatusUnknown x Nothing
  ]

exceptionHandlers :: [Handler (ServerResponse 'Normal a)]
exceptionHandlers =
  [ Handler $ \(err :: ClusterInitedErr) -> do
      Log.debug $ Log.buildString' err
      returnErrResp StatusFailedPrecondition clusterInitedErr

  , Handler $ \(err :: ClusterReadyErr) -> do
      Log.debug $ Log.buildString' err
      returnErrResp StatusFailedPrecondition clusterReadyErr

  , Handler $ \(err :: ClusterNotReadyErr) -> do
      Log.debug $ Log.buildString' err
      returnErrResp StatusFailedPrecondition clusterNotReadyErr

  , Handler $ \(err :: FailedToStart) -> do
      Log.fatal $ Log.buildString' err
      returnErrResp StatusFailedPrecondition "Cluster failed to start"

  , Handler $ \(err :: DuplicateNodeId) -> do
      Log.fatal $ Log.buildString' err
      returnErrResp StatusAlreadyExists "Duplicate node id join not allowed "

  , Handler $ \(err :: EmptyPingRequest) -> do
      Log.fatal $ Log.buildString' err
      returnErrResp StatusInvalidArgument "Empty ping request"

  , Handler $ \(err :: EmptyJoinRequest) -> do
      Log.fatal $ Log.buildString' err
      returnErrResp StatusInvalidArgument "Empty join request"

  , Handler $ \(err :: IOException) -> do
      Log.fatal $ Log.buildString' err
      returnErrResp StatusInternal (fromString $ displayException err)

  , Handler $ \(err :: SomeException) -> do
      Log.fatal $ Log.buildString' err
      let x = "UnKnown exception: " <> displayException err
      returnErrResp StatusUnknown (fromString x)
  ]

--------------------------------------------------------------------------------

initCluster :: GossipContext -> IO ()
initCluster GossipContext{..} = tryPutMVar clusterInited User >>= \case
  True  -> return ()
  False -> Log.warning "The server has already received an init signal"

broadcastEvent :: GossipContext -> EventMessage -> IO ()
broadcastEvent GossipContext {..} = atomically . writeTQueue eventPool

createEventHandlers :: [(EventName, EventHandler)] -> Map.Map EventName EventHandler
createEventHandlers = Map.fromList

getSeenEvents :: GossipContext -> IO SeenEvents
getSeenEvents GossipContext {..} = readTVarIO seenEvents

getMemberList :: GossipContext -> IO [I.ServerNode]
getMemberList GossipContext {..} =
  (readTVarIO serverList >>= filterM (\x -> readTVarIO (serverState x) <&> (== ServerAlive)) . Map.elems . snd)
  <&> ((:) serverSelf . map serverInfo)

getMemberListSTM :: GossipContext -> STM [I.ServerNode]
getMemberListSTM GossipContext {..} = do
  readTVar serverList >>= filterM (\x -> readTVar (serverState x) <&> (== ServerAlive)) . Map.elems . snd
  <&> ((:) serverSelf . map serverInfo)

getOtherMembersSTM :: GossipContext -> STM [I.ServerNode]
getOtherMembersSTM GossipContext {..} = do
  readTVar serverList >>= filterM (\x -> readTVar (serverState x) <&> (== ServerAlive)) . Map.elems . snd
  <&> map serverInfo

getMemberListWithEpochSTM :: GossipContext -> STM (Word32, [I.ServerNode])
getMemberListWithEpochSTM GossipContext {..} =
  readTVar serverList >>= \(epoch, sList) ->
    (,) epoch <$> (filterM (\x -> readTVar (serverState x) <&> (== ServerAlive)) (Map.elems sList)
                <&> ((:) serverSelf . map serverInfo))

getEpoch :: GossipContext -> IO Word32
getEpoch GossipContext {..} =
  readTVarIO serverList <&> fst

getEpochSTM :: GossipContext -> STM Word32
getEpochSTM GossipContext {..} =
  readTVar serverList <&> fst

getFailedNodes :: GossipContext -> IO [I.ServerNode]
getFailedNodes GossipContext {..} = readTVarIO deadServers <&> Map.elems

getFailedNodesSTM :: GossipContext -> STM [I.ServerNode]
getFailedNodesSTM GossipContext {..} = readTVar deadServers <&> Map.elems

getClusterStatus :: GossipContext -> IO (HM.HashMap Word32 ServerNodeStatus)
getClusterStatus gc@GossipContext {..} = do
  alives <- getMemberList gc
  deads <- getFailedNodes gc
  isReady <- tryReadMVar clusterReady
  let self = helper (case isReady of Just _  -> NodeStateRunning; Nothing -> NodeStateStarting)
           . fromInternalServerNode
           $ serverSelf
  return $ HM.fromList $
       self
     : map (helper NodeStateRunning . fromInternalServerNode) alives
    ++ map (helper NodeStateDead . fromInternalServerNode) deads
  where
    helper state node@ServerNode{..} =
      (serverNodeId, ServerNodeStatus { serverNodeStatusNode  = Just node
                                      , serverNodeStatusState = EnumPB state})
