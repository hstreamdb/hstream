{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Server.Handlers where

import           Control.Concurrent                 (tryReadMVar)
import           Control.Concurrent.STM             (atomically,
                                                     newEmptyTMVarIO, readTMVar,
                                                     readTVar, readTVarIO,
                                                     stateTVar, writeTChan,
                                                     writeTQueue)
import           Control.Exception                  (catches, throwIO)
import           Control.Monad                      (when)
import qualified Data.Map.Strict                    as Map
import qualified Data.Vector                        as V
import           HsGrpc.Server
import           Network.GRPC.HighLevel.Generated   (GRPCMethodType (..),
                                                     ServerRequest (..),
                                                     ServerResponse)
import           System.Timeout                     (timeout)

import           HStream.Gossip.HStreamGossip       (Ack (..), Empty (..),
                                                     Gossip (..),
                                                     HStreamGossip (..),
                                                     JoinReq (..),
                                                     JoinResp (..), Ping (..),
                                                     PingReq (..),
                                                     PingReqResp (PingReqResp))
import           HStream.Gossip.Types               (GossipContext (..),
                                                     GossipOpts (..),
                                                     RequestAction (..),
                                                     ServerState (..),
                                                     ServerStatus (..))
import qualified HStream.Gossip.Types               as T
import           HStream.Gossip.Utils               (ClusterInitedErr (..),
                                                     ClusterReadyErr (..),
                                                     DuplicateNodeId (..),
                                                     EmptyJoinRequest (..),
                                                     EmptyPingRequest (..),
                                                     broadcast, exHandlers,
                                                     exceptionHandlers,
                                                     getMessagesToSend,
                                                     returnResp)
import qualified HStream.Server.HStreamInternal     as I
import qualified Proto.HStream.Gossip.HStreamGossip as P

handlers :: GossipContext
  -> HStreamGossip ServerRequest ServerResponse
handlers gc = HStreamGossip {
    hstreamGossipSendBootstrapPing = catchExceptions . sendBootstrapPingHandler gc
  , hstreamGossipSendPing          = catchExceptions . sendPingHandler gc
  , hstreamGossipSendPingReq       = catchExceptions . sendPingReqHandler gc
  , hstreamGossipSendJoin          = catchExceptions . sendJoinHandler gc
  , hstreamGossipSendGossip        = catchExceptions . sendGossipHandler gc

  , hstreamGossipCliJoin           = undefined
  , hstreamGossipCliCluster        = undefined
  , hstreamGossipCliUserEvent      = undefined
  , hstreamGossipCliGetSeenEvents  = undefined
  }
  where
    catchExceptions action = action `catches` exceptionHandlers

handlersNew :: GossipContext -> [ServiceHandler]
handlersNew gc = [
    unary (GRPC :: GRPC P.HStreamGossip "sendBootstrapPing") (const $ catchExceptions . sendBootstrapPingCore gc)
  , unary (GRPC :: GRPC P.HStreamGossip "sendPing"         ) (const $ catchExceptions . sendPingCore gc)
  , unary (GRPC :: GRPC P.HStreamGossip "sendPingReq"      ) (const $ catchExceptions . sendPingReqCore gc)
  , unary (GRPC :: GRPC P.HStreamGossip "sendJoin"         ) (const $ catchExceptions . sendJoinCore gc)
  , unary (GRPC :: GRPC P.HStreamGossip "sendGossip"       ) (const $ catchExceptions . sendGossipCore gc)
  ]
  where
    catchExceptions action = action `catches` exHandlers

sendBootstrapPingHandler :: GossipContext
  -> ServerRequest 'Normal Empty I.ServerNode
  -> IO (ServerResponse 'Normal I.ServerNode)
sendBootstrapPingHandler gc (ServerNormalRequest _metadata node) =
  sendBootstrapPingCore gc node >>= returnResp

sendBootstrapPingCore :: GossipContext
  -> Empty -> IO I.ServerNode
sendBootstrapPingCore GossipContext{..} _ =
  tryReadMVar clusterReady >>= \case
    Nothing -> tryReadMVar clusterInited >>= \case
      Nothing -> return serverSelf
      Just _  -> throwIO ClusterInitedErr
    Just _ -> throwIO ClusterReadyErr

sendPingHandler :: GossipContext
  -> ServerRequest 'Normal Ping Ack
  -> IO (ServerResponse 'Normal Ack)
sendPingHandler gc (ServerNormalRequest _metadata ping) =
  sendPingCore gc ping >>= returnResp

sendPingCore :: GossipContext -> Ping -> IO Ack
sendPingCore GossipContext{..} Ping {..} = do
  msgs <- atomically $ do
    broadcast (V.toList pingMsg) statePool eventPool
    memberMap <- snd <$> readTVar serverList
    stateTVar broadcastPool $ getMessagesToSend (fromIntegral (Map.size memberMap))
  return (Ack $ V.fromList msgs)

sendPingReqHandler :: GossipContext
  -> ServerRequest 'Normal PingReq PingReqResp
  -> IO (ServerResponse 'Normal PingReqResp)
sendPingReqHandler gc (ServerNormalRequest _metadata pingReq) = do
  sendPingReqCore gc pingReq >>= returnResp

sendPingReqCore :: GossipContext -> PingReq -> IO PingReqResp
sendPingReqCore GossipContext{..} PingReq {..} = do
  msgs <- atomically $ do
    broadcast (V.toList pingReqMsg) statePool eventPool
    memberMap <- snd <$> readTVar serverList
    stateTVar broadcastPool $ getMessagesToSend (fromIntegral (Map.size memberMap))
  case pingReqTarget of
    Nothing -> throwIO EmptyPingRequest
    Just x  -> do
      isAcked <- newEmptyTMVarIO
      atomically $ do
        writeTChan actionChan (DoPingReqPing (I.serverNodeId x) isAcked msgs)
      timeout (roundtripTimeout gossipOpts) (atomically $ readTMVar isAcked) >>= \case
        Just msg -> do
          newMsgs <- atomically $ do
            broadcast msg statePool eventPool
            memberMap <- snd <$> readTVar serverList
            stateTVar broadcastPool $ getMessagesToSend (fromIntegral (Map.size memberMap))
          return (PingReqResp True (V.fromList newMsgs))
        Nothing  -> return (PingReqResp False (V.fromList msgs))

sendJoinHandler :: GossipContext
  -> ServerRequest 'Normal JoinReq JoinResp
  -> IO (ServerResponse 'Normal JoinResp)
sendJoinHandler gc (ServerNormalRequest _metadata joinReq) = do
  sendJoinCore gc joinReq >>= returnResp

sendJoinCore :: GossipContext
  -> JoinReq
  -> IO JoinResp
sendJoinCore GossipContext{..} JoinReq {..} = do
  case joinReqNew of
    Nothing -> throwIO EmptyJoinRequest
    Just node@I.ServerNode{..} -> do
      when (serverNodeId == I.serverNodeId serverSelf) $ throwIO DuplicateNodeId
      (epoch, sMap') <- readTVarIO serverList
      case Map.lookup serverNodeId sMap' of
        Just ServerStatus{..} -> do
          state <- readTVarIO serverState
          when (state == ServerAlive) $ throwIO DuplicateNodeId
          atomically $ do
            inc <- readTVar stateIncarnation
            writeTQueue statePool $ T.GAlive (inc + 1) node node
        Nothing -> atomically $ writeTQueue statePool $ T.GAlive 1 node node
      return JoinResp {
        joinRespEpoch   = epoch
      , joinRespMembers = V.fromList $ serverSelf : (serverInfo <$> Map.elems sMap')
      }

sendGossipHandler :: GossipContext -> ServerRequest 'Normal Gossip Empty -> IO (ServerResponse 'Normal Empty)
sendGossipHandler gc (ServerNormalRequest _metadata gossip) = do
  sendGossipCore gc gossip >>= returnResp

sendGossipCore :: GossipContext -> Gossip -> IO Empty
sendGossipCore GossipContext{..} Gossip {..} = do
  atomically $ broadcast (V.toList gossipMsg) statePool eventPool
  return Empty

-- cliJoinHandler :: GossipContext -> ServerRequest 'Normal CliJoinReq JoinResp -> IO (ServerResponse 'Normal JoinResp)
-- cliJoinHandler gc (ServerNormalRequest _metadata cliJoinReq) = cliJoinCore gc cliJoinReq >>= returnResp

-- cliJoinCore gc@GossipContext{..} CliJoinReq {..} = do
--   GRPC.withGRPCClient (mkGRPCClientConf' cliJoinReqHost (fromIntegral cliJoinReqPort)) $ \client -> do
--    HStreamGossip{..} <- hstreamGossipClient client
--    hstreamGossipSendJoin (mkClientNormalRequest JoinReq { joinReqNew = Just serverSelf}) >>= \case
--      GRPC.ClientNormalResponse resp@(JoinResp members) _ _ _ _ -> do
--        membersOld <- (map serverInfo . Map.elems) . snd <$> readTVarIO serverList
--        mapM_ (\x -> addToServerList gc x (T.GJoin x) OK) $ V.toList members \\ membersOld
--        return resp
--      GRPC.ClientErrorResponse _             -> throwIO FailedToJoin

-- cliClusterHandler :: GossipContext -> ServerRequest 'Normal Empty Cluster -> IO (ServerResponse 'Normal Cluster)
-- cliClusterHandler GossipContext{..} _serverReq = do
--   members <- V.fromList . (:) serverSelf . map serverInfo . Map.elems . snd <$> readTVarIO serverList
--   returnResp Cluster {clusterMembers = members}

-- cliUserEventHandler :: GossipContext -> ServerRequest 'Normal UserEvent Empty -> IO (ServerResponse 'Normal Empty)
-- cliUserEventHandler gc (ServerNormalRequest _metadata UserEvent {..}) = do
--   broadCastUserEvent gc userEventName userEventPayload
--   returnResp Empty

-- cliGetSeenEventsHandler :: GossipContext -> ServerRequest 'Normal Empty SeenEvents -> IO (ServerResponse 'Normal SeenEvents)
-- cliGetSeenEventsHandler GossipContext{..} _ = do
--   sEvents <- IM.elems <$> readTVarIO seenEvents
--   let resp = SeenEvents . V.fromList $ uncurry UserEvent <$> concat sEvents
--   returnResp resp
