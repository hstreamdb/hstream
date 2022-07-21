{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Handlers where

import           Control.Concurrent.STM           (atomically, newEmptyTMVarIO,
                                                   readTMVar, readTVar,
                                                   readTVarIO, stateTVar,
                                                   writeTChan, writeTQueue)
import qualified Data.IntMap.Strict               as IM
import           Data.List                        ((\\))
import qualified Data.Map.Strict                  as Map
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated (GRPCMethodType (..),
                                                   ServerRequest (..),
                                                   ServerResponse,
                                                   StatusCode (..))
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           System.Timeout                   (timeout)

import           HStream.Gossip.Core              (addToServerList,
                                                   broadCastUserEvent)
import           HStream.Gossip.HStreamGossip     (Ack (..), CliJoinReq (..),
                                                   Cluster (..), Empty (..),
                                                   Gossip (..),
                                                   HStreamGossip (..),
                                                   JoinReq (..), JoinResp (..),
                                                   Ping (..), PingReq (..),
                                                   PingReqResp (PingReqResp),
                                                   SeenEvents (SeenEvents),
                                                   UserEvent (..),
                                                   hstreamGossipClient)
import           HStream.Gossip.Types             (GossipContext (..),
                                                   GossipOpts (..),
                                                   RequestAction (..),
                                                   ServerState (OK),
                                                   ServerStatus (..))
import qualified HStream.Gossip.Types             as T
import           HStream.Gossip.Utils             (broadcast, getMessagesToSend,
                                                   mkClientNormalRequest,
                                                   mkGRPCClientConf',
                                                   returnErrResp, returnResp)
import qualified HStream.Server.HStreamInternal   as I

handlers :: GossipContext
  -> HStreamGossip ServerRequest ServerResponse
handlers gc = HStreamGossip {
    hstreamGossipBootstrapPing = bootstrapPingHandler gc
  , hstreamGossipPing          = pingHandler gc
  , hstreamGossipPingReq       = pingReqHandler gc
  , hstreamGossipJoin          = joinHandler gc
  , hstreamGossipGossip        = gossipHandler gc

  , hstreamGossipCliJoin          = cliJoinHandler gc
  , hstreamGossipCliCluster       = cliClusterHandler gc
  , hstreamGossipCliUserEvent     = cliUserEventHandler gc
  , hstreamGossipCliGetSeenEvents = undefined
  }

bootstrapPingHandler :: GossipContext
  -> ServerRequest 'Normal Empty I.ServerNode
  -> IO (ServerResponse 'Normal I.ServerNode)
bootstrapPingHandler GossipContext{..} _req = returnResp serverSelf

pingHandler :: GossipContext
  -> ServerRequest 'Normal Ping Ack
  -> IO (ServerResponse 'Normal Ack)
pingHandler GossipContext{..} (ServerNormalRequest _metadata Ping {..}) = do
  msgs <- atomically $ do
    broadcast (V.toList pingMsg) statePool eventPool
    memberMap <- snd <$> readTVar serverList
    stateTVar broadcastPool $ getMessagesToSend (fromIntegral (Map.size memberMap))
  returnResp (Ack $ V.fromList msgs)

pingReqHandler :: GossipContext
  -> ServerRequest 'Normal PingReq PingReqResp
  -> IO (ServerResponse 'Normal PingReqResp)
pingReqHandler GossipContext{..} (ServerNormalRequest _metadata PingReq {..}) = do
  msgs <- atomically $ do
    broadcast (V.toList pingReqMsg) statePool eventPool
    memberMap <- snd <$> readTVar serverList
    stateTVar broadcastPool $ getMessagesToSend (fromIntegral (Map.size memberMap))
  case pingReqTarget of
    Nothing -> returnErrResp StatusInternal "Received empty pingReq request"
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
          returnResp (PingReqResp True (V.fromList newMsgs))
        Nothing  -> returnResp (PingReqResp False (V.fromList msgs))

joinHandler :: GossipContext
  -> ServerRequest 'Normal JoinReq JoinResp
  -> IO (ServerResponse 'Normal JoinResp)
joinHandler GossipContext{..} (ServerNormalRequest _metadata JoinReq {..}) = do
  case joinReqNew of
    Nothing -> error "no node info in join request"
    Just node@I.ServerNode{..} -> do
      sMap' <- snd <$> readTVarIO serverList
      case Map.lookup serverNodeId sMap' of
        Nothing | serverNodeId /= I.serverNodeId serverSelf -> do
          atomically $ writeTQueue statePool $ T.GJoin node
          returnResp . JoinResp . V.fromList $ serverSelf : (serverInfo <$> Map.elems sMap')
        _  -> returnErrResp StatusAlreadyExists "Node with the same id already exists"

gossipHandler :: GossipContext -> ServerRequest 'Normal Gossip Empty -> IO (ServerResponse 'Normal Empty)
gossipHandler GossipContext{..} (ServerNormalRequest _metadata Gossip {..}) = do
  atomically $ broadcast (V.toList gossipMsg) statePool eventPool
  returnResp Empty

cliJoinHandler :: GossipContext -> ServerRequest 'Normal CliJoinReq JoinResp -> IO (ServerResponse 'Normal JoinResp)
cliJoinHandler gc@GossipContext{..} (ServerNormalRequest _metadata CliJoinReq {..}) = do
   GRPC.withGRPCClient (mkGRPCClientConf' cliJoinReqHost (fromIntegral cliJoinReqPort)) $ \client -> do
    HStreamGossip{..} <- hstreamGossipClient client
    hstreamGossipJoin (mkClientNormalRequest JoinReq { joinReqNew = Just serverSelf}) >>= \case
      GRPC.ClientNormalResponse resp@(JoinResp members) _ _ _ _ -> do
        membersOld <- (map serverInfo . Map.elems) . snd <$> readTVarIO serverList
        mapM_ (\x -> addToServerList gc x (T.GJoin x) OK) $ V.toList members \\ membersOld
        returnResp resp
      GRPC.ClientErrorResponse _             -> error "failed to join"

cliClusterHandler :: GossipContext -> ServerRequest 'Normal Empty Cluster -> IO (ServerResponse 'Normal Cluster)
cliClusterHandler GossipContext{..} _serverReq = do
  members <- V.fromList . (:) serverSelf . map serverInfo . Map.elems . snd <$> readTVarIO serverList
  returnResp Cluster {clusterMembers = members}

cliUserEventHandler :: GossipContext -> ServerRequest 'Normal UserEvent Empty -> IO (ServerResponse 'Normal Empty)
cliUserEventHandler gc (ServerNormalRequest _metadata UserEvent {..}) = do
  broadCastUserEvent gc userEventName userEventPayload
  returnResp Empty

cliGetSeenEventsHandler :: GossipContext -> ServerRequest 'Normal Empty SeenEvents -> IO (ServerResponse 'Normal SeenEvents)
cliGetSeenEventsHandler GossipContext{..} _ = do
  sEvents <- IM.elems <$> readTVarIO seenEvents
  let resp = SeenEvents . V.fromList $ uncurry UserEvent <$> concat sEvents
  returnResp resp
