{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip.Probe where

import           Control.Concurrent             (readMVar, threadDelay)
import           Control.Concurrent.STM         (TMVar, atomically, check,
                                                 newEmptyTMVarIO, putTMVar,
                                                 readTMVar, readTVar,
                                                 readTVarIO, stateTVar,
                                                 takeTMVar, writeTChan,
                                                 writeTQueue)
import           Control.Exception              (throwIO)
import           Control.Monad                  (forever, join, when)
import           Data.ByteString                (ByteString)
import           Data.List                      ((\\))
import qualified Data.List                      as L
import qualified Data.Vector                    as V
import           Data.Word                      (Word32)
import           Network.GRPC.HighLevel         (GRPCIOError (..),
                                                 StatusCode (..),
                                                 StatusDetails (..))
import           Network.GRPC.HighLevel.Client  (ClientError (..),
                                                 ClientResult (..))
import qualified Network.GRPC.HighLevel.Client  as GRPC
import           System.Random                  (RandomGen)
import           System.Random.Shuffle          (shuffle')
import           System.Timeout                 (timeout)

import           HStream.Gossip.HStreamGossip   as API (Ack (..),
                                                        BootstrapPing (..),
                                                        Empty (..),
                                                        HStreamGossip (..),
                                                        Ping (..), PingReq (..),
                                                        PingReqResp (..),
                                                        hstreamGossipClient)
import           HStream.Gossip.Types           (GossipContext (..),
                                                 GossipOpts (..), Messages,
                                                 RequestAction (..), ServerId,
                                                 ServerStatus (..))
import qualified HStream.Gossip.Types           as T
import           HStream.Gossip.Utils           (ClusterInitedErr (..),
                                                 ClusterReadyErr (..),
                                                 broadcast, clusterInitedErr,
                                                 clusterReadyErr,
                                                 getMessagesToSend,
                                                 getOtherMembersSTM,
                                                 mkClientNormalRequest,
                                                 mkClientNormalRequest')
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as I

bootstrapPing :: (ByteString, Int) -> Bool -> GRPC.Client -> IO (Maybe I.ServerNode)
bootstrapPing (joinHost, joinPort) bootstrapPingIgnoreInitedErr client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipSendBootstrapPing (mkClientNormalRequest BootstrapPing{..}) >>= \case
    ClientNormalResponse serverNode _ _ _ _ -> do
      Log.debug $ "The server "
                <> Log.buildString' serverNode
                <> " ready"
      return (Just serverNode)
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusFailedPrecondition details)) -> do
      when (details == clusterInitedErr)$ throwIO ClusterInitedErr
      when (details == clusterReadyErr) $ throwIO ClusterReadyErr
      Log.debug $ "The server "
                <> Log.build joinHost <> ":"
                <> Log.build joinPort
                <> " returned an unexpected status details"
                <> Log.build (unStatusDetails details)
      return Nothing
    ClientErrorResponse err                   -> do
      Log.debug $ "The server "
                <> Log.build joinHost <> ":"
                <> Log.build joinPort
                <> " has not been started: "
                <> Log.buildString' err
      return Nothing

ping :: Int -> Messages -> GRPC.Client -> IO (Maybe Ack)
ping ttSec msg client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipSendPing (mkClientNormalRequest' (Ping $ V.fromList msg) ttSec) >>= \case
    ClientNormalResponse ack _ _ status _ -> do
      Log.trace $ Log.buildString' ack
      Log.trace $ Log.buildString' status
      return (Just ack)
    ClientErrorResponse _            -> Log.info "failed to ping" >> return Nothing

pingReq :: I.ServerNode -> Messages -> GRPC.Client -> IO (Either (Maybe Messages) Messages)
pingReq sNode msg client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipSendPingReq (mkClientNormalRequest $ PingReq (Just sNode) $ V.fromList msg) >>= \case
    ClientNormalResponse PingReqResp{..} _ _ _ _ ->
      if pingReqRespAcked
        then return . Right $ V.toList pingReqRespMsg
        else return . Left . Just $ V.toList pingReqRespMsg
    ClientErrorResponse _            -> Log.info "no acks" >> return (Left Nothing)

pingReqPing :: Messages -> TMVar Messages -> GRPC.Client -> IO ()
pingReqPing msg isAcked client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipSendPing (mkClientNormalRequest $ Ping $ V.fromList msg) >>= \case
    ClientNormalResponse (Ack msg') _ _ _ _ -> do
      Log.debug . Log.buildString $ "Received ack with " <> show msg'
      atomically $ putTMVar isAcked $ V.toList msg'
    ClientErrorResponse _            -> Log.info "no acks"

doPing
  :: GRPC.Client -> GossipContext -> ServerStatus
  -> Word32 -> Messages
  -> IO ()
doPing client gc@GossipContext{gossipOpts = GossipOpts{..}, ..}
  ss@ServerStatus{serverInfo = sNode@I.ServerNode{..}, ..} _sid msg = do
  cInc <- readTVarIO stateIncarnation
  maybeAck <- timeout probeInterval $ do
    Log.trace . Log.buildString $ show (I.serverNodeId serverSelf)
                               <> "Sending ping >>> " <> show serverNodeId
    isAcked  <- newEmptyTMVarIO
    maybeAck <- timeout roundtripTimeout (ping (max 1 (roundtripTimeout `div` (1000*1000))) msg client)
    acked    <- join . atomically . handleAck isAcked . join $ maybeAck
    if acked then return True  else do
      atomically $ do
        inc     <- readTVar stateIncarnation
        writeTQueue statePool $ T.GSuspect inc sNode serverSelf
      atomically $ takeTMVar isAcked
      return True
  case maybeAck of
    Nothing -> do
      Log.info $ "[" <> Log.buildString (show (I.serverNodeId serverSelf))
              <> "]Ping and PingReq exceeds timeout"
      atomically $ do
        inc     <- readTVar stateIncarnation
        when (inc == cInc) $
          writeTQueue statePool $ T.GConfirm inc sNode serverSelf
    Just _ -> pure ()
  where
    handleAck _isAcked (Just (Ack msgs)) = do
      broadcast (V.toList msgs) statePool eventPool
      return $ pure True
    handleAck isAcked Nothing = do
      inc     <- readTVar stateIncarnation
      members <- L.delete serverNodeId . map I.serverNodeId <$> getOtherMembersSTM gc
      case members of
        [] -> return $ pure False
        _  -> do
          let selected = take 1 $ shuffle' members (length members) randomGen
          writeTChan actionChan (DoPingReq selected ss isAcked msg)
          return $ do
            Log.info "No Ack received, sending PingReq"
            timeout roundtripTimeout (atomically $ readTMVar isAcked) >>= \case
              Nothing -> do
                atomically $ writeTQueue statePool $ T.GSuspect inc sNode serverSelf
                pure False
              Just _  -> pure True

scheduleProbe :: GossipContext -> IO ()
scheduleProbe gc@GossipContext{..} = do
  _ <- readMVar clusterInited
  forever $ do
    memberMap <- atomically $ do
      memberMap <- getOtherMembersSTM gc
      check (not $ null memberMap)
      return memberMap
    let members = I.serverNodeId <$> memberMap
    let pingOrder = shuffle' members (length members) randomGen
    runProbe gc randomGen pingOrder members

-- TODO: When a new server join in the cluster, add it randomly
runProbe :: RandomGen gen => GossipContext -> gen -> [ServerId] -> [ServerId] -> IO ()
runProbe gc@GossipContext{..} gen (x:xs) members = do
  atomically $ do
    msgs <- stateTVar broadcastPool $ getMessagesToSend (fromIntegral (length members))
    writeTChan actionChan (DoPing x msgs)
  threadDelay $ probeInterval gossipOpts
  members' <- atomically $ map I.serverNodeId <$> getOtherMembersSTM gc
  let xs' = if members' == members then xs else (xs \\ (members \\ members')) ++ (members' \\ members)
  runProbe gc gen xs' members'
runProbe _sc _gen [] _ids = pure ()
