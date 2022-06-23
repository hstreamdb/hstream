{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Gossip.Probe where

import           Control.Concurrent               (threadDelay)
import           Control.Concurrent.STM           (TMVar, atomically, check,
                                                   newEmptyTMVarIO, putTMVar,
                                                   readTMVar, readTVar,
                                                   readTVarIO, stateTVar,
                                                   takeTMVar, writeTChan,
                                                   writeTQueue)
import           Control.Monad                    (forever, join, when)
import           Data.ByteString                  (ByteString)
import           Data.List                        ((\\))
import qualified Data.List                        as L
import qualified Data.Map                         as Map
import           Data.Serialize                   (decode, encode)
import           Data.Word                        (Word32)
import qualified HStream.Logger                   as Log
import           Network.GRPC.HighLevel.Client    (ClientError (..),
                                                   ClientResult (..),
                                                   StatusCode (..),
                                                   StatusDetails (..))
import qualified Network.GRPC.HighLevel.Client    as GRPC
import           Network.GRPC.HighLevel.Generated (GRPCIOError (..))
import           System.Random                    (RandomGen)
import           System.Random.Shuffle            (shuffle')
import           System.Timeout                   (timeout)

import           HStream.Gossip.HStreamGossip     as API (Ack (..), Empty (..),
                                                          HStreamGossip (..),
                                                          Ping (..),
                                                          PingReq (..),
                                                          ServerNodeInternal (..),
                                                          hstreamGossipClient)
import           HStream.Gossip.Types             (GossipContext (..),
                                                   GossipOpts (..), Message,
                                                   RequestAction (..), ServerId,
                                                   ServerState (Suspicious),
                                                   ServerStatus (..),
                                                   StateMessage (..))
import           HStream.Gossip.Utils             (decodeThenBroadCast,
                                                   getMessagesToSend, getMsgInc,
                                                   mkClientNormalRequest)

bootstrapPing :: GRPC.Client -> IO (Maybe ServerNodeInternal)
bootstrapPing client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipBootstrapPing (mkClientNormalRequest Empty) >>= \case
    ClientNormalResponse serverNode _ _ _ _ -> return (Just serverNode)
    ClientErrorResponse _                   -> Log.debug "The server has not been started"
                                            >> return Nothing

ping :: ByteString -> GRPC.Client -> IO (Maybe Ack)
ping msg client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipPing (mkClientNormalRequest $ Ping msg) >>= \case
    ClientNormalResponse ack _ _ _ _ -> do
      return (Just ack)
    ClientErrorResponse _            -> Log.info "failed to ping" >> return Nothing

pingReq :: ServerNodeInternal -> ByteString -> GRPC.Client -> IO (Either (Maybe ByteString) Ack)
pingReq sNode msg client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipPingReq (mkClientNormalRequest $ PingReq (Just sNode) msg) >>= \case
    ClientNormalResponse ack _ _ _ _ -> return (Right ack)
    ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInternal (StatusDetails details)))
      -> return (Left (Just details))
    ClientErrorResponse _            -> Log.info "no acks" >> return (Left Nothing)

pingReqPing :: ByteString -> TMVar ByteString -> GRPC.Client -> IO ()
pingReqPing msg isAcked client = do
  HStreamGossip{..} <- hstreamGossipClient client
  hstreamGossipPing (mkClientNormalRequest $ Ping msg) >>= \case
    ClientNormalResponse (Ack msg') _ _ _ _ -> do
      Log.debug . Log.buildString $ "Received ack with " <> show (decode msg' :: Either String [Message])
      atomically $ putTMVar isAcked msg'
    ClientErrorResponse _            -> Log.info "no acks"

doPing
  :: GRPC.Client -> GossipContext -> ServerStatus
  -> Word32 -> ByteString
  -> IO ()
doPing client GossipContext{..} ss@ServerStatus{serverInfo = sNode@ServerNodeInternal{..}, ..} sid msg = do
  maybeAck <- timeout (probeInterval gossipOpts) $ do
    Log.debug . Log.buildString $ show (API.serverNodeInternalId serverSelf) <> "Sending ping >>> " <> show serverNodeInternalId
    isAcked  <- newEmptyTMVarIO
    maybeAck <- timeout (roundtripTimeout gossipOpts) (ping msg client)
    acked    <- join . atomically . handleAck isAcked . join $ maybeAck
    if acked then return () else do
      atomically $ do
        inc     <- getMsgInc <$> readTVar latestMessage
        writeTQueue statePool $ Suspect inc sNode serverSelf
      atomically $ takeTMVar isAcked
  case maybeAck of
    Nothing -> do
      Log.info $ "[" <> Log.buildString (show (API.serverNodeInternalId serverSelf)) <> "]Ping and PingReq exceeds timeout"
      atomically $ do
        inc     <- getMsgInc <$> readTVar latestMessage
        state   <- readTVar serverState
        when (state == Suspicious) $
          writeTQueue statePool $ Confirm inc sNode serverSelf
    Just _ -> pure ()
  where
    handleAck _isAcked (Just (Ack msgsBS)) = do
      decodeThenBroadCast msgsBS statePool eventPool
      return $ pure True
    handleAck isAcked Nothing = do
      inc     <- getMsgInc <$> readTVar latestMessage
      members <- L.delete serverNodeInternalId . Map.keys <$> readTVar serverList
      case members of
        [] -> return $ pure False
        _  -> do
          let selected = take 1 $ shuffle' members (length members) randomGen
          writeTChan actionChan (DoPingReq selected ss isAcked msg)
          return $ do
            Log.info "No Ack received, sending PingReq"
            timeout (roundtripTimeout gossipOpts) (atomically $ readTMVar isAcked) >>= \case
              Nothing -> do
                atomically $ writeTQueue statePool $ Suspect inc sNode serverSelf
                pure False
              Just _  -> pure True

scheduleProbe :: GossipContext -> IO ()
scheduleProbe gc@GossipContext{..} = forever $ do
  memberMap <- atomically $ do
    memberMap <- readTVar serverList
    check (not $ Map.null memberMap)
    return memberMap
  let members = Map.keys memberMap
  let pingOrder = shuffle' members (length members) randomGen
  runProbe gc randomGen pingOrder members

-- TODO: When a new server join in the cluster, add it randomly
runProbe :: RandomGen gen => GossipContext -> gen -> [ServerId] -> [ServerId] -> IO ()
runProbe gc@GossipContext{..} gen (x:xs) members = do
  atomically $ do
    msgs <- stateTVar broadcastPool $ getMessagesToSend (fromIntegral (length members))
    writeTChan actionChan (DoPing x (encode msgs))
  threadDelay $ probeInterval gossipOpts
  members' <- Map.keys <$> readTVarIO serverList
  let xs' = if members' == members then xs else (xs \\ (members \\ members')) ++ (members' \\ members)
  runProbe gc gen xs' members'
runProbe _sc _gen [] _ids = pure ()
