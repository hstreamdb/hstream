{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Core where

import           Control.Concurrent.Async         (async, cancel, linkOnly,
                                                   withAsync)
import           Control.Concurrent.STM           (atomically, dupTChan,
                                                   flushTQueue, modifyTVar,
                                                   modifyTVar', newTVarIO,
                                                   peekTQueue, readTVar,
                                                   readTVarIO, stateTVar,
                                                   tryPutTMVar, writeTQueue,
                                                   writeTVar)
import           Control.Concurrent.STM.TChan     (readTChan)
import           Control.Exception                (SomeException, handle)
import           Control.Monad                    (forever, join, unless, void,
                                                   when)
import qualified Data.IntMap.Strict               as IM
import qualified Data.Map.Strict                  as Map
import qualified HStream.Logger                   as Log
import           Network.GRPC.HighLevel.Generated (withGRPCClient)

import           HStream.Gossip.Gossip            (gossip)
import           HStream.Gossip.HStreamGossip     as API (Ack (..))
import           HStream.Gossip.Probe             (doPing, pingReq, pingReqPing)
import           HStream.Gossip.Types             (EventMessage (Event),
                                                   GossipContext (..),
                                                   Message (..),
                                                   RequestAction (..),
                                                   ServerState (OK, Suspicious),
                                                   ServerStatus (..),
                                                   StateMessage (..))
import           HStream.Gossip.Utils             (broadcastMessage,
                                                   cleanStateMessages,
                                                   decodeThenBroadCast,
                                                   getMsgInc, mkGRPCClientConf,
                                                   updateLamportTime,
                                                   updateStatus)
import qualified HStream.Server.HStreamInternal   as I

--------------------------------------------------------------------------------
-- Add a new member to the server list

addToServerList :: GossipContext -> I.ServerNode -> StateMessage -> ServerState -> IO ()
addToServerList gc@GossipContext{..} node@I.ServerNode{..} msg state = unless (node == serverSelf) $ do
  initMsg <- newTVarIO msg
  initState <- newTVarIO state
  let status = ServerStatus {
    serverInfo    = node
  , latestMessage = initMsg
  , serverState   = initState
  }
  newAsync <- async (joinWorkers gc status)
  atomically $ do
    modifyTVar' serverList (Map.insert serverNodeId status)
    modifyTVar' workers (Map.insert serverNodeId newAsync)

joinWorkers :: GossipContext -> ServerStatus -> IO ()
joinWorkers gc@GossipContext{..} ss@ServerStatus{serverInfo = sNode@I.ServerNode{..}, ..} =
  handle (\(e ::SomeException) -> print e) $ unless (sNode == serverSelf) $ do
  workersMap <- readTVarIO workers
  case Map.lookup (I.serverNodeId serverSelf) workersMap of
    Just a  -> linkOnly (const True) a
    Nothing -> error "Impossible happened"
  Log.info . Log.buildString $ "Setting up workers for: " <> show serverNodeId
  withGRPCClient (mkGRPCClientConf sNode) $ \client -> do
    myChan <- atomically $ dupTChan actionChan
    -- FIXME: This could be problematic when we have too many nodes in cluster.
    loop client myChan
  where
    loop client myChan = do
      action <- atomically (readTChan myChan)
      withAsync (doAction client action) (\_ -> loop client myChan)
    doAction client action = case action of
      DoPing sid msg -> when (sid == serverNodeId) $
        doPing client gc ss sid msg
      DoPingReq sids ServerStatus{serverInfo = sInfo} isAcked msg -> when (serverNodeId `elem` sids) $ do
        Log.info . Log.buildString $ "Sending ping Req to " <> show serverNodeId <> " asking for " <> show (I.serverNodeId sInfo)
        ack <- pingReq sInfo msg client
        atomically $ case ack of
          Right (Ack msgsBS) -> do
            decodeThenBroadCast msgsBS  statePool eventPool
            inc <- getMsgInc <$> readTVar latestMessage
            writeTQueue statePool $ Alive inc sNode serverSelf
            void $ tryPutTMVar isAcked ()
          Left (Just msgsBS) -> decodeThenBroadCast msgsBS statePool eventPool
          Left Nothing       -> pure ()
      DoPingReqPing sid isAcked msg -> when (sid == serverNodeId) $ do
        Log.info . Log.buildString $ "Sending PingReqPing to: " <> show serverNodeId
        pingReqPing msg isAcked client
      DoGossip sids msg -> when (serverNodeId `elem` sids) $ do
        gossip msg client

--------------------------------------------------------------------------------
-- Messages

runStateHandler :: GossipContext -> IO ()
runStateHandler gc@GossipContext{..} = forever $ do
  newMsgs <- atomically $ do
    void $ peekTQueue statePool
    flushTQueue statePool
  unless (null newMsgs) $ do
    handleStateMessages gc $ cleanStateMessages newMsgs

handleStateMessages :: GossipContext -> [StateMessage] -> IO ()
handleStateMessages = mapM_ . handleStateMessage

handleStateMessage :: GossipContext -> StateMessage -> IO ()
handleStateMessage gc@GossipContext{..} msg@(Join node@I.ServerNode{..}) = unless (node == serverSelf) $ do
  Log.info . Log.buildString $ "[" <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  sMap <- readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing -> do
      addToServerList gc node msg OK
      atomically $ modifyTVar broadcastPool (broadcastMessage $ StateMessage msg)
    Just ServerStatus{..} -> unless (serverInfo == node) $
      -- TODO: vote to resolve conflict
      Log.warning . Log.buildString $ "Node won't be added to the list to conflict of server id"
handleStateMessage GossipContext{..} msg@(Confirm _inc I.ServerNode{..} _node)= do
  Log.info . Log.buildString $ "[" <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  sMap <- readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing               -> pure ()
    Just ServerStatus{..} -> join $ atomically $ do
      modifyTVar broadcastPool (broadcastMessage $ StateMessage msg)
      writeTVar latestMessage msg
      modifyTVar' serverList (Map.delete serverNodeId)
      mWorker <- stateTVar workers (Map.updateLookupWithKey (\_ _ -> Nothing) serverNodeId)
      case mWorker of
        Nothing -> return (pure ())
        Just  a -> return $ do
          Log.info . Log.buildString $ "Stopping Worker" <> show serverNodeId
          cancel a
handleStateMessage GossipContext{..} msg@(Suspect inc node@I.ServerNode{..} _node) = do
  Log.info . Log.buildString $ "[" <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  join . atomically $ if node == serverSelf
    then writeTQueue statePool (Alive (succ inc) node serverSelf) >> return (pure ())
    else do
      sMap <- readTVar serverList
      case Map.lookup serverNodeId sMap of
        Just ss -> do
          updated <- updateStatus ss msg Suspicious
          when updated $ modifyTVar broadcastPool (broadcastMessage $ StateMessage msg)
          return (pure ())
        Nothing -> return $ Log.debug "Suspected node not found in the server list"
          -- addToServerList gc node msg Suspicious
handleStateMessage gc@GossipContext{..} msg@(Alive _inc node@I.ServerNode{..} _node) = do
  Log.info . Log.buildString $ "[" <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  unless (node == serverSelf) $ do
    sMap <- readTVarIO serverList
    case Map.lookup serverNodeId sMap of
      Just ss -> atomically $ do
        updated <- updateStatus ss msg OK
        when updated $ modifyTVar broadcastPool (broadcastMessage $ StateMessage msg)
      Nothing -> addToServerList gc node msg OK

runEventHandler :: GossipContext -> IO ()
runEventHandler gc@GossipContext{..} = forever $ do
  newMsgs <- atomically $ do
    void $ peekTQueue eventPool
    flushTQueue eventPool
  unless (null newMsgs) $ do
    handleEventMessages gc newMsgs

handleEventMessages :: GossipContext -> [EventMessage] -> IO ()
handleEventMessages = mapM_ . handleEventMessage

-- handleEventMessage :: GossipContext -> EventMessage -> IO Bool
handleEventMessage :: GossipContext -> EventMessage -> IO ()
handleEventMessage GossipContext{..} msg@(Event eName lpTime bs) = join . atomically $ do
  let event = (eName, bs)
  currentTime <- updateLamportTime eventLpTime lpTime
  seen <- readTVar seenEvents
  let len = fromIntegral $ max 10 (IM.size seen)
      lpInt = fromIntegral lpTime
  if currentTime > len && lpTime < currentTime - len then return $ pure ()
    else case IM.lookup (fromIntegral lpTime) seen of
      Nothing -> do
        modifyTVar seenEvents $ IM.insert lpInt [event]
        modifyTVar broadcastPool $ broadcastMessage (EventMessage msg)
        return $ Log.info . Log.buildString $ "Event handled:" <> show msg
      Just events -> if event `elem` events
        then return $ pure ()
        else do
          modifyTVar seenEvents $ IM.insertWith (++) lpInt [event]
          modifyTVar broadcastPool $ broadcastMessage (EventMessage msg)
          return $ Log.info . Log.buildString $ "Event handled:" <> show msg
