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
import           Data.Bifunctor                   (bimap)
import qualified Data.IntMap.Strict               as IM
import qualified Data.Map.Strict                  as Map
import           Network.GRPC.HighLevel.Generated (withGRPCClient)

import           HStream.Gossip.Gossip            (gossip)
import           HStream.Gossip.Probe             (doPing, pingReq, pingReqPing)
import           HStream.Gossip.Types             (EventMessage (EventMessage),
                                                   EventName, EventPayload,
                                                   GossipContext (..),
                                                   RequestAction (..),
                                                   ServerState (OK, Suspicious),
                                                   ServerStatus (..),
                                                   StateMessage (..))
import qualified HStream.Gossip.Types             as T
import           HStream.Gossip.Utils             (broadcast, broadcastMessage,
                                                   cleanStateMessages,
                                                   getMsgInc, incrementTVar,
                                                   mkGRPCClientConf,
                                                   updateLamportTime,
                                                   updateStatus)
import qualified HStream.Logger                   as Log
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
    modifyTVar' serverList $ bimap succ (Map.insert serverNodeId status)
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
          Right msgs -> do
            broadcast msgs statePool eventPool
            inc <- getMsgInc <$> readTVar latestMessage
            writeTQueue statePool $ T.GAlive inc sNode serverSelf
            void $ tryPutTMVar isAcked ()
          Left (Just msgs)   -> broadcast msgs statePool eventPool
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
handleStateMessage gc@GossipContext{..} msg@(T.GJoin node@I.ServerNode{..}) = unless (node == serverSelf) $ do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling " <> show node <> " joining cluster"
  sMap <- snd <$> readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing -> do
      addToServerList gc node msg OK
      atomically $ do
        modifyTVar' deadServers $ Map.delete serverNodeId
        modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
      Log.info . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] " <> show node <> " has joined the cluster"
    Just ServerStatus{..} -> unless (serverInfo == node) $
      -- TODO: vote to resolve conflict
      Log.warning . Log.buildString $ "Node won't be added to the list to conflict of server id"
handleStateMessage GossipContext{..} msg@(T.GConfirm _inc node@I.ServerNode{..} _node)= do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling " <> show node <> " leaving cluster"
  sMap <- snd <$> readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing               -> pure ()
    Just ServerStatus{..} -> join $ atomically $ do
      modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
      writeTVar latestMessage msg
      modifyTVar' serverList $ bimap succ (Map.delete serverNodeId)
      modifyTVar' deadServers $ Map.insert serverNodeId serverInfo
      mWorker <- stateTVar workers (Map.updateLookupWithKey (\_ _ -> Nothing) serverNodeId)
      case mWorker of
        Nothing -> return (pure ())
        Just  a -> return $ do
          Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] " <> show node <> " left cluster"
          Log.info . Log.buildString $ "Stopping Worker" <> show serverNodeId
          cancel a
handleStateMessage GossipContext{..} msg@(T.GSuspect inc node@I.ServerNode{..} _node) = do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  join . atomically $ if node == serverSelf
    then writeTQueue statePool (T.GAlive (succ inc) node serverSelf) >> return (pure ())
    else do
      sMap <- snd <$> readTVar serverList
      case Map.lookup serverNodeId sMap of
        Just ss -> do
          updated <- updateStatus ss msg Suspicious
          when updated $ modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
          return (pure ())
        Nothing -> return $ Log.debug "Suspected node not found in the server list"
          -- addToServerList gc node msg Suspicious
handleStateMessage gc@GossipContext{..} msg@(T.GAlive _inc node@I.ServerNode{..} _node) = do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] Handling" <> show msg
  unless (node == serverSelf) $ do
    sMap <- snd <$> readTVarIO serverList
    case Map.lookup serverNodeId sMap of
      Just ss -> atomically $ do
        updated <- updateStatus ss msg OK
        when updated $ modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
      Nothing -> addToServerList gc node msg OK
handleStateMessage _ _ = error "illegal state message"

runEventHandler :: GossipContext -> IO ()
runEventHandler gc@GossipContext{..} = forever $ do
  newMsgs <- atomically $ do
    void $ peekTQueue eventPool
    flushTQueue eventPool
  unless (null newMsgs) $ do
    handleEventMessages gc newMsgs

handleEventMessages :: GossipContext -> [EventMessage] -> IO ()
handleEventMessages = mapM_ . handleEventMessage

handleEventMessage :: GossipContext -> EventMessage -> IO ()
handleEventMessage GossipContext{..} msg@(EventMessage eName lpTime bs) = do
  Log.debug . Log.buildString $ "[Server Node" <> show (I.serverNodeId serverSelf)
                            <> "] Received Custom Event" <> show eName <> " with lamport " <> show lpTime
  join . atomically $ do
    currentTime <- fromIntegral <$> updateLamportTime eventLpTime lpTime
    seen <- readTVar seenEvents
    -- FIXME: max length should be a setting for seen buffer
    let len = max 10 (IM.size seen)
        lpInt = fromIntegral lpTime
    if currentTime > len && lpInt < currentTime - len then return $ pure ()
      else case IM.lookup lpInt seen of
        Nothing -> handleNewEvent lpInt
        Just events -> if event `elem` events
          then return $ pure ()
          else handleNewEvent lpInt
   where
     event = (eName, bs)
     handleNewEvent lpInt = do
        modifyTVar seenEvents $ IM.insertWith (++) lpInt [event]
        modifyTVar broadcastPool $ broadcastMessage (T.GEvent msg)
        return $ case Map.lookup eName eventHandlers of
          Nothing     -> Log.info $ "Action dealing with event " <> Log.buildString' eName <> " not found"
          Just action -> do
            Log.info . Log.buildString $ "[Server Node" <> show (I.serverNodeId serverSelf)
                                      <> "] Handling Custom Event" <> show eName <> " with lamport " <> show lpInt
            action bs

broadCastUserEvent :: GossipContext -> EventName -> EventPayload -> IO ()
broadCastUserEvent gc@GossipContext {..} userEventName userEventPayload= do
  lpTime <- atomically $ incrementTVar eventLpTime
  let eventMessage = EventMessage userEventName lpTime userEventPayload
  handleEventMessage gc eventMessage
