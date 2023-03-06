{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Handlers where

import           Control.Concurrent             (killThread, readMVar,
                                                 tryPutMVar)
import           Control.Concurrent.STM         (atomically, check, flushTQueue,
                                                 modifyTVar, modifyTVar',
                                                 peekTQueue, readTVar,
                                                 readTVarIO, stateTVar,
                                                 writeTQueue, writeTVar)
import           Control.Monad                  (forever, join, unless, void,
                                                 when)
import           Data.Bifunctor                 (first, second)
import qualified Data.ByteString.Lazy           as BL
import qualified Data.IntMap.Strict             as IM
import qualified Data.Map.Strict                as Map
import           Data.Time.Clock.System
import qualified Data.Vector                    as V
import qualified Proto3.Suite                   as PT

import           HStream.Base                   (throwIOError)
import           HStream.Gossip.HStreamGossip   (ServerList (..))
import           HStream.Gossip.Types           (EventMessage (EventMessage),
                                                 EventName, EventPayload,
                                                 GossipContext (..),
                                                 InitType (Gossip),
                                                 ServerState (..),
                                                 ServerStatus (..),
                                                 StateMessage (..))
import qualified HStream.Gossip.Types           as T
import           HStream.Gossip.Utils           (broadcastMessage,
                                                 eventNameINIT, eventNameINITED,
                                                 incrementTVar,
                                                 initServerStatus,
                                                 updateLamportTime)
import           HStream.Gossip.Worker          (addToServerList, initGossip)
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as I

runStateHandler :: GossipContext -> IO ()
runStateHandler gc@GossipContext{..} = forever $ do
  newMsgs <- atomically $ do
    void $ peekTQueue statePool
    flushTQueue statePool
  unless (null newMsgs) $ do
    handleStateMessages gc newMsgs

handleStateMessages :: GossipContext -> [StateMessage] -> IO ()
handleStateMessages = mapM_ . handleStateMessage

handleStateMessage :: GossipContext -> StateMessage -> IO ()
handleStateMessage GossipContext{..} msg@(T.GConfirm inc node@I.ServerNode{..} _node)= do
  Log.debug . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] received message " <> show node <> " leaving cluster"
  sMap <- snd <$> readTVarIO serverList
  case Map.lookup serverNodeId sMap of
    Nothing               -> pure ()
    Just ServerStatus{..} -> do
      inc' <- readTVarIO stateIncarnation
      state <- readTVarIO serverState
      when (inc >= inc' && state /= ServerDead) $ do
        Log.info . Log.buildString $ "HStream-Gossip: [Server Node " <> show (I.serverNodeId serverSelf) <> "] handling message " <> show (T.TC msg)
        now <- getSystemTime
        mWorker <- atomically $ do
          modifyTVar' broadcastPool (broadcastMessage $ T.GState msg)
          writeTVar latestMessage msg
          writeTVar serverState ServerDead
          writeTVar stateChange now
          writeTVar incarnation inc
          modifyTVar' serverList $ first succ
          modifyTVar' deadServers $ Map.insert serverNodeId serverInfo
          stateTVar workers (Map.updateLookupWithKey (\_ _ -> Nothing) serverNodeId)
        case mWorker of
          Nothing -> pure ()
          Just  a -> do
            Log.info . Log.buildString $ "Stopping Worker" <> show serverNodeId
            killThread a
            Log.info . Log.buildString $ "[Server Node " <> show (I.serverNodeId serverSelf) <> "] " <> show node <> " left cluster"
handleStateMessage GossipContext{..} msg@(T.GSuspect inc node@I.ServerNode{..} _node) = do
  Log.debug . Log.buildString $ "HStream-Gossip: [Server Node " <> show (I.serverNodeId serverSelf) <> "] received" <> show (T.TC msg)
  now <- getSystemTime
  join . atomically $ if node == serverSelf
    then -- TODO: add incarnation comparison
      writeTQueue statePool (T.GAlive (succ inc) node serverSelf) >> return (pure ())
    else do
      sMap <- snd <$> readTVar serverList
      case Map.lookup serverNodeId sMap of
        Just ServerStatus{..} -> do
          inc' <- readTVar stateIncarnation
          state <- readTVar serverState
          when (inc > inc' && state == ServerSuspicious || inc >= inc' && state == ServerAlive) $ do
            writeTVar latestMessage msg
            writeTVar serverState ServerSuspicious
            writeTVar stateChange now
            writeTVar incarnation inc
            modifyTVar broadcastPool (broadcastMessage $ T.GState msg)
          return (Log.info . Log.buildString $ "HStream-Gossip: [Server Node " <> show (I.serverNodeId serverSelf) <> "] handled message " <> show (T.TC msg))
        Nothing -> return $ Log.debug "Suspected node not found in the server list"
          -- addToServerList gc node msg Suspicious
handleStateMessage gc@GossipContext{..} msg@(T.GAlive i node@I.ServerNode{..} _node) = do
  Log.debug . Log.buildString $ "HStream-Gossip: [Server Node " <> show (I.serverNodeId serverSelf) <> "] received message " <> show (T.TC msg)
  when (node /= serverSelf) $ do -- TODO: Remove this condition
    now <- getSystemTime
    join . atomically $ do
      sMap <- snd <$> readTVar serverList
      status@ServerStatus{..} <- case Map.lookup serverNodeId sMap of
        Just ss -> return ss -- TODO: 1.check address and port 2.check if the old node is dead long enough 3.update address
        Nothing -> do status <- initServerStatus node now
                      modifyTVar' serverList $ second (Map.insert serverNodeId status)
                      modifyTVar' deadServers $ Map.insert serverNodeId node
                      return status
      inc <- readTVar stateIncarnation
      oldState <- readTVar serverState
      let whetherHandle = not (node /= serverSelf && i <= inc {- &&  !nodeUpdate -}) && not (i < inc && node == serverSelf)
      when whetherHandle $ do
        -- cleanSuspicious
        -- TODO: May need to refute if node == serverSelf
        writeTVar serverState ServerAlive
        writeTVar stateIncarnation i
        modifyTVar' broadcastPool (broadcastMessage $ T.GState msg)
        modifyTVar' deadServers $ Map.delete serverNodeId
      if oldState == ServerDead && node /= serverSelf
        then return (logHandled whetherHandle >> addToServerList gc node status False)
        else return (logHandled whetherHandle)
  where
    logHandled p = when p $ Log.info . Log.buildString $
      "[INFO] HStream-Gossip: [Server Node " <> show (I.serverNodeId serverSelf) <> "] handled message " <> show (T.TC msg)

handleStateMessage _ _ = throwIOError "illegal state message"

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
handleEventMessage gc@GossipContext{..} msg@(EventMessage eName lpTime bs) = do
  Log.trace . Log.buildString $ "[Server Node" <> show (I.serverNodeId serverSelf)
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
        modifyTVar' seenEvents $ IM.insertWith (++) lpInt [event]
        modifyTVar' broadcastPool $ broadcastMessage (T.GEvent msg)
        return $ case Map.lookup eName eventHandlers of
          Nothing     -> if eName == eventNameINIT
            then do
              Log.info . Log.buildString $ "[Server Node" <> show (I.serverNodeId serverSelf)
                                        <> "] Handling Internal Event" <> show eName <> " with lamport " <> show lpInt
              (isSeed, _, wasIDead) <- readMVar seedsInfo
              when (isSeed && not wasIDead) $ handleINITEvent gc bs
            else Log.info $ "Action dealing with event " <> Log.buildString' eName <> " not found"
          Just action -> do
            action bs

handleINITEvent :: GossipContext -> EventPayload -> IO ()
handleINITEvent gc@GossipContext{..} payload = do
  case PT.fromByteString payload of
    Left err -> Log.warning $ Log.buildString' err
    Right ServerList{..} -> do
      initGossip gc $ V.toList serverListNodes
      void $ tryPutMVar clusterInited Gossip
      atomically $ do
        mWorkers <- readTVar workers
        check $ (Map.size mWorkers + 1) == length seeds
      broadCastUserEvent gc eventNameINITED (BL.toStrict $ PT.toLazyByteString serverSelf)

broadCastUserEvent :: GossipContext -> EventName -> EventPayload -> IO ()
broadCastUserEvent gc@GossipContext {..} userEventName userEventPayload= do
  lpTime <- atomically $ incrementTVar eventLpTime
  let eventMessage = EventMessage userEventName lpTime userEventPayload
  handleEventMessage gc eventMessage
