{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Worker where

import           Control.Concurrent             (killThread, newEmptyMVar,
                                                 putMVar, readMVar, takeMVar,
                                                 tryPutMVar)
import           Control.Concurrent.STM         (atomically, check, dupTChan,
                                                 flushTQueue, modifyTVar,
                                                 modifyTVar', newTVarIO,
                                                 peekTQueue, readTVar,
                                                 readTVarIO, stateTVar,
                                                 tryPutTMVar, writeTQueue,
                                                 writeTVar)
import           Control.Concurrent.STM.TChan   (readTChan)
import           Control.Exception              (finally)
import           Control.Monad                  (forever, join, replicateM,
                                                 unless, void, when)
import           Data.Bifunctor                 (bimap)
import qualified Data.ByteString.Lazy           as BL
import qualified Data.IntMap.Strict             as IM
import           Data.IORef                     (newIORef, readIORef,
                                                 writeIORef)
import qualified Data.Map.Strict                as Map
import           Data.Time.Clock.System
import qualified Data.Vector                    as V
import qualified Proto3.Suite                   as PT
import qualified SlaveThread

import           HStream.Base                   (throwIOError)
import qualified HStream.Common.GrpcHaskell     as GRPC
import           HStream.Gossip.Gossip          (gossip)
import           HStream.Gossip.HStreamGossip   (ServerList (..))
import           HStream.Gossip.Probe           (doPing, pingReq, pingReqPing)
import           HStream.Gossip.Types           (EventMessage (EventMessage),
                                                 EventName, EventPayload,
                                                 GossipContext (..),
                                                 InitType (Gossip),
                                                 RequestAction (..),
                                                 ServerState (..),
                                                 ServerStatus (..),
                                                 StateMessage (..))
import qualified HStream.Gossip.Types           as T
import           HStream.Gossip.Utils           (broadcast, eventNameINIT,
                                                 eventNameINITED,
                                                 getMemberListWithEpochSTM,
                                                 getMsgInc, initServerStatus,
                                                 mkGRPCClientConf)
import qualified HStream.Logger                 as Log
import qualified HStream.Server.HStreamInternal as I

initGossip :: GossipContext -> [I.ServerNode] -> IO ()
initGossip gc = mapM_ (\x -> do
  if (x == serverSelf gc)
    then return ()
    else do
      now <- getSystemTime
      status <- atomically $ do
        status@ServerStatus{..} <- initServerStatus x now
        writeTVar serverState ServerAlive
        return status
      addToServerList gc x status True)

addToServerList :: GossipContext -> I.ServerNode -> ServerStatus -> Bool -> IO ()
addToServerList gc@GossipContext{..} node@I.ServerNode{..} status isJoin = unless (node == serverSelf) $ do
  grpcClientRef <- newIORef Nothing
  let grpcClientFinalizer = do client <- readIORef grpcClientRef
                               maybe (pure ()) GRPC.deleteGrpcClient client
  workersThread <- SlaveThread.forkFinally grpcClientFinalizer $ do
    client <- GRPC.initGrpcClient $ mkGRPCClientConf node
    writeIORef grpcClientRef (Just client)
    joinWorkers client gc status
  ((e1,old), (e2,new)) <- atomically $ do
    old <- getMemberListWithEpochSTM gc
    modifyTVar' serverList $ bimap (if isJoin then id else succ) (Map.insert serverNodeId status)
    modifyTVar' workers (Map.insert serverNodeId workersThread)
    new <- getMemberListWithEpochSTM gc
    return (old, new)
  Log.debug $ "Update server list from " <> Log.buildString' (map I.serverNodeId old) <> " with epoch " <> Log.buildString' e1
                               <> " to " <> Log.buildString' (map I.serverNodeId new) <> " with epoch " <> Log.buildString' e2

joinWorkers :: GRPC.Client -> GossipContext -> ServerStatus -> IO ()
joinWorkers client gc@GossipContext{..} ss@ServerStatus{serverInfo = sNode@I.ServerNode{..}, ..} = do
    Log.info . Log.buildString $ "Setting up workers for: " <> show serverNodeId
    myChan <- atomically $ dupTChan actionChan
    mvars <- replicateM (T.joinWorkerConcurrency gossipOpts) $ do
      mvar <- newEmptyMVar
      void $ SlaveThread.fork $ do
        finally (forever $ do action <- atomically (readTChan myChan)
                              doAction action
                )
                (putMVar mvar ())
      pure mvar
    -- Normally, this is an infinite block
    mapM_ takeMVar mvars
  where
    doAction action = case action of
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
