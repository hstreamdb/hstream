{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Start where

import           Control.Concurrent               (MVar, newEmptyMVar, newMVar,
                                                   putMVar, readMVar,
                                                   threadDelay, tryPutMVar)
import           Control.Concurrent.Async         (Async, async, link2Only,
                                                   mapConcurrently)
import           Control.Concurrent.STM           (TVar, atomically, check,
                                                   modifyTVar,
                                                   newBroadcastTChanIO,
                                                   newTQueueIO, newTVarIO,
                                                   readTVar, stateTVar)
import           Control.Monad                    (void, when)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Lazy             as BL
import           Data.List                        ((\\))
import qualified Data.Map.Strict                  as Map
import qualified Data.Vector                      as V
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           Proto3.Suite                     (def)
import qualified Proto3.Suite                     as PT
import           System.Random                    (initStdGen)

import           HStream.Gossip.Core              (addToServerList,
                                                   broadCastUserEvent,
                                                   runEventHandler,
                                                   runStateHandler)
import           HStream.Gossip.Gossip            (scheduleGossip)
import           HStream.Gossip.Handlers          (handlers)
import qualified HStream.Gossip.HStreamGossip     as API
import           HStream.Gossip.Probe             (bootstrapPing, scheduleProbe)
import           HStream.Gossip.Types             (EventHandlers, EventPayload,
                                                   GossipContext (..),
                                                   GossipOpts (..),
                                                   InitType (..),
                                                   ServerState (..))
import qualified HStream.Gossip.Types             as T
import           HStream.Gossip.Utils             (eventNameINIT,
                                                   eventNameINITED,
                                                   mkClientNormalRequest,
                                                   mkGRPCClientConf')
import qualified HStream.Logger                   as Log
import qualified HStream.Server.HStreamInternal   as I
import qualified HStream.Utils                    as U

initGossipContext :: GossipOpts -> EventHandlers -> I.ServerNode -> [(ByteString, Int)] -> IO GossipContext
initGossipContext gossipOpts _eventHandlers serverSelf seeds' = do
  when (null seeds') $ error " Please specify at least one node to start with"
  let current = (I.serverNodeHost serverSelf, fromIntegral $ I.serverNodeGossipPort serverSelf)
      seeds = seeds' \\ [current]
  numInited     <- if current `elem` seeds' then Just <$> newTVarIO 0 else return Nothing
  actionChan    <- newBroadcastTChanIO
  statePool     <- newTQueueIO
  eventPool     <- newTQueueIO
  eventLpTime   <- newTVarIO 0
  seenEvents    <- newTVarIO mempty
  broadcastPool <- newTVarIO mempty
  serverList    <- newTVarIO (0, mempty)
  workers       <- newTVarIO mempty
  deadServers   <- newTVarIO mempty
  incarnation   <- newTVarIO 0
  randomGen     <- initStdGen
  clusterInited <- newEmptyMVar
  clusterReady  <- if current `elem` seeds' then newEmptyMVar else newMVar ()
  let eventHandlers = Map.insert eventNameINITED (handleINITEDEvent numInited (length seeds') clusterReady) _eventHandlers
  let gc = GossipContext {..}
  return gc { eventHandlers = Map.insert eventNameINIT (handleINITEvent gc) eventHandlers}
--------------------------------------------------------------------------------

startGossip :: ByteString -> GossipContext -> IO (Async ())
startGossip grpcHost gc@GossipContext {..} = do
  a <- startListeners grpcHost gc
  atomically $ modifyTVar workers (Map.insert (I.serverNodeId serverSelf) a)
  case numInited of
    Just _  -> bootstrap seeds gc
    Nothing -> joinCluster serverSelf seeds >>= initGossip gc >> putMVar clusterReady ()
  return a

bootstrap :: [(ByteString, Int)] -> GossipContext -> IO ()
bootstrap [] gc@GossipContext{..} = do
  Log.info "Only one node in the cluster, no bootstrapping needed"
  broadCastUserEvent gc eventNameINIT (BL.toStrict $ PT.toLazyByteString (API.ServerList $ V.singleton serverSelf))
bootstrap initialServers gc@GossipContext{..} = do
  readMVar clusterInited >>= \case
    User ->  do
      members <- waitForServersToStart initialServers
      broadCastUserEvent gc eventNameINIT (BL.toStrict $ PT.toLazyByteString (API.ServerList $ V.fromList (serverSelf : members)))
    _ -> return ()

handleINITEvent :: GossipContext -> EventPayload -> IO ()
handleINITEvent gc@GossipContext{..} payload = do
  case PT.fromByteString payload of
    Left err -> Log.warning $ Log.buildString' err
    Right API.ServerList{..} -> do
      initGossip gc $ V.toList serverListNodes
      -- atomically $ maybe (pure () ) (`modifyTVar'` (+ 1)) numInited
      void $ tryPutMVar clusterInited Gossip
      atomically $ do
        mWorkers <- readTVar workers
        check $ Map.size mWorkers == (length seeds + 1)
      broadCastUserEvent gc eventNameINITED (BL.toStrict $ PT.toLazyByteString serverSelf)

handleINITEDEvent :: Maybe (TVar Int) -> Int -> MVar () -> EventPayload -> IO ()
handleINITEDEvent (Just inited) l ready payload = case PT.fromByteString payload of
  Left err -> Log.warning $ Log.buildString' err
  Right (node :: I.ServerNode) -> do
    Log.debug $ Log.buildString' node <> " has been initialized"
    x <- atomically $ stateTVar inited (\x -> (x + 1, x + 1))
    if x == l then putMVar ready () >> Log.info "All servers have been initialized"
      else when (x > l) $ Log.warning "More seeds has been initiated, something went wrong"
handleINITEDEvent Nothing _ _ _ = pure ()

startListeners ::  ByteString -> GossipContext -> IO (Async ())
startListeners grpcHost gc@GossipContext {..} = do
  let grpcOpts = GRPC.defaultServiceOptions {
      GRPC.serverHost = GRPC.Host grpcHost
    , GRPC.serverPort = GRPC.Port $ fromIntegral $ I.serverNodeGossipPort serverSelf
    , GRPC.serverOnStarted = Just (Log.debug . Log.buildString $ "Server node " <> show serverSelf <> " started")
    }
  let api = handlers gc
  aynscs@(a1:_) <- mapM async ( API.hstreamGossipServer api grpcOpts
                              : map ($ gc) [ runStateHandler
                                           , runEventHandler
                                           , scheduleGossip
                                           , scheduleProbe ])
  mapM_ (link2Only (const True) a1) aynscs
  return a1

waitForServersToStart :: [(ByteString, Int)] -> IO [I.ServerNode]
waitForServersToStart = mapConcurrently wait
  where
    wait join@(joinHost,joinPort) = GRPC.withGRPCClient (mkGRPCClientConf' joinHost joinPort) (loop join)
    loop join client = do
      started <- bootstrapPing join client
      case started of
        Nothing   -> do
          threadDelay $ 1000 * 1000
          loop join client
        Just node -> return node

joinCluster :: I.ServerNode -> [(ByteString, Int)] -> IO [I.ServerNode]
joinCluster _ [] =
  error $ "Failed to join the cluster, "
       <> "please make sure the seed-nodes lists at least one available node from the cluster."
joinCluster sNode ((joinHost, joinPort):rest) = do
  members <- GRPC.withGRPCClient (mkGRPCClientConf' joinHost joinPort) $ \client -> do
    API.HStreamGossip{..} <- API.hstreamGossipClient client
    hstreamGossipJoin (mkClientNormalRequest def { API.joinReqNew = Just sNode}) >>= \case
      GRPC.ClientNormalResponse (API.JoinResp xs) _ _ _ _ -> do
        Log.info . Log.buildString $ "Successfully joined cluster with " <> show xs
        return $ V.toList xs \\ [sNode]
      GRPC.ClientErrorResponse _ -> do
        Log.info . Log.buildString $ "failed to join " <> U.bs2str joinHost <> ":" <> show joinPort
        return []
  if null members then joinCluster sNode rest else return members

initGossip :: GossipContext -> [I.ServerNode] -> IO ()
initGossip gc = mapM_ (\x -> addToServerList gc x (T.GJoin x) OK)
