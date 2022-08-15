{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Gossip.Start where

import           Control.Concurrent               (MVar, forkIO, modifyMVar,
                                                   newEmptyMVar, newMVar,
                                                   putMVar, readMVar,
                                                   threadDelay)
import           Control.Concurrent.Async         (Async, async, link2Only,
                                                   mapConcurrently)
import           Control.Concurrent.STM           (TVar, atomically, modifyTVar,
                                                   newBroadcastTChanIO,
                                                   newTQueueIO, newTVarIO,
                                                   stateTVar)
import           Control.Exception                (handle, throwIO, try)
import           Control.Monad                    (void, when)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Lazy             as BL
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as Map
import qualified Data.Vector                      as V
import qualified Network.GRPC.HighLevel.Generated as GRPC
import           Proto3.Suite                     (def)
import qualified Proto3.Suite                     as PT
import           System.Random                    (initStdGen)

import           HStream.Gossip.Core              (broadCastUserEvent,
                                                   initGossip, runEventHandler,
                                                   runStateHandler)
import           HStream.Gossip.Gossip            (scheduleGossip)
import           HStream.Gossip.Handlers          (handlers)
import qualified HStream.Gossip.HStreamGossip     as API
import           HStream.Gossip.Probe             (bootstrapPing, scheduleProbe)
import           HStream.Gossip.Types             (EventHandlers, EventPayload,
                                                   GossipContext (..),
                                                   GossipOpts (..),
                                                   InitType (..))
import           HStream.Gossip.Utils             (ClusterInitedErr (..),
                                                   ClusterReadyErr (..),
                                                   FailedToStart (..),
                                                   eventNameINIT,
                                                   eventNameINITED,
                                                   maxRetryTimeInterval,
                                                   mkClientNormalRequest,
                                                   mkGRPCClientConf')
import qualified HStream.Logger                   as Log
import qualified HStream.Server.HStreamInternal   as I
import qualified HStream.Utils                    as U

initGossipContext :: GossipOpts -> EventHandlers -> I.ServerNode -> [(ByteString, Int)] -> IO GossipContext
initGossipContext gossipOpts _eventHandlers serverSelf seeds = do
  when (null seeds) $ error " Please specify at least one node to start with"
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
  clusterReady  <- newEmptyMVar
  numInited     <- newEmptyMVar
  let eventHandlers = Map.insert eventNameINITED (handleINITEDEvent numInited (length seeds) clusterReady) _eventHandlers
  return GossipContext {..}

startGossip :: GossipContext -> IO ()
startGossip gc@GossipContext {..} = do
  seedInfo <- newEmptyMVar
  a <- startListeners (I.serverNodeHost serverSelf) seedInfo gc
  (isSeed, seeds', wasIDead) <- readMVar seedInfo
  atomically $ modifyTVar workers (Map.insert (I.serverNodeId serverSelf) a)
  if isSeed && not wasIDead
    then newTVarIO 0 >>= putMVar numInited . Just
      >> threadDelay 1000000
      >> bootstrap seeds' gc
    else putMVar numInited Nothing
      >> joinCluster serverSelf seeds'
     >>= initGossip gc
      >> putMVar clusterInited Gossip
      >> putMVar clusterReady ()

--------------------------------------------------------------------------------

bootstrap :: [(ByteString, Int)] -> GossipContext -> IO ()
bootstrap [] GossipContext{..} = do
  Log.info "Only one node in the cluster, no bootstrapping needed"
  putMVar clusterInited Gossip
  putMVar clusterReady ()
  Log.info "All servers have been initialized"
bootstrap initialServers gc@GossipContext{..} = handle
  (\(_ :: ClusterInitedErr) -> do
      Log.warning "Received multiple init signals in the cluster, this one will be ignored"
      return ())
  $ do
  readMVar clusterInited >>= \case
    User ->  do
      members <- waitForServersToStart initialServers
      broadCastUserEvent gc eventNameINIT (BL.toStrict $ PT.toLazyByteString (API.ServerList $ V.fromList (serverSelf : members)))
    _ -> return ()

amIASeed :: I.ServerNode -> [(ByteString, Int)] -> IO (Bool, [(ByteString, Int)], Bool)
amIASeed self@I.ServerNode{..} seeds = do
    Log.debug . Log.buildString' $ seeds
    if current `elem` seeds then return (True, L.delete current seeds, False) else pingToFindOut (False, seeds, False) seeds
  where
    current = (serverNodeHost, fromIntegral serverNodeGossipPort)
    pingToFindOut old@(isSeed, oldSeeds, wasDead) (join@(joinHost, joinPort):rest) = do
      GRPC.withGRPCClient (mkGRPCClientConf' joinHost joinPort) $ \client -> do
        started <- try (bootstrapPing join client)
        new <- case started of
            Right Nothing     -> do
              Log.debug . Log.buildString $ "I am not " <> show join
              return old
            Right (Just node) -> if node == self then do
              Log.debug ("I am a seed: " <> Log.buildString' join)
              return (True, L.delete join oldSeeds, wasDead)
                                                 else return old
            Left (_ :: ClusterReadyErr) -> return (isSeed, oldSeeds, True)
        pingToFindOut new rest
    pingToFindOut old _ = return old

handleINITEDEvent :: MVar (Maybe (TVar Int)) -> Int -> MVar () -> EventPayload -> IO ()
handleINITEDEvent initedM l ready payload = readMVar initedM >>= \case
  Nothing     -> return ()
  Just inited -> case PT.fromByteString payload of
    Left err -> Log.warning $ Log.buildString' err
    Right (node :: I.ServerNode) -> do
      Log.debug $ Log.buildString' node <> " has been initialized"
      x <- atomically $ stateTVar inited (\x -> (x + 1, x + 1))
      if x == l then putMVar ready () >> Log.info "All servers have been initialized"
        else when (x > l) $ Log.warning "More seeds has been initiated, something went wrong"

startListeners ::  ByteString -> MVar (Bool, [(ByteString, Int)], Bool) -> GossipContext -> IO (Async ())
startListeners grpcHost seedInfo gc@GossipContext {..} = do
  let grpcOpts = GRPC.defaultServiceOptions {
      GRPC.serverHost = GRPC.Host grpcHost
    , GRPC.serverPort = GRPC.Port $ fromIntegral $ I.serverNodeGossipPort serverSelf
    , GRPC.serverOnStarted = Just $ do
        void . forkIO $ amIASeed serverSelf seeds >>= putMVar seedInfo
        Log.debug . Log.buildString $ "Server node " <> show serverSelf <> " started"
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
joinCluster node joins = do
  retryCount <- newMVar 0
  loop retryCount
  where
    loop retryCount = do
      members <- joinCluster' node joins
      case members of
        [] -> retry retryCount
        _  -> return members
    retry :: MVar Int -> IO [I.ServerNode]
    retry retryCount = do
      count <- modifyMVar retryCount (\x -> return (x + 1, x + 1))
      -- TODO: Allow configuration to specify the retry count
      if count >= 5
        then do
          Log.fatal $ "Failed to join the cluster, "
                    <> "please make sure the seed-nodes lists at least one available node from the cluster."
          throwIO FailedToStart
        else do
          Log.warning $ Log.buildString $ "Failed to join, retrying " <> show count <> " time"
          threadDelay $ min ((2 ^ count) * 1000 * 1000) maxRetryTimeInterval
          loop retryCount

joinCluster' :: I.ServerNode -> [(ByteString, Int)] -> IO [I.ServerNode]
joinCluster' _ [] = return []
joinCluster' sNode ((joinHost, joinPort):rest) = do
  members <- GRPC.withGRPCClient (mkGRPCClientConf' joinHost joinPort) $ \client -> do
    API.HStreamGossip{..} <- API.hstreamGossipClient client
    hstreamGossipJoin (mkClientNormalRequest def { API.joinReqNew = Just sNode}) >>= \case
      GRPC.ClientNormalResponse (API.JoinResp xs) _ _ _ _ -> do
        Log.info . Log.buildString $ "Successfully joined cluster with " <> show xs
        return $ L.delete sNode (V.toList xs)
      GRPC.ClientErrorResponse (GRPC.ClientIOError (GRPC.GRPCIOBadStatusCode GRPC.StatusAlreadyExists _))  -> do
        Log.fatal "Failed to join the cluster, node with the same id already exists"
        throwIO FailedToStart
      GRPC.ClientErrorResponse _ -> do
        Log.info . Log.buildString $ "failed to join " <> U.bs2str joinHost <> ":" <> show joinPort
        return []
  if null members then joinCluster' sNode rest else return members
