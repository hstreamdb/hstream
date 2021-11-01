{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Leader (
    selectLeader
  ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.Foldable                    (foldrM)
import           Data.List                        ((\\))
import qualified Data.Map                         as Map
import qualified Data.Map.Strict                  as M
import           Data.Maybe                       (mapMaybe)
import           Data.String                      (fromString)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.UUID                        as UUID
import           Data.UUID.V4                     (nextRandom)
import           Data.Word                        (Word32)
import           GHC.IO                           (unsafePerformIO)
import           HStream.Client.Utils             (mkClientNormalRequest,
                                                   mkGRPCClientConf)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (ServerNode (serverNodeId))
import           HStream.Server.HStreamInternal
import           HStream.Server.LoadBalance       (getNodesRanking,
                                                   startLoadBalancer,
                                                   updateLoadReports)
import           HStream.Server.Persistence       (decodeZNodeValue,
                                                   decodeZNodeValue',
                                                   encodeValueToBytes,
                                                   leaderPath, serverIdPath,
                                                   setZkData)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Persistence.Nodes (getServerInternalAddr)
import           HStream.Server.Types             (LoadManager (..),
                                                   ProducerContext (ProducerContext, _prdctxNode, _prdctxStream),
                                                   ServerContext (..), ServerID,
                                                   SubscriptionContext (..))
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB
import           ZooKeeper
import           ZooKeeper.Exception
import           ZooKeeper.Recipe.Election        (election)
import           ZooKeeper.Types

selectLeader :: ServerContext -> LoadManager -> IO ()
selectLeader ctx@ServerContext{..} lm = do
  void $ forkIO $ do
    zooWatchGet zkHandle leaderPath (\_ -> watcherApp) (\_ -> return ())
  uuid <- nextRandom
  void . forkIO $ election zkHandle "/election" (CB.pack . UUID.toString $ uuid)
    (do
      void $ zooSet zkHandle P.leaderPath (Just $ encodeValueToBytes serverID) Nothing
      updateLeader serverID

      -- Leader: watch for nodes changes & do load balancing
      Log.i $ "Current leader: " <> Log.buildString (show serverID)
      startLoadBalancer zkHandle lm
      putMVar watchLock ()
      actionTriggedByNodesChange ctx zkHandle lm
      -- Set watcher for nodes changes
      watchNodes ctx lm
    )
    (\_ -> stepApp)
  where
    watcherApp = do
      stepApp
      zooWatchGet zkHandle leaderPath (\_ -> watcherApp) (\_ -> return ())
    stepApp = do
      DataCompletion v _ <- zooGet zkHandle leaderPath
      case v of
        Just x  -> updateLeader (read . CB.unpack . CB.fromBytes $ x)
        Nothing -> pure ()
    updateLeader new = do
      noLeader <- isEmptyMVar leaderID
      case () of
        _ | noLeader  -> putMVar leaderID new
          | otherwise -> void $ swapMVar leaderID new

watchNodes :: ServerContext -> LoadManager -> IO ()
watchNodes sc@ServerContext{..} lm = do
  zooWatchGetChildren zkHandle serverIdPath callback result
  where
    callback HsWatcherCtx{..} = do
      _ <- forkIO $ watchNodes sc lm
      actionTriggedByNodesChange sc watcherCtxZHandle lm
    result _ = pure ()

actionTriggedByNodesChange :: ServerContext -> ZHandle -> LoadManager -> IO ()
actionTriggedByNodesChange ctx zkHandle LoadManager{..} = do
  void $ takeMVar watchLock
  StringsCompletion (StringVector children) <-
    zooGetChildren zkHandle serverIdPath
  serverMap <- getCurrentServers zkHandle children
  oldNodes <- getPrevServers zkHandle
  let newNodes = children \\ M.keys oldNodes
  let failedNodes = M.keys oldNodes \\ children
  unless (null newNodes) $ do
    Log.debug "Some node started. "
    updateLoadReports zkHandle loadReports
  unless (null failedNodes) $ do
    let failedNodesNames = mapMaybe (`M.lookup` oldNodes) failedNodes
    Log.debug $ fromString (show failedNodesNames)
             <> " failed/terminated since last checked. "
    -- recover subscriptions
    getFailedSubcsriptions ctx failedNodesNames
      >>= mapM_ (restartSubscription ctx)
    -- recover streams
    getFailedProducers ctx failedNodesNames
      >>= mapM_ (restartProducer ctx)
  setPrevServers zkHandle serverMap
  putMVar watchLock ()

--------------------------------------------------------------------------------

getFailedSubcsriptions :: ServerContext -> [ServerID] -> IO [String]
getFailedSubcsriptions ServerContext{..} deadServers = do
  subs <- try (P.listObjects zkHandle) >>= \case
    Left (_ :: ZooException) -> do
      ms <- readMVar subscriptionCtx
      let ks = Map.keys ms
      vs <- mapM readMVar (Map.elems ms)
      return $ ks `zip` vs
    Right subs_ -> return . Map.toList $ Map.mapKeys T.unpack subs_
  let deads = foldr (\(subId, SubscriptionContext{..}) xs ->
                        if _subctxNode `elem` deadServers
                        then subId:xs else xs
                    ) [] subs
  Log.warning . Log.buildString $ "Following subscriptions died: " <> show deads
  return deads

restartSubscription :: ServerContext -> String -> IO Bool
restartSubscription ctx@ServerContext{..} subID = do
  getNodesRanking ctx >>= go
  where
    go [] = do
      Log.warning . Log.buildString $
        "No available node to restart subscription " <> subID
      return False
    go (node:nodes) = do
      addr <- getServerInternalAddr zkHandle (serverNodeId node)
      withGRPCClient (mkGRPCClientConf addr) $ \client -> do
        HStreamInternal{..} <- hstreamInternalClient client
        let req = TakeSubscriptionRequest (TL.pack subID)
        hstreamInternalTakeSubscription (mkClientNormalRequest req) >>= \case
          (ClientNormalResponse _ _meta1 _meta2 _code _details) -> do
            return True
          (ClientErrorResponse err) -> do
            Log.warning . Log.buildString $ show err
            go nodes

getFailedProducers :: ServerContext -> [ServerID] -> IO [ProducerContext]
getFailedProducers ServerContext{..} deadServers = do
  prds <- try (P.listObjects zkHandle) >>= \case
    Left (_ :: ZooException) -> return []
    Right prds_              -> return $ Map.elems prds_
  let deads = foldr (\prd@ProducerContext{..} xs ->
                        if serverNodeId _prdctxNode `elem` deadServers
                        then prd:xs else xs
                    ) [] prds
  Log.warning . Log.buildString $ "Following streams have to be transferred: " <> show deads
  return deads

restartProducer :: ServerContext -> ProducerContext -> IO Bool
restartProducer ctx@ServerContext{..} ProducerContext{..} = do
  getNodesRanking ctx >>= go
  where
    go [] = do
      Log.warning . Log.buildString $
        "No available node to transfer stream " <> T.unpack _prdctxStream
      return False
    go (node:nodes) = do
      addr <- getServerInternalAddr zkHandle (serverNodeId node)
      withGRPCClient (mkGRPCClientConf addr) $ \client -> do
        Log.debug . Log.buildString $ "Sending producer to " <> show node
        HStreamInternal{..} <- hstreamInternalClient client
        let req = TakeStreamRequest (TL.fromStrict _prdctxStream)
        hstreamInternalTakeStream (mkClientNormalRequest req) >>= \case
          (ClientNormalResponse _ _meta1 _meta2 _code _details) -> do
            return True
          (ClientErrorResponse err) -> do
            Log.warning . Log.buildString $ show err
            go nodes

--------------------------------------------------------------------------------

watchLock :: MVar ()
watchLock = unsafePerformIO newEmptyMVar
{-# NOINLINE watchLock #-}

getPrevServers :: ZHandle -> IO (M.Map CBytes Word32)
getPrevServers zk = do
  decodeZNodeValue zk serverIdPath >>= \case
    Just x -> return x; Nothing -> return M.empty

getCurrentServers :: ZHandle -> [CBytes] -> IO (M.Map CBytes Word32)
getCurrentServers zk = foldrM f M.empty
  where
    f x y = do
      name <- getServerIDFromId zk x
      return $ M.insert x name y

getServerIDFromId :: ZHandle -> CBytes -> IO Word32
getServerIDFromId zk serverId =
  decodeZNodeValue' zk $ serverIdPath <> "/" <> serverId

setPrevServers :: ZHandle -> M.Map CBytes Word32 -> IO ()
setPrevServers zk = setZkData zk serverIdPath . encodeValueToBytes
