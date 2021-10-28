{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Leader (
    selectLeader
  ) where

import           Control.Concurrent
import           Control.Monad
import           Data.Foldable              (foldrM)
import           Data.List                  ((\\))
import qualified Data.Map.Strict            as M
import           Data.String                (fromString)
import qualified Data.UUID                  as UUID
import           Data.UUID.V4               (nextRandom)
import           GHC.IO                     (unsafePerformIO)
import           Z.Data.CBytes              (CBytes)
import qualified Z.Data.CBytes              as CB
import           ZooKeeper
import           ZooKeeper.Recipe.Election  (election)
import           ZooKeeper.Types

import qualified HStream.Logger             as Log
import           HStream.Server.LoadBalance (startLoadBalancer,
                                             updateLoadReports)
import           HStream.Server.Persistence (decodeZNodeValue,
                                             decodeZNodeValue',
                                             encodeValueToBytes, leaderPath,
                                             serverIdPath, setZkData)
import           HStream.Server.Types       (LoadManager (..),
                                             ServerContext (..))

selectLeader :: ServerContext -> LoadManager -> IO ()
selectLeader ctx@ServerContext{..} lm = do
  uuid <- nextRandom
  void . forkIO $ election zkHandle "/election" (CB.pack . UUID.toString $ uuid)
    (do
      void $ zooSet zkHandle leaderPath (Just $ CB.toBytes serverName) Nothing
      updateLeader serverName

      -- Leader: watch for nodes changes & do load balancing
      Log.i $ "Current leader: " <> Log.buildString (show serverName)
      startLoadBalancer zkHandle lm
      putMVar watchLock ()
      actionTriggedByNodesChange zkHandle lm
      -- Set watcher for nodes changes
      watchNodes ctx lm
    )
    (\_ -> do
      DataCompletion v _ <- zooGet zkHandle leaderPath
      case v of
        Just x  -> updateLeader (CB.fromBytes x)
        Nothing -> pure ()
    )
  where
    updateLeader new = do
      noLeader <- isEmptyMVar leaderName
      case () of
        _ | noLeader  -> putMVar leaderName new
          | otherwise -> void $ swapMVar leaderName new

watchNodes :: ServerContext -> LoadManager -> IO ()
watchNodes sc@ServerContext{..} lm = do
  zooWatchGetChildren zkHandle serverIdPath callback result
  where
    callback HsWatcherCtx{..} = do
      _ <- forkIO $ watchNodes sc lm
      actionTriggedByNodesChange watcherCtxZHandle lm
    result _ = pure ()

actionTriggedByNodesChange :: ZHandle -> LoadManager -> IO ()
actionTriggedByNodesChange zkHandle LoadManager{..} = do
  void $ takeMVar watchLock
  StringsCompletion (StringVector children) <-
    zooGetChildren zkHandle serverIdPath
  serverMap <- getCurrentServers zkHandle children
  oldNodes <- getPrevServers zkHandle
  let newNodes = children \\ M.keys oldNodes
  let failedNodes = M.keys oldNodes \\ children
  unless (null newNodes) $ do
    Log.info "Some node started. "
    updateLoadReports zkHandle loadReports
  unless (null failedNodes) $ do
    let failedNodesNames = map (`M.lookup` oldNodes) failedNodes
    Log.info $ fromString (show failedNodesNames) <>  " failed. "
  setPrevServers zkHandle serverMap
  putMVar watchLock ()

--------------------------------------------------------------------------------

watchLock :: MVar ()
watchLock = unsafePerformIO newEmptyMVar
{-# NOINLINE watchLock #-}

getPrevServers :: ZHandle -> IO (M.Map CBytes String)
getPrevServers zk = do
  decodeZNodeValue zk serverIdPath >>= \case
    Just x -> return x; Nothing -> return M.empty

getCurrentServers :: ZHandle -> [CBytes] -> IO (M.Map CBytes String)
getCurrentServers zk = foldrM f M.empty
  where
    f x y = do
      name <- getServerNameFromId zk x
      return $ M.insert x name y

getServerNameFromId :: ZHandle -> CBytes -> IO String
getServerNameFromId zk serverId =
  decodeZNodeValue' zk $ serverIdPath <> "/" <> serverId

setPrevServers :: ZHandle -> M.Map CBytes String -> IO ()
setPrevServers zk = setZkData zk serverIdPath . encodeValueToBytes
