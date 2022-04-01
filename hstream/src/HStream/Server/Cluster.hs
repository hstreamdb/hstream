{-# LANGUAGE PatternSynonyms #-}
module HStream.Server.Cluster where

import           Control.Concurrent               (MVar, modifyMVar_)
import           Control.Exception                (SomeException, handle, try)
import           Control.Monad                    (void, when)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int32)
import           Data.List                        (sort)
import           Data.Maybe                       (isJust)
import qualified Data.Text                        as T
import           Data.Unique                      (hashUnique, newUnique)
import           Data.Word                        (Word32)
import           System.Exit                      (exitFailure)
import qualified Z.Data.CBytes                    as CB
import           Z.Data.CBytes                    (CBytes)
import           ZooKeeper                        (zooCreate, zooCreateOpInit,
                                                   zooExists, zooGet, zooMulti,
                                                   zooSetOpInit,
                                                   zooWatchGetChildren)
import           ZooKeeper.Exception              (ZNONODE)
import qualified ZooKeeper.Recipe                 as Recipe
import           ZooKeeper.Types                  (DataCompletion (..),
                                                   HsWatcherCtx (..), Stat (..),
                                                   StringVector (..),
                                                   StringsCompletion (..),
                                                   ZHandle,
                                                   pattern ZooEphemeral,
                                                   zooOpenAclUnsafe)

import           HStream.Common.ConsistentHashing (HashRing, constructServerMap)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence
import           HStream.Server.Types             (ServerContext (..), ServerID)
import           HStream.Utils                    (pattern EnumPB)

{-
  Starting hservers will no longer be happening in parallel.
-}
joinCluster :: ZHandle -> ServerID -> T.Text -> Word32 -> Word32 -> IO ()
joinCluster zk serverID host port port' = do
  let nodeInfo = NodeInfo { serverHost = host
                          , serverPort = port
                          , serverInternalPort = port'
                          }
  uniq <- newUnique
  void $ Recipe.withLock zk serverRootLockPath (CB.pack . show . hashUnique $ uniq) $ do
    serverStatusMap <- decodeZNodeValue zk serverRootPath
    let nodeStatus = ServerNodeStatus
          { serverNodeStatusState = EnumPB NodeStateRunning
          , serverNodeStatusNode  = Just ServerNode
              { serverNodeId = serverID
              , serverNodeHost = host
              , serverNodePort = port
              }
          }
    tryDeleteAllPath zk "/barrier" >> createPath zk "/barrier"
    let val = case serverStatusMap of
          Just hmap -> HM.insert serverID nodeStatus hmap
          Nothing   -> HM.singleton serverID nodeStatus
    let ops = [ createEphemeral (serverRootPath, Just $ encodeValueToBytes nodeInfo)
              , zooSetOpInit serverRootPath (Just $ encodeValueToBytes val) Nothing
              ]
    e' <- try $ zooMulti zk ops >> waitForApprovals zk >> tryDeleteAllPath zk "/barrier"
    case e' of
      Left (e :: SomeException) -> do
        Log.fatal . Log.buildString $ "Server failed to start: " <> show e
        exitFailure
      Right _ -> return ()
  where
    createEphemeral (path, content) =
      zooCreateOpInit (path <> "/" <> CB.pack (show serverID))
                      content 0 zooOpenAclUnsafe ZooEphemeral

waitForApprovals :: ZHandle  -> IO ()
waitForApprovals zk = do
  approvalRequired <- getChildrenNum zk serverRootPath
  approvals <- getChildrenNum zk "/barrier"
  when (approvals < approvalRequired - 1) $ waitForApprovals zk

actionTriggeredByServerNodeChange :: ServerContext -> IO ()
actionTriggeredByServerNodeChange sc@ServerContext {..}= do
  zooWatchGetChildren zkHandle serverRootPath
    callback action
  where
    callback HsWatcherCtx {..} = do
      actionTriggeredByServerNodeChange sc
    action (StringsCompletion (StringVector children)) = do
      updateHashRing zkHandle children loadBalanceHashRing
      approve zkHandle (CB.pack (show serverID))

-- However, reconstruct hashRing every time can be expensive
-- when we have a large number of nodes in the cluster.
-- TODO: Instead of reconstruction, we should use the operation insert/delete.
updateHashRing :: ZHandle -> [CBytes] -> MVar HashRing -> IO ()
updateHashRing zk children mhr = modifyMVar_ mhr $ \_ -> do
    serverNodes <- mapM (getServerNode' zk) children
    let newRing = constructServerMap . sort $ serverNodes
    Log.debug $ Log.buildString' newRing
    return newRing

approve :: ZHandle -> CBytes -> IO ()
approve zh sid = do
  exists <- zooExists zh "/barrier"
  when (isJust exists) $
    handle (\(e :: ZNONODE) -> putStr "------------++++++++++++++++++++++---------------------" >> print e)
      $ void $ zooCreate zh ("/barrier/" <> sid) Nothing zooOpenAclUnsafe ZooEphemeral

barrierPath :: CBytes
barrierPath = "/barrier"

getChildrenNum :: ZHandle -> CBytes -> IO Int32
getChildrenNum zk path = do
  DataCompletion _ Stat{..} <- zooGet zk path
  return statNumChildren
