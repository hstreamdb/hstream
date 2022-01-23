{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization
  ( initializeServer
  , initNodePath
  ) where

import           Control.Concurrent               (MVar, newMVar)
import           Control.Exception                (SomeException, try)
import           Control.Monad                    (void)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (sort)
import qualified Data.Text                        as T
import qualified Data.Set as Set 
import           Data.Unique                      (hashUnique, newUnique)
import           Data.Word                        (Word32)
import           Network.GRPC.HighLevel.Generated
import           System.Exit                      (exitFailure)
import qualified Z.Data.CBytes                    as CB
import           Z.Foreign                        (toByteString)
import           ZooKeeper                        (zooCreateOpInit,
                                                   zooGetChildren, zooMulti,
                                                   zooSet, zooSetOpInit)
import qualified ZooKeeper.Recipe                 as Recipe
import           ZooKeeper.Types

import           HStream.Common.ConsistentHashing (HashRing, constructHashRing)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence       (NodeInfo (..),
                                                   decodeZNodeValue,
                                                   encodeValueToBytes,
                                                   getServerNode',
                                                   serverRootLockPath,
                                                   serverRootPath, tryGetChildren)
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import           HStream.Store                    (HsLogAttrs (..),
                                                   LogAttrs (..),
                                                   initCheckpointStoreLogID,
                                                   newLDClient)
import qualified HStream.Store.Admin.API          as AA
import           HStream.Utils

{-
  Starting hservers will no longer be happening in parallel.
  The status Running does not fully reflect running, however,
  if we want status to be more precise,
  two things are needed:
-}
-- TODO: a callback when grpc server is successfully started
-- TODO: a consistent algorithm to oversee the whole cluster
initNodePath :: ZHandle -> ServerID -> T.Text -> Word32 -> Word32 -> IO ()
initNodePath zk serverID host port port' = do
  let nodeInfo = NodeInfo { serverHost = host
                          , serverPort = port
                          , serverInternalPort = port'
                          }
  uniq <- newUnique
  void $ Recipe.withLock zk serverRootLockPath (CB.pack . show . hashUnique $ uniq) $ do
    serverStatusMap <- decodeZNodeValue zk serverRootPath
    let nodeStatus = ServerNodeStatus {
            serverNodeStatusState = mkEnumerated NodeStateRunning
          , serverNodeStatusNode  = Just ServerNode {
              serverNodeId = serverID
            , serverNodeHost = host
            , serverNodePort = port
          }
          }
    let val = case serverStatusMap of
          Just hmap -> HM.insert serverID nodeStatus hmap
          Nothing   -> HM.singleton serverID nodeStatus
    let ops = [ createEphemeral (serverRootPath, Just $ encodeValueToBytes nodeInfo)
              , zooSetOpInit serverRootPath (Just $ encodeValueToBytes val) Nothing
              ]
    e' <- try $ zooMulti zk ops
    case e' of
      Left (e :: SomeException) -> do
        Log.fatal . Log.buildString $ "Server failed to start: " <> show e
        exitFailure
      Right _ -> return ()
  where
    createEphemeral (path, content) =
      zooCreateOpInit (path <> "/" <> CB.pack (show serverID))
                      content 0 zooOpenAclUnsafe ZooEphemeral

initializeServer
  :: ServerOpts -> ZHandle
  -> IO (ServiceOptions, ServiceOptions, ServerContext)
initializeServer ServerOpts{..} zk = do
  Log.setLogLevel _serverLogLevel _serverLogWithColor
  ldclient <- newLDClient _ldConfigPath
  _ <- initCheckpointStoreLogID ldclient (LogAttrs $ HsLogAttrs _ckpRepFactor mempty)
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout

  statsHolder <- newStatsHolder

  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subscribeRuntimeInfo <- newMVar HM.empty

  hashRing <- initializeHashRing zk

  return (
    defaultServiceOptions {
      Network.GRPC.HighLevel.Generated.serverHost =
        Host . toByteString . CB.toBytes $ _serverHost
    , Network.GRPC.HighLevel.Generated.serverPort =
        Port . fromIntegral $ _serverPort
    },
    defaultServiceOptions {
      Network.GRPC.HighLevel.Generated.serverHost =
        Host . toByteString . CB.toBytes $ _serverHost
    , Network.GRPC.HighLevel.Generated.serverPort =
        Port . fromIntegral $ _serverInternalPort
    },
    ServerContext {
      zkHandle                 = zk
    , scLDClient               = ldclient
    , serverID                 = _serverID
    , scDefaultStreamRepFactor = _topicRepFactor
    , runningQueries           = runningQs
    , runningConnectors        = runningCs
    , scSubscribeRuntimeInfo     = subscribeRuntimeInfo
    , cmpStrategy              = _compression
    , headerConfig             = headerConfig
    , scStatsHolder            = statsHolder
    , loadBalanceHashRing      = hashRing
    })

--------------------------------------------------------------------------------

initializeHashRing :: ZHandle -> IO (MVar HashRing)
initializeHashRing zk = do
  StringsCompletion (StringVector children) <-
    zooGetChildren zk serverRootPath
  serverNodes <- mapM (getServerNode' zk) children
  newMVar . constructHashRing . sort $ serverNodes

--------------------------------------------------------------------------------
