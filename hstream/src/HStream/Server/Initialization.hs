{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization
  ( initializeServer
  , initNodePath
  ) where

import           Control.Concurrent               (MVar, newEmptyMVar, newMVar)
import           Control.Exception                (SomeException, try)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (sort)
import qualified Data.Map                         as Map
import qualified Data.Text                        as T
import           Data.Word                        (Word32)
import           Network.GRPC.HighLevel.Generated
import           System.Exit                      (exitFailure)
import qualified Z.Data.CBytes                    as CB
import           Z.Foreign                        (toByteString)
import           ZooKeeper                        (zooCreateOpInit,
                                                   zooGetChildren, zooMulti)
import           ZooKeeper.Types

import qualified HStream.Logger                   as Log
import           HStream.Server.ConsistentHashing (constructHashRing)
import           HStream.Server.Persistence       (NodeInfo (..),
                                                   NodeStatus (..),
                                                   decodeZNodeValue',
                                                   encodeValueToBytes,
                                                   serverIdPath, serverRootPath, getServerNode)
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import           HStream.Store                    (HsLogAttrs (..),
                                                   LogAttrs (..),
                                                   initCheckpointStoreLogID,
                                                   newLDClient)
import qualified HStream.Store.Admin.API          as AA

initNodePath :: ZHandle -> ServerID -> T.Text -> Word32 -> Word32 -> IO ()
initNodePath zk serverID host port port' = do
  let nodeInfo = NodeInfo { nodeStatus = Ready
                          , serverHost = host
                          , serverPort = port
                          , serverInternalPort = port'
                          }
  let ops = [ createEphemeral (serverRootPath, Just $ encodeValueToBytes nodeInfo)
            -- , createEphemeral (serverLoadPath, Nothing)
            , zooCreateOpInit (serverIdPath <> "/")
                      (Just (encodeValueToBytes serverID)) 0 zooOpenAclUnsafe ZooEphemeralSequential
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
  subscriptionCtx <- newMVar Map.empty

  currentLeader <- newEmptyMVar

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
    , subscribeRuntimeInfo     = subscribeRuntimeInfo
    , subscriptionCtx          = subscriptionCtx
    , cmpStrategy              = _compression
    , headerConfig             = headerConfig
    , scStatsHolder            = statsHolder
    , leaderID                 = currentLeader
    , loadBalanceHashRing      = hashRing
    })

--------------------------------------------------------------------------------

initializeHashRing :: ZHandle -> IO (MVar HashRing)
initializeHashRing zk = do
  StringsCompletion (StringVector children) <-
    zooGetChildren zk serverIdPath
  serverNodes <- mapM getServerNodeFromId (sort children)
  newMVar $ constructHashRing 7 serverNodes
  where
    getServerNodeFromId seqId = do
      serverID <- decodeZNodeValue' zk $ serverIdPath <> "/" <> seqId
      getServerNode zk serverID
