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
import qualified Data.Text.Lazy                   as TL
import           Data.Time.Clock.System           (SystemTime (..),
                                                   getSystemTime)
import           Network.GRPC.HighLevel.Generated
import           System.Exit                      (exitFailure)
import           System.Statgrab                  (CPU (..), NetworkIO (..),
                                                   NetworkInterface (..), Stats,
                                                   runStats, snapshot,
                                                   snapshots)
import qualified Z.Data.CBytes                    as CB
import           Z.Foreign                        (toByteString)
import           ZooKeeper                        (zooCreateOpInit, zooMulti)
import           ZooKeeper.Types

import qualified HStream.Logger                   as Log
import           HStream.Server.LoadBalance       (updateLoadReport)
import           HStream.Server.Persistence       (NodeInfo (..),
                                                   NodeStatus (..),
                                                   serverLoadPath,
                                                   serverRootPath)
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import           HStream.Store                    (HsLogAttrs (HsLogAttrs),
                                                   LogAttrs (LogAttrs),
                                                   initCheckpointStoreLogID,
                                                   newLDClient)
import qualified HStream.Store.Admin.API          as AA
import           HStream.Utils                    (valueToBytes)

initNodePath :: ZHandle -> CB.CBytes -> TL.Text -> TL.Text -> IO ()
initNodePath zk serverName uri uri' = do
  let nodeInfo = NodeInfo { nodeStatus = Ready, serverUri = uri, serverInternalUri = uri'}
  let ops = [ createEphemeral (serverRootPath, Just $ valueToBytes nodeInfo)
            , createEphemeral (serverLoadPath, Nothing)
            ]
  e' <- try $ zooMulti zk ops
  case e' of
    Left (e :: SomeException) -> do
      Log.fatal . Log.buildString $ "Server failed to start: " <> show e
      exitFailure
    Right _ -> return ()
  where
    createEphemeral (path, content) =
      zooCreateOpInit (path <> "/" <> serverName)
                      content 0 zooOpenAclUnsafe ZooEphemeral

initializeServer
  :: ServerOpts -> ZHandle
  -> IO (ServiceOptions, ServiceOptions, ServerContext, LoadManager)
initializeServer ServerOpts{..} zk = do
  Log.setLogLevel _serverLogLevel _serverLogWithColor
  ldclient <- newLDClient _ldConfigPath
  _ <- initCheckpointStoreLogID ldclient (LogAttrs $ HsLogAttrs _ckpRepFactor mempty)
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout

  statsHolder <- newStatsHolder

  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subscribeRuntimeInfo <- newMVar HM.empty

  lastSysResUsage <- initLastSysResUsage
  currentLoadReport <- initLoadReport lastSysResUsage
  currentLoadReports <- newMVar HM.empty

  currentLeader <- newEmptyMVar

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
    , serverName               = _serverName
    , scDefaultStreamRepFactor = _topicRepFactor
    , minServers               = _serverMinNum
    , runningQueries           = runningQs
    , runningConnectors        = runningCs
    , subscribeRuntimeInfo     = subscribeRuntimeInfo
    , cmpStrategy              = _compression
    , headerConfig             = headerConfig
    , scStatsHolder            = statsHolder
    , leaderName               = currentLeader
    },
    LoadManager {
      sName           = _serverName
    , loadReport      = currentLoadReport
    , lastSysResUsage = lastSysResUsage
    , loadReports     = currentLoadReports
    })

--------------------------------------------------------------------------------

initLastSysResUsage :: IO (MVar SystemResourceUsage)
initLastSysResUsage = do
  CPU{..} <- runStats (snapshot :: Stats CPU)
  _nis <- runStats (snapshots :: Stats [NetworkInterface])
  _nios <- runStats (snapshots :: Stats [NetworkIO])
  MkSystemTime seconds _ <- getSystemTime
  let _temp = filter ((== 1) . ifaceUp . fst) . zip _nis $ _nios
      nios  = snd <$> _temp
  newMVar SystemResourceUsage {
    cpuUsage = (cpuIdle, cpuTotal)
  , txTotal = sum $ ifaceTX <$> nios
  , rxTotal = sum $ ifaceRX <$> nios
  , collectedTime = toInteger seconds}

initLoadReport :: MVar SystemResourceUsage -> IO (MVar LoadReport)
initLoadReport mSysResUsage = do
  lrMVar <- newMVar LoadReport {
      systemResourceUsage = SystemResourcePercentageUsage 0 0 0 0
    , isUnderloaded = True
    , isOverloaded = False
    }
  updateLoadReport mSysResUsage lrMVar
  return lrMVar
