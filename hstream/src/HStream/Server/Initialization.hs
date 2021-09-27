{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization where

import           Control.Concurrent               (MVar, newMVar, readMVar)
import           Control.Exception                (SomeException, try)
import qualified Data.HashMap.Strict              as HM
import           Data.Time.Clock.System           (SystemTime (..),
                                                   getSystemTime)
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
import           HStream.Server.LoadBalance
import           HStream.Server.Persistence       (serverLoadPath,
                                                   serverRootPath)
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import           HStream.Store                    (HsLogAttrs (HsLogAttrs),
                                                   LogAttrs (LogAttrs),
                                                   initCheckpointStoreLogID,
                                                   newLDClient)
import qualified HStream.Store.Admin.API          as AA
import           HStream.Utils                    (setupSigsegvHandler,
                                                   valueToBytes)
import           Network.GRPC.HighLevel.Generated

initNodePath :: ZHandle -> CB.CBytes -> String -> String -> IO ()
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

initialRanking :: MVar (HM.HashMap ServerName LoadReport) -> IO [ServerName]
initialRanking hmapM = do
  hmap <- readMVar hmapM
  return $ generateRanking hmap

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

initializeServer
  :: ServerOpts
  -> IO (ServiceOptions, ServiceOptions, ZHandle -> ServerContext, LoadManager)
initializeServer ServerOpts{..} = do
  Log.setLogLevel _serverLogLevel _serverLogWithColor
  setupSigsegvHandler
  ldclient <- newLDClient _ldConfigPath
  _ <- initCheckpointStoreLogID ldclient (LogAttrs $ HsLogAttrs _ckpRepFactor mempty)
  let headerConfig = AA.HeaderConfig _ldAdminHost _ldAdminPort _ldAdminProtocolId _ldAdminConnTimeout _ldAdminSendTimeout _ldAdminRecvTimeout

  statsHolder <- newStatsHolder

  lastSysResUsage <- initLastSysResUsage
  currentLoadReport <- initLoadReport lastSysResUsage

  currentLeader <- newMVar CB.empty

  -- data for load balance
  currentRanking     <- newMVar []
  currentLoadReports <- newMVar HM.empty
  runningQs <- newMVar HM.empty
  runningCs <- newMVar HM.empty
  subscribeRuntimeInfo <- newMVar HM.empty
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
    \zk -> ServerContext {
      zkHandle   = zk
    , serverName = _serverName
    , runningQueries           = runningQs
    , serverHost = _serverHost
    , scLDClient               = ldclient
    , scDefaultStreamRepFactor = _topicRepFactor
    , runningConnectors        = runningCs
    , subscribeRuntimeInfo     = subscribeRuntimeInfo
    , cmpStrategy              = _compression
    , headerConfig             = headerConfig
    , scStatsHolder            = statsHolder
    , serverPort = fromIntegral _serverPort
    , serverInternalPort  = fromIntegral _serverInternalPort
    , loadReport = currentLoadReport
    , ranking    = currentRanking
    , leaderName = currentLeader
    },
    LoadManager {
      lastSysResUsage = lastSysResUsage
    , loadReports = currentLoadReports
    })
