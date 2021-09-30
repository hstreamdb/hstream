{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Initialization where

import           Control.Concurrent               (MVar, newMVar, readMVar)
import qualified Data.HashMap.Strict              as HM
import           Data.Time.Clock.System           (SystemTime (..),
                                                   getSystemTime)
import           Network.GRPC.HighLevel.Generated
import           System.Statgrab                  (CPU (..), NetworkIO (..),
                                                   NetworkInterface (..), Stats,
                                                   runStats, snapshot,
                                                   snapshots)
import qualified Z.Data.CBytes                    as CB
import           Z.Foreign                        (toByteString)
import           ZooKeeper.Types

import qualified HStream.Logger                   as Log
import           HStream.Server.LoadBalance
import           HStream.Server.Types
import           HStream.Stats                    (newStatsHolder)
import           HStream.Store                    (HsLogAttrs (HsLogAttrs),
                                                   LogAttrs (LogAttrs),
                                                   initCheckpointStoreLogID,
                                                   newLDClient)
import qualified HStream.Store.Admin.API          as AA
import           HStream.Utils                    (setupSigsegvHandler)

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
  :: ServerOpts -> ZHandle
  -> IO (ServiceOptions, ServerContext, LoadManager)
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

  -- data for load balance
  currentRanking     <- newMVar []
  currentLoadReports <- newMVar HM.empty
  return (
    defaultServiceOptions {
      Network.GRPC.HighLevel.Generated.serverHost =
        Host . toByteString . CB.toBytes $ _serverHost
    , Network.GRPC.HighLevel.Generated.serverPort =
        Port . fromIntegral $ _serverPort
    },
    ServerContext {
      zkHandle                 = zk
    , scLDClient               = ldclient
    , serverName               = _serverName
    , scDefaultStreamRepFactor = _topicRepFactor
    , runningQueries           = runningQs
    , runningConnectors        = runningCs
    , subscribeRuntimeInfo     = subscribeRuntimeInfo
    , cmpStrategy              = _compression
    , headerConfig             = headerConfig
    , scStatsHolder            = statsHolder
    , loadReport = currentLoadReport
    , ranking    = currentRanking
    },
    LoadManager {
      lastSysResUsage = lastSysResUsage
    , loadReports = currentLoadReports
    })
