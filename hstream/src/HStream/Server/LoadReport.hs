{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.LoadReport
  ( localReportUpdateTimer
  , updateLoadReport
  ) where

import           Control.Concurrent
import           Control.Concurrent.Suspend (sDelay)
import           Control.Concurrent.Timer   (newTimer, repeatedStart)
import           Control.Monad
import           Data.Time.Clock.System     (SystemTime (..), getSystemTime)
import           System.Statgrab

import           HStream.Server.Types

localReportUpdateTimer :: MVar LoadReport -> IO ()
localReportUpdateTimer mlr = do
  timer <- newTimer
  void $ repeatedStart timer (updateLoadReport mlr) (sDelay 5)

-- | Update Local Load Report
updateLoadReport :: MVar LoadReport -> IO ()
updateLoadReport mLoadReport = do
  LoadReport {..} <- readMVar mLoadReport
  loadReport <- generateLoadReport sysResUsage
  void $ swapMVar mLoadReport loadReport

--------------------------------------------------------------------------------
-- Get Stats

generateLoadReport :: SystemResourceUsage -> IO LoadReport
generateLoadReport sys@SystemResourceUsage {..} = do
  (newCpuUsage, cpuPct) <- getCpuPercentage cpuUsage
  memPct <- getMemPercentage
  MkSystemTime _seconds _ <- getSystemTime
  let seconds = toInteger _seconds
  ((tx, rx), (bandwidthOutUsage, bandwidthInUsage))
    <- getTotalNicUsage (txTotal, rxTotal) (seconds - collectedTime)
  return (LoadReport {
      sysResUsage = sys {
        cpuUsage = newCpuUsage
      , collectedTime = seconds
      , txTotal = tx
      , rxTotal = rx
    }
    , sysResPctUsage = SystemResourcePercentageUsage {
        cpuPctUsage = cpuPct
      , memoryPctUsage = memPct
      , bandwidthInUsage = bandwidthInUsage
      , bandwidthOutUsage = bandwidthOutUsage
      }
    , isUnderloaded = False
    , isOverloaded  = False
    })

getCpuPercentage :: (Integer, Integer) -> IO ((Integer, Integer), Double)
getCpuPercentage (lastCpuUsage, lastCpuTotal) = do
  CPU {..} <- runStats (snapshot :: Stats CPU)
  let cpuUsage = cpuTotal - cpuIdle
  let cpuPctUsage = fromIntegral (cpuUsage - lastCpuUsage)
                  / fromIntegral (cpuTotal - lastCpuTotal) * 100
  return ((cpuUsage, cpuTotal), cpuPctUsage)

getMemPercentage :: IO Double
getMemPercentage = do
  Memory{..} <- runStats (snapshot :: Stats Memory)
  return $ fromIntegral memUsed / fromIntegral memTotal * 100

getTotalNicUsage
  :: (Integer, Integer) -> Integer
  -> IO ((Integer, Integer), (Double, Double))
getTotalNicUsage (lastTx, lastRx) timeDiff = do
  _nis <- runStats (snapshots :: Stats [NetworkInterface])
  _nios <- runStats (snapshots :: Stats [NetworkIO])
  let _temp = filter ((== 1) . ifaceUp . fst) . zip _nis $ _nios
      nis   = fst <$> _temp
      nios  = snd <$> _temp
  let limit     = sum $ ifaceSpeed <$> nis
      currentTx = sum $ ifaceTX <$> nios
      currentRx = sum $ ifaceRX <$> nios
      currentOutPct = fromIntegral (currentTx - lastTx) * 8
                    / fromIntegral timeDiff
                    / fromIntegral limit * 100
      currentInPct = fromIntegral (currentRx - lastRx) * 8
                   / fromIntegral timeDiff
                   / fromIntegral limit * 100
  return ((currentTx, currentRx)
         ,(currentOutPct, currentInPct))

-- -------------------------------------------------------------------------- --

initLastSysResUsage :: IO SystemResourceUsage
initLastSysResUsage = do
  CPU{..} <- runStats (snapshot :: Stats CPU)
  _nis <- runStats (snapshots :: Stats [NetworkInterface])
  _nios <- runStats (snapshots :: Stats [NetworkIO])
  MkSystemTime seconds _ <- getSystemTime
  let _temp = filter ((== 1) . ifaceUp . fst) . zip _nis $ _nios
      nios  = snd <$> _temp
  return SystemResourceUsage {
    cpuUsage = (cpuIdle, cpuTotal)
  , txTotal = sum $ ifaceTX <$> nios
  , rxTotal = sum $ ifaceRX <$> nios
  , collectedTime = toInteger seconds}

initLoadReport :: SystemResourceUsage -> IO (MVar LoadReport)
initLoadReport sysResUsage = do
  lrMVar <- newMVar LoadReport {
      sysResUsage = sysResUsage
    , sysResPctUsage = SystemResourcePercentageUsage 0 0 0 0
    , isUnderloaded = True
    , isOverloaded = False
    }
  updateLoadReport lrMVar
  return lrMVar
