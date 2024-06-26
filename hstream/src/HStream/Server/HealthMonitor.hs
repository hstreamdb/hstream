{-# LANGUAGE MultiWayIf #-}

module HStream.Server.HealthMonitor
 ( HealthMonitor
 , mkHealthMonitor
 , startMonitor
 )
where

import           Control.Concurrent               (threadDelay)
import           Control.Exception                (SomeException, try)
import           Control.Monad                    (forever, when)
import           Data.Time
import           System.Clock

import           HStream.Common.ZookeeperClient   (unsafeGetZHandle)
import qualified HStream.Logger                   as Log
import           HStream.MetaStore.Types          (MetaHandle (..))
import           HStream.MetaStore.ZookeeperUtils (checkRecoverable)
import           HStream.Server.CacheStore        (StoreMode (..), dumpToHStore,
                                                   initCacheStore,
                                                   setCacheStoreMode)
import           HStream.Server.Types             (ServerContext (..),
                                                   ServerMode (..),
                                                   getServerMode, setServerMode)
import qualified HStream.Store                    as S

data HealthMonitor = HealthMonitor
  { ldChecker             :: S.LdChecker
  , metaHandle            :: MetaHandle
  , ldUnhealthyNodesLimit :: Int
  }

mkHealthMonitor :: S.LDClient -> MetaHandle -> Int -> IO HealthMonitor
mkHealthMonitor ldClient metaHandle ldUnhealthyNodesLimit = do
  ldChecker <- S.newLdChecker ldClient
  return HealthMonitor{..}

startMonitor :: ServerContext -> HealthMonitor -> Int -> IO ()
startMonitor sc hm delaySecond = forever $ do
  threadDelay $ delaySecond * 1000 * 1000
  start <- getCurrentTime
  Log.debug $ "========== docheck start..." <> " in " <> Log.build (show start)
  res <- try @SomeException $ docheck sc hm
  end <- getCurrentTime
  case res of
    Left e  -> Log.fatal $ "monitor check error: " <> Log.build (show e)
    Right _ -> return ()
  let diff = nominalDiffTimeToSeconds $ diffUTCTime end start
  when (diff > 1) $
    Log.warning $ "Monitor check return slow, total use " <> Log.build (show diff) <> "s"
  Log.debug $ "========== docheck end..." <> " in " <> Log.build (show end)
          <> ", with start time: " <> Log.build (show start)
          <> ", duration: " <> Log.build (show diff)

docheck :: ServerContext -> HealthMonitor -> IO ()
docheck sc@ServerContext{..} hm = do
  ldHealthy <- checkLdCluster hm
  metaHealthy <- checkMeta hm
  serverMode <- getServerMode sc
  when (not ldHealthy || not metaHealthy) $
    Log.warning $ "Healthy check find unhealthy service: "
               <> "ld cluster healthy: " <> Log.build (show ldHealthy)
               <> ", meta cluster healthy: " <> Log.build (show metaHealthy)
               <> ", serverMode: " <> Log.build (show serverMode)
  if | serverMode == ServerNormal && not ldHealthy && not metaHealthy -> do
        initCacheStore cacheStore
        setCacheStoreMode cacheStore CacheMode
        setServerMode sc ServerBackup
        Log.info $ "Change server to back up mode"
     | serverMode == ServerBackup && ldHealthy && metaHealthy -> do
        setServerMode sc ServerNormal
        setCacheStoreMode cacheStore DumpMode
        Log.info $ "Change server to normal mode"
        dumpToHStore cacheStore scLDClient cmpStrategy
     | otherwise -> return ()

checkLdCluster :: HealthMonitor -> IO Bool
checkLdCluster HealthMonitor{..} = do
  start <- getTime Monotonic
  res <- S.isLdClusterHealthy ldChecker ldUnhealthyNodesLimit
  end <- getTime Monotonic
  let sDuration = toNanoSecs (diffTimeSpec end start) `div` 1000000
  if sDuration > 1000
    then Log.warning $ "CheckLdCluster slow, total time " <> Log.build sDuration <> "ms"
    else Log.debug $ "Finish checkLdClusster, total time " <> Log.build sDuration <> "ms"
  return res

checkMeta :: HealthMonitor -> IO Bool
checkMeta HealthMonitor{..} | ZKHandle c <- metaHandle = do
  start <- getTime Monotonic
  res <- checkRecoverable =<< unsafeGetZHandle c
  end <- getTime Monotonic
  let sDuration = toNanoSecs (diffTimeSpec end start) `div` 1000000
  if sDuration > 1000
    then Log.warning $ "CheckMeta slow, total time " <> Log.build sDuration <> "ms"
    else Log.debug $ "Finish checkMeta, total time " <> Log.build sDuration <> "ms"
  return res
checkMeta HealthMonitor{..} | _ <- metaHandle = do
  return True
