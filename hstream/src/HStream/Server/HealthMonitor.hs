{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MultiWayIf   #-}

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
import qualified HStream.Stats                    as ST
import qualified HStream.Store                    as S

data HealthMonitor = HealthMonitor
  { ldChecker             :: S.LdChecker
  , metaHandle            :: MetaHandle
  , statsHolder           :: ST.StatsHolder
  , ldUnhealthyNodesLimit :: Int
  }

mkHealthMonitor :: S.LDClient -> ST.StatsHolder -> MetaHandle -> Int -> IO HealthMonitor
mkHealthMonitor ldClient statsHolder metaHandle ldUnhealthyNodesLimit = do
  ldChecker <- S.newLdChecker ldClient
  return HealthMonitor{..}

startMonitor :: ServerContext -> HealthMonitor -> Int -> IO ()
startMonitor sc hm delaySecond = forever $ do
  threadDelay $ delaySecond * 1000 * 1000
  start <- getCurrentTime
  -- Log.debug $ "========== docheck start..." <> " in " <> Log.build (show start)
  res <- try @SomeException $ docheck sc hm
  end <- getCurrentTime
  case res of
    Left e  -> Log.fatal $ "monitor check error: " <> Log.build (show e)
    Right _ -> return ()
  let diff = nominalDiffTimeToSeconds $ diffUTCTime end start
  when (diff > 1) $
    Log.warning $ "Monitor check return slow, total use " <> Log.build (show diff) <> "s"
  Log.debug $ "Health monitor finish check in " <> Log.build (show end)
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
  withLatency statsHolder "LdCluster" $ S.isLdClusterHealthy ldChecker ldUnhealthyNodesLimit

checkMeta :: HealthMonitor -> IO Bool
checkMeta HealthMonitor{..} | ZKHandle c <- metaHandle = do
  withLatency statsHolder "MetaCluster" $ checkRecoverable =<< unsafeGetZHandle c
checkMeta HealthMonitor{..} | _ <- metaHandle = do
  return True

withLatency :: ST.StatsHolder -> String -> IO a -> IO a
withLatency statsHolder checkType action = do
  !start <- getTime Monotonic
  res <- action
  !end <- getTime Monotonic
  let msDuration = toNanoSecs (diffTimeSpec end start) `div` 1000000
  when (msDuration > 1000) $
    Log.warning $ "check " <> Log.build checkType <> " return slow, total time " <> Log.build msDuration <> "ms"
  case checkType of
    "LdCluster"   -> do
      ST.serverHistogramAdd statsHolder ST.SHL_CheckStoreClusterLatency (fromIntegral msDuration)
    "MetaCluster" -> do
      ST.serverHistogramAdd statsHolder ST.SHL_CheckMetaClusterLatency (fromIntegral msDuration)
    _      -> return ()
  return res
