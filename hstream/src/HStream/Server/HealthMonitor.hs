{-# LANGUAGE MultiWayIf #-}

module HStream.Server.HealthMonitor
 ( HealthMonitor
 , mkHealthMonitor
 , startMonitor
 )
where

import           Control.Concurrent                           (threadDelay)
import           Control.Exception                            (SomeException,
                                                               try)
import           Control.Monad                                (forever, when)

import           HStream.Common.ZookeeperClient               (unsafeGetZHandle)
import qualified HStream.Logger                               as Log
import           HStream.MetaStore.Types                      (MetaHandle (..))
import           HStream.MetaStore.ZookeeperUtils             (checkRecoverable)
import           HStream.Server.CacheStore                    (StoreMode (..),
                                                               dumpToHStore,
                                                               initCacheStore,
                                                               setCacheStoreMode)
import           HStream.Server.Types                         (ServerContext (..),
                                                               ServerMode (..),
                                                               getServerMode,
                                                               setServerMode)
import qualified HStream.Store                                as S

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
  res <- try @SomeException $ docheck sc hm
  case res of
    Left e  -> Log.fatal $ "monitor check error: " <> Log.build (show e)
    Right _ -> return ()

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
        initCacheStore cachedStore
        setCacheStoreMode cachedStore CacheMode
        setServerMode sc ServerBackup
        Log.info $ "change server to back up mode"
     | serverMode == ServerBackup && ldHealthy && metaHealthy -> do
        setServerMode sc ServerNormal
        setCacheStoreMode cachedStore DumpMode
        Log.info $ "change server to normal mode"
        dumpToHStore cachedStore scLDClient cmpStrategy
     | otherwise -> return ()

checkLdCluster :: HealthMonitor -> IO Bool
checkLdCluster HealthMonitor{..} = do
  S.isLdClusterHealthy ldChecker ldUnhealthyNodesLimit

checkMeta :: HealthMonitor -> IO Bool
checkMeta HealthMonitor{..} | ZKHandle c <- metaHandle = do
  checkRecoverable =<< unsafeGetZHandle c
checkMeta HealthMonitor{..} | _ <- metaHandle = do
  return True
