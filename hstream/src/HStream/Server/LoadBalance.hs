{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module HStream.Server.LoadBalance where


import           Control.Concurrent
import           Control.Concurrent.Suspend (mDelay, sDelay)
import           Control.Concurrent.Timer   (newTimer, repeatedStart)
import           Control.Monad              (forever, void, when)
import           Data.Aeson                 (decode, encode)
import qualified Data.HashMap.Strict        as HM
import           Data.List                  (sortOn, (\\))
import           Data.Time.Clock.System     (SystemTime (..), getSystemTime)
import           GHC.IO                     (unsafePerformIO)
import           System.Statgrab
import           ZooKeeper                  (zooGetChildren, zooSet,
                                             zooWatchGet)
import           ZooKeeper.Types

import qualified HStream.Logger             as Log
import           HStream.Server.Persistence (serverLoadPath, serverRootPath)
import           HStream.Server.Types
import           HStream.Utils              (bytesToLazyByteString,
                                             lazyByteStringToBytes)

writeLoadReportToZooKeeper :: ZHandle -> ServerName -> LoadReport -> IO ()
writeLoadReportToZooKeeper zk name lr = void $
  zooSet zk (serverLoadPath <> "/" <> name) (Just ((lazyByteStringToBytes . encode) lr)) Nothing

-- Update data on the server according to the data in zk and set watch for any new updates
updateLoadReports
  :: ZHandle -> MVar ServerLoadReports
  -> MVar ServerRanking
  -> IO ()
updateLoadReports zk hmapM rankingM = do
  Log.debug . Log.buildString $ "Updating local load reports map and set watch on every server"
  names <- unStrVec . strsCompletionValues <$> zooGetChildren zk serverRootPath
  _names <- HM.keys <$> readMVar hmapM
  mapM_ (getAndWatchReport zk hmapM rankingM) (names \\ _names)

--------------------------------------------------------------------------------

getAndWatchReport
  :: ZHandle
  -> MVar ServerLoadReports
  -> MVar ServerRanking
  -> ServerName
  -> IO ()
getAndWatchReport zk hmapM rankingM name = do
  Log.debug . Log.buildCBytes $ "Getting data from zk and setting watch for server " <> name
  _ <- forkIO $ zooWatchGet zk
                (serverLoadPath <> "/" <> name)
                (watchFun hmapM rankingM name)
                (getReportCB hmapM rankingM name)
  Log.debug . Log.buildCBytes $ "Getting data from zk and set watch for server " <> name <> " done"

-- | The watch function set on every server load report in zk
watchFun
  :: MVar ServerLoadReports
  -> MVar ServerRanking
  -> ServerName
  -> HsWatcherCtx -> IO ()
watchFun hmapM rankingM address HsWatcherCtx{..} = do
  Log.debug . Log.buildString $ "Watch triggered, updating map and ranking "
  case watcherCtxType of
    ZooDeleteEvent -> do
      modifyMVar_ hmapM (pure . HM.delete address)
      updateMapAndRanking hmapM rankingM
    ZooChangedEvent -> do
      getAndWatchReport watcherCtxZHandle hmapM rankingM address
    _ -> return ()

-- | The call back function when we get load report info
getReportCB :: MVar ServerLoadReports -> MVar ServerRanking -> ServerName ->
  DataCompletion -> IO ()
getReportCB hmapM rankingM address DataCompletion{..} = do
  let lr = decode . bytesToLazyByteString <$> dataCompletionValue
  insertLoadReportsMap hmapM address lr
  updateMapAndRanking hmapM rankingM

updateMapAndRanking :: MVar ServerLoadReports -> MVar ServerRanking -> IO ()
updateMapAndRanking hmapM rankingM = do
  hmap <- readMVar hmapM
  Log.debug . Log.buildString $
    "Local loads map updated: " <> show hmap
  let newRanking = generateRanking hmap
  void $ swapMVar rankingM newRanking
  Log.debug . Log.buildString $
    "Ranking updated, new ranking is " <> show newRanking

-- Update the load report map stored in the server local data
insertLoadReportsMap
  :: MVar ServerLoadReports
  -> ServerName
  -> Maybe (Maybe LoadReport)
  -> IO ()
insertLoadReportsMap mVar name (Just (Just x)) = do
  Log.debug . Log.buildCBytes $ "Updating load info of " <> name <> " in load reports map"
  modifyMVar_ mVar (return . HM.insert name x)
  Log.debug . Log.buildCBytes $ "Load info of " <> name <> " in load reports map updated"
insertLoadReportsMap _ _ _ = pure ()

-- TODO: This should return a list of list instead / or just the candidates
generateRanking :: HM.HashMap ServerName LoadReport -> [ServerName]
generateRanking hmap = fst <$> sortOn (getPercentageUsage . snd) (HM.toList hmap)

-- TODO: add a more sophisticated algorithm to decide the score of a server
getPercentageUsage :: LoadReport -> Double
getPercentageUsage LoadReport {systemResourceUsage = SystemResourcePercentageUsage {..} }
  = maximum [cpuPctUsage, memoryPctUsage] --, bandwidthInUsage, bandwidthOutUsage]

--------------------------------------------------------------------------------
-- Timer

localReportUpdateTimer :: ServerContext -> LoadManager -> IO ()
localReportUpdateTimer ServerContext{..} LoadManager{..}= do
  timer1 <- newTimer
  void $ repeatedStart timer1
    (do
      hmap <- readMVar loadReports
      updateLoadReport lastSysResUsage loadReport
      lr' <- readMVar loadReport
      Log.debug . Log.buildString $ "Scheduled local report update" <> show lr'
      when (abs (getPercentageUsage lr' -
            maybe 0 getPercentageUsage (HM.lookup serverName hmap)) > 5)
        (putMVar ifUpdateZK ()))
    (sDelay 5)

zkReportUpdateTimer :: ServerName -> ServerContext -> IO ()
zkReportUpdateTimer name ServerContext{..} = do
  timer2 <- newTimer
  _ <- repeatedStart timer2
    (do
      putMVar ifUpdateZK ()
      Log.debug . Log.buildString $ "Scheduled zk local report store")
    (mDelay 5)
  void $ forkIO $ forever $ do
    _ <- takeMVar ifUpdateZK
    readMVar loadReport >>=
      writeLoadReportToZooKeeper zkHandle name

ifUpdateZK :: MVar ()
ifUpdateZK = unsafePerformIO $ newMVar ()
{-# NOINLINE ifUpdateZK #-}

-- | Update Local Load Report
updateLoadReport :: MVar SystemResourceUsage -> MVar LoadReport -> IO ()
updateLoadReport mSysResUsg mLoadReport = do
  lastUsage <- readMVar mSysResUsg
  (lastUsage', loadReport) <- generateLoadReport lastUsage
  void $ swapMVar mLoadReport loadReport
  void $ swapMVar mSysResUsg lastUsage'

--------------------------------------------------------------------------------
-- Get Stats

generateLoadReport :: SystemResourceUsage -> IO (SystemResourceUsage, LoadReport)
generateLoadReport sys@SystemResourceUsage {..} = do
  (newCpuUsage, cpuPct) <- getCpuPercentage cpuUsage
  memPct <- getMemPercentage
  MkSystemTime _seconds _ <- getSystemTime
  let seconds = toInteger _seconds
  ((tx, rx), (bandwidthOutUsage, bandwidthInUsage))
    <- getTotalNicUsage (txTotal, rxTotal) (seconds - collectedTime)
  return (sys {
      cpuUsage = newCpuUsage
    , collectedTime = seconds
    , txTotal = tx
    , rxTotal = rx
    }, LoadReport {
      systemResourceUsage = SystemResourcePercentageUsage {
        cpuPctUsage = cpuPct
      , memoryPctUsage = memPct
      , bandwidthInUsage = bandwidthInUsage
      , bandwidthOutUsage = bandwidthOutUsage
      }
    , isUnderloaded = False
    , isOverloaded = False
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

--------------------------------------------------------------------------------
-- Implementation details
-- A Server

-- On one thread:
-- generateLoadReport (generate ServerData)
-- -> compare to the old loadReport stored in zk -> update loadReport in zk

-- On the other thread:
-- getAllLoadReports from ZK and set a watch function
-- watch function should automatically update ranking
