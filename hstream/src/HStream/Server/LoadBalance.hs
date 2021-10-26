{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.LoadBalance
  ( getNodesRanking

  , startLoadBalancer
  , startWritingLoadReport
  , getRanking

  , updateLoadReport
  , updateLoadReports
  ) where

import           Control.Concurrent
import           Control.Concurrent.Suspend       (mDelay, sDelay)
import           Control.Concurrent.Timer         (newTimer, repeatedStart)
import           Control.Monad
import           Data.Aeson                       (decode, encode)
import qualified Data.HashMap.Strict              as HM
import           Data.IORef                       (IORef, atomicWriteIORef,
                                                   newIORef, readIORef)
import           Data.List                        (sortOn, (\\))
import           Data.Time.Clock.System           (SystemTime (..),
                                                   getSystemTime)
import qualified Data.Vector                      as V
import           GHC.IO                           (unsafePerformIO)
import           Network.GRPC.HighLevel.Client
import           Network.GRPC.HighLevel.Generated (withGRPCClient)
import           System.Statgrab
import           ZooKeeper                        (zooGetChildren, zooSet,
                                                   zooWatchGet)
import           ZooKeeper.Types

import           HStream.Client.Utils             (mkClientNormalRequest,
                                                   mkGRPCClientConf,
                                                   serverNodeToSocketAddr)
import qualified HStream.Logger                   as Log
import           HStream.Server.HStreamApi        (ServerNode)
import           HStream.Server.HStreamInternal
import           HStream.Server.Persistence       (getServerInternalAddr,
                                                   getServerNode,
                                                   serverLoadPath,
                                                   serverRootPath)
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (Empty))
import           HStream.Utils                    (bytesToLazyByteString,
                                                   lazyByteStringToBytes)

--------------------------------------------------------------------------------

getNodesRanking :: ServerContext -> IO [ServerNode]
getNodesRanking ServerContext{..} = do
  leader <- readMVar leaderName
  case serverName == leader of
    True -> do
      getRanking >>= mapM (getServerNode zkHandle)
    False -> do
      addr <- getServerInternalAddr zkHandle leader
      withGRPCClient (mkGRPCClientConf addr) $ \client -> do
        HStreamInternal{..} <- hstreamInternalClient client
        hstreamInternalGetNodesRanking (mkClientNormalRequest Empty) >>= \case
          ClientNormalResponse (GetNodesRankingResponse nodes) _meta1 _meta2 _code _details -> do
            return $ V.toList nodes
          _ -> do
            Log.warning . Log.buildCBytes $
              "Failed to get nodes ranking from leader " <> leader
            return []

--------------------------------------------------------------------------------

getRanking :: IO ServerRanking
getRanking = readIORef serverRanking

startLoadBalancer :: ZHandle -> LoadManager -> IO ()
startLoadBalancer zk lm@LoadManager{..} = do
  -- Write load balancing data
  updateLoadReports zk loadReports

  -- Load balancing service
  zkReportUpdateTimer zk lm

startWritingLoadReport :: ZHandle -> LoadManager -> IO ()
startWritingLoadReport zk lm@LoadManager{..} = do
  lr <- readMVar loadReport
  writeLoadReportToZooKeeper zk sName lr
  localReportUpdateTimer lm
--------------------------------------------------------------------------------

serverRanking :: IORef [ServerName]
serverRanking = unsafePerformIO $ newIORef []
{-# NOINLINE serverRanking #-}

writeLoadReportToZooKeeper :: ZHandle -> ServerName -> LoadReport -> IO ()
writeLoadReportToZooKeeper zk name lr = void $
  zooSet zk (serverLoadPath <> "/" <> name) (Just ((lazyByteStringToBytes . encode) lr)) Nothing

-- Update data on the server according to the data in zk and set watch for any new updates
updateLoadReports
  :: ZHandle -> MVar ServerLoadReports
  -> IO ()
updateLoadReports zk hmapM  = do
  Log.debug "Updating local load reports map and set watch on every server"
  names <- unStrVec . strsCompletionValues <$> zooGetChildren zk serverRootPath
  _names <- HM.keys <$> readMVar hmapM
  mapM_ (getAndWatchReport zk hmapM) (names \\ _names)

--------------------------------------------------------------------------------
-- Timer

localReportUpdateTimer :: LoadManager -> IO ()
localReportUpdateTimer LoadManager{..} = do
  timer <- newTimer
  void $ repeatedStart timer
    (do
      hmap <- readMVar loadReports
      updateLoadReport lastSysResUsage loadReport
      lr' <- readMVar loadReport
      Log.debug . Log.buildString $ "Scheduled local report update" <> show lr'
      when (abs (getPercentageUsage lr' -
            maybe 0 getPercentageUsage (HM.lookup sName hmap)) > 5)
        (putMVar ifUpdateZK ()))
    (sDelay 5)

zkReportUpdateTimer :: ZHandle -> LoadManager -> IO ()
zkReportUpdateTimer zk LoadManager {..} = do
  timer2 <- newTimer
  _ <- repeatedStart timer2
    (do
      putMVar ifUpdateZK ()
      Log.debug . Log.buildString $ "Scheduled zk local report store")
    (mDelay 5)
  void $ forkIO $ forever $ do
    _ <- takeMVar ifUpdateZK
    readMVar loadReport >>=
      writeLoadReportToZooKeeper zk sName

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

getAndWatchReport
  :: ZHandle
  -> MVar ServerLoadReports
  -> ServerName
  -> IO ()
getAndWatchReport zk hmapM name = do
  _ <- forkIO $ zooWatchGet zk
                (serverLoadPath <> "/" <> name)
                (watchFun hmapM name)
                (getReportCB hmapM name)
  Log.debug . Log.buildCBytes $ "Got data from zk and set watch for server " <> name

-- | The watch function set on every server load report in zk
watchFun
  :: MVar ServerLoadReports
  -> ServerName
  -> HsWatcherCtx -> IO ()
watchFun hmapM address HsWatcherCtx{..} = do
  Log.debug . Log.buildString $ "Watch triggered, updating map and ranking "
  case watcherCtxType of
    ZooDeleteEvent -> do
      modifyMVar_ hmapM (pure . HM.delete address)
      updateMapAndRanking hmapM
    ZooChangedEvent -> do
      getAndWatchReport watcherCtxZHandle hmapM  address
    _ -> return ()

-- | The call back function when we get load report info
getReportCB :: MVar ServerLoadReports -> ServerName ->
  DataCompletion -> IO ()
getReportCB hmapM  address DataCompletion{..} = do
  let lr = decode . bytesToLazyByteString <$> dataCompletionValue
  insertLoadReportsMap hmapM address lr
  updateMapAndRanking hmapM

updateMapAndRanking :: MVar ServerLoadReports -> IO ()
updateMapAndRanking hmapM = do
  hmap <- readMVar hmapM
  Log.debug . Log.buildString $
    "Local loads map updated: " <> show hmap
  let newRanking = generateRanking hmap
  oldRanking <- readIORef serverRanking
  when (oldRanking /= newRanking) $ do
    atomicWriteIORef serverRanking newRanking
    Log.info . Log.buildString $
      "Ranking updated, new ranking is " <> show newRanking

-- Update the load report map stored in the server local data
insertLoadReportsMap
  :: MVar ServerLoadReports
  -> ServerName
  -> Maybe (Maybe LoadReport)
  -> IO ()
insertLoadReportsMap mVar name (Just (Just x)) = do
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
