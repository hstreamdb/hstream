{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Monad
import           Data.Aeson              (Object, Value (..))
import qualified Data.HashMap.Lazy       as HM
import           Data.IORef
import qualified Data.List               as L
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word               (Word64)
import           System.IO.Unsafe        (unsafePerformIO)
import           Z.IO.Logger

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import qualified HStream.Utils.Aeson     as A

--------------------------------------------------------------------------------
-- unit: ms
posixTimeToMilliSeconds :: POSIXTime -> Word64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Word64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime
--------------------------------------------------------------------------------

mappingShard :: IO (Shard Object Word64, Node, Node)
mappingShard = do
  let mapper = Mapper (\row -> Right $ A.fromList [("aa", row A.! "a"), ("bb", row A.! "b")])
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec

      (builder_3, node_mapped) = addNode builder_2 subgraph_0 (MapSpec node_1 mapper)

      (builder_4, node_out) = addNode builder_3 subgraph_0  (OutputSpec node_mapped)
  let graph = buildGraph builder_4

  shard <- buildShard graph
  return (shard, node_1, node_out)

totalDataChangeCount_in :: IORef Word64
totalDataChangeCount_in = unsafePerformIO $ newIORef 0
{-# NOINLINE totalDataChangeCount_in #-}

totalDataChangeCount_out :: IORef Word64
totalDataChangeCount_out = unsafePerformIO $ newIORef 0
{-# NOINLINE totalDataChangeCount_out #-}

main :: IO ()
main = do
  newStdLogger (defaultLoggerConfig { loggerLevel = INFO }) >>= setDefaultLogger
  withDefaultLogger $ do
    (shard, inNode, outNode) <- mappingShard

    stop_m <- newEmptyMVar
    forkIO $ run shard stop_m

    startTime <- getCurrentTimestamp

    forkIO . forever $ do
      replicateM_ 1000 $ do
        ts <- getCurrentTimestamp
        let dc = DataChange
               { dcRow = A.fromList [("a", Number 2), ("b", Number 1)]
               , dcTimestamp = Timestamp ts []
               , dcDiff = 1
               }
        pushInput shard inNode dc
        atomicModifyIORef totalDataChangeCount_in (\x -> (x+1, ()))
      threadDelay 2000000
      n <- readIORef totalDataChangeCount_in
      print $ "[In ] ---> " <> show n

    forkIO . forever $ do
      threadDelay 100000
      flushInput shard inNode

    forkIO . forever $ do
      threadDelay 100000
      ts <- getCurrentTimestamp
      advanceInput shard inNode (Timestamp ts [])

    forkIO . forever $ popOutput shard outNode (threadDelay 1000000)
      (\dcb -> forM_ (dcbChanges dcb) $ \DataChange{..} -> do
          let (Number x) = dcRow A.! "bb"
              n = if dcDiff > 0 then (fromIntegral . floor $ x) * (fromIntegral dcDiff) else 0
          atomicModifyIORef totalDataChangeCount_out (\x -> (x+n, ()))
          print $ "[Out] ---> " <> show n
      )

    forever $ do
      threadDelay 1000000
      curTime <- getCurrentTimestamp
      curCount_in <- readIORef totalDataChangeCount_in
      curCount_out <- readIORef totalDataChangeCount_out
      let diffTimeSeconds = fromIntegral (curTime - startTime) / 1000
          changesPerSec_in = fromIntegral curCount_in / diffTimeSeconds
          changesPerSec_out = fromIntegral curCount_out / diffTimeSeconds
      print "============= changes per second =============="
      print $ "(In) "  <> show changesPerSec_in
      print $ "(Out) " <> show changesPerSec_out
