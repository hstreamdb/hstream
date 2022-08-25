{-# LANGUAGE OverloadedStrings #-}

import           Control.Concurrent
import           Control.Monad
import           Data.Aeson            (Object, Value (..))
import qualified Data.Aeson.KeyMap     as KM
import qualified Data.HashMap.Lazy     as HM
import           Data.IORef
import           Data.Maybe            (fromJust)
import           Data.Time
import           Data.Time.Clock.POSIX
import           Data.Word             (Word64)
import           System.IO.Unsafe      (unsafePerformIO)
import           Z.IO.Logger

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types

--------------------------------------------------------------------------------
-- unit: ms
posixTimeToMilliSeconds :: POSIXTime -> Word64
posixTimeToMilliSeconds =
  floor . (* 1000) . nominalDiffTimeToSeconds

-- return millisecond timestamp
getCurrentTimestamp :: IO Word64
getCurrentTimestamp = posixTimeToMilliSeconds <$> getPOSIXTime
--------------------------------------------------------------------------------

reducingShard :: IO (Shard Object Word64, Node, Node)
reducingShard = do
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec
  let mapper (Row o) = Row . KM.fromHashMap $ HM.adjust (\(Number n) -> (Number n)) "a" (KM.toHashMap o)
      keygen (Row o) = Row . KM.fromList $ [("b", fromJust $ KM.lookup "b" o)]
      reducer (Row value) (Row row) = let (Just (Number x)) = KM.lookup "a" row
                                      in Row . KM.fromHashMap $ HM.adjust (\(Number n) -> (Number (n+1))) "cnt" (KM.toHashMap value)
      initValue = Row $ KM.fromList [("cnt", Number 0)]

  let (builder_3, node_2) = addNode builder_2 subgraph_0 (IndexSpec node_1)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (ReduceSpec node_2 initValue keygen (Reducer reducer))
      (builder_5, node_4) = addNode builder_4 subgraph_0 (OutputSpec node_3)
  let graph = buildGraph builder_5

  shard <- buildShard graph
  return (shard, node_1, node_4)

totalDataChangeCount :: IORef Word64
totalDataChangeCount = unsafePerformIO $ newIORef 0
{-# NOINLINE totalDataChangeCount #-}

main :: IO ()
main = do
  newStdLogger (defaultLoggerConfig { loggerLevel = INFO }) >>= setDefaultLogger
  withDefaultLogger $ do
    (shard, inNode, outNode) <- reducingShard

    forkIO $ run shard

    startTime <- getCurrentTimestamp

    forkIO . forever $ do
      replicateM_ 10000 $ do
        ts <- getCurrentTimestamp
        let dc = DataChange
               { dcRow = Row $ KM.fromList [("a", Number 1), ("b", Number 2)]
               , dcTimestamp = Timestamp ts []
               , dcDiff = 1
               }
        pushInput shard inNode dc
      threadDelay 2000000

    forkIO . forever $ do
      threadDelay 100000
      flushInput shard inNode

    forkIO . forever $ do
      threadDelay 100000
      ts <- getCurrentTimestamp
      advanceInput shard inNode (Timestamp ts [])

    forkIO . forever $ popOutput shard outNode
      (\dcb -> do
          let lastChange = last $ dcbChanges dcb
          let (Just (Number x)) = KM.lookup "cnt" (unRow $ dcRow lastChange)
              n = fromIntegral (floor x)
          atomicModifyIORef totalDataChangeCount (\x -> (n, ()))
          print $ "---> " <> show n
      )

    forever $ do
      threadDelay 1000000
      curTime <- getCurrentTimestamp
      curCount <- readIORef totalDataChangeCount
      let diffTimeSeconds = fromIntegral (curTime - startTime) / 1000
          changesPerSec = fromIntegral curCount / diffTimeSeconds
      print "============= changes per second =============="
      print $ show changesPerSec
