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

reducingShard :: IO (Shard Object Word64, (Node,Node), Node)
reducingShard = do
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, node_2) = addNode builder_2 subgraph_0 InputSpec
  let (builder_4, node_1') = addNode builder_3 subgraph_0 (IndexSpec node_1)
      (builder_5, node_2') = addNode builder_4 subgraph_0 (IndexSpec node_2)


  let joinCond = \row1 row2 -> (A.!) row1 "a" == (A.!) row2 "a"
      rowgen o1 o2 = A.fromList [ ("a", (A.!) o1 "a")
                                , ("b", (A.!) o1 "b")
                                , ("c", (A.!) o2 "c")
                                ]
      joinType = MergeJoinInner
      nullRowgen = \row -> A.fromList (map (\(k,v) -> (k,Null)) (A.toList row))
  let (builder_6, node_3) = addNode builder_5 subgraph_0 (JoinSpec node_1' node_2' joinType joinCond (Joiner rowgen) nullRowgen)

  let (builder_7, node_4) = addNode builder_6 subgraph_0 (OutputSpec node_3)

  let graph = buildGraph builder_7

  shard <- buildShard graph
  return (shard, (node_1,node_2), node_4)

totalDataChangeCount_1 :: IORef Word64
totalDataChangeCount_1 = unsafePerformIO $ newIORef 0
{-# NOINLINE totalDataChangeCount_1 #-}

totalDataChangeCount_2 :: IORef Word64
totalDataChangeCount_2 = unsafePerformIO $ newIORef 0
{-# NOINLINE totalDataChangeCount_2 #-}

totalDataChangeCount_out :: IORef Word64
totalDataChangeCount_out = unsafePerformIO $ newIORef 0
{-# NOINLINE totalDataChangeCount_out #-}

main :: IO ()
main = do
  (shard, (inNode_1,inNode_2), outNode) <- reducingShard

  stop_m <- newEmptyMVar
  forkIO $ run shard stop_m

  startTime <- getCurrentTimestamp

  -- Node_1 input
  forkIO . forever $ do
    replicateM_ 100 $ do
      ts <- getCurrentTimestamp
      let dc = DataChange
             { dcRow = A.fromList [("a", Number 1), ("b", Number 2)]
             , dcTimestamp = Timestamp ts []
             , dcDiff = 1
             }
      pushInput shard inNode_1 dc
      atomicModifyIORef totalDataChangeCount_1 (\x -> (x+1, ()))
    threadDelay 2000000
    n <- readIORef totalDataChangeCount_1
    print $ "[In_1] ---> " <> show n

  -- Node_2 input
  forkIO . forever $ do
    replicateM_ 100 $ do
      ts <- getCurrentTimestamp
      let dc = DataChange
             { dcRow = A.fromList [("a", Number 1), ("c", Number 3)]
             , dcTimestamp = Timestamp ts []
             , dcDiff = 1
             }
      pushInput shard inNode_2 dc
      atomicModifyIORef totalDataChangeCount_2 (\x -> (x+1, ()))
    threadDelay 2000000
    n <- readIORef totalDataChangeCount_2
    print $ "[In_2] ---> " <> show n

  forkIO . forever $ do
    threadDelay 100000
    flushInput shard inNode_1
    flushInput shard inNode_2

  forkIO . forever $ do
    threadDelay 100000
    ts <- getCurrentTimestamp
    advanceInput shard inNode_1 (Timestamp ts [])
    advanceInput shard inNode_2 (Timestamp ts [])

  forkIO . forever $ popOutput shard outNode (threadDelay 1000000)
    (\dcb -> forM_ (dcbChanges dcb) $ \DataChange{..} -> do
        let (Number x) = dcRow A.! "a"
            n = if dcDiff > 0 then (fromIntegral . floor $ x) * (fromIntegral dcDiff) else 0
        atomicModifyIORef totalDataChangeCount_out (\x -> (x+n, ()))
        print $ "[Out] ---> " <> show n
    )

  forever $ do
    threadDelay 1000000
    curTime <- getCurrentTimestamp
    curCount_1 <- readIORef totalDataChangeCount_1
    curCount_2 <- readIORef totalDataChangeCount_2
    curCount_out <- readIORef totalDataChangeCount_out
    let diffTimeSeconds = fromIntegral (curTime - startTime) / 1000
        changesPerSec_1 = fromIntegral curCount_1 / diffTimeSeconds
        changesPerSec_2 = fromIntegral curCount_2 / diffTimeSeconds
        changesPerSec_out = fromIntegral curCount_out / diffTimeSeconds
    print "============= changes per second =============="
    print $ "(In_1)" <> show changesPerSec_1
    print $ "(In_2)" <> show changesPerSec_2
    print $ "(Out) " <> show changesPerSec_out
