{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module DiffFlow.LoopSpec where

import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Monad
import           Data.Aeson              (Object, Value (..))
import           Data.Hashable           (Hashable)
import qualified Data.List               as L
import           Data.MultiSet           (MultiSet)
import qualified Data.MultiSet           as MultiSet
import           Data.Set                (Set)
import qualified Data.Set                as Set
import           Data.Word
import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import           Test.Hspec

import qualified HStream.Utils.Aeson     as A

shardBody :: MVar () -> MVar [DataChangeBatch Object Word32] -> MVar [DataChangeBatch Object Word32] -> IO ()
shardBody isDone reachOut_m reachSummaryOut_m = do

  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0

  let (builder_2, edges) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, edges_1) = addNode builder_2 subgraph_1 (TimestampPushSpec edges)
      (builder_4, reach_future) = addNode builder_3 subgraph_1 (TimestampIncSpec Nothing)
      (builder_5, reach_index) = addNode builder_4 subgraph_1 (IndexSpec reach_future)
      (builder_6, distinct_reach_index) = addNode builder_5 subgraph_1 (DistinctSpec reach_index)

  let mapper = Mapper (\row -> let v1 = (A.!) row "v1"
                                   v2 = (A.!) row "v2"
                                in Right $ A.fromList [("v1", v2), ("v2", v1)])
  let (builder_7, swapped_edges) = addNode builder_6 subgraph_1 (MapSpec edges_1 mapper)
      (builder_8, swapped_edges_index) = addNode builder_7 subgraph_1 (IndexSpec swapped_edges)

  let joiner = Joiner (\row1 row2 -> let v1  = (A.!) row1 "v1"
                                         v2  = (A.!) row1 "v2"
                                         v1' = (A.!) row2 "v1"
                                         v2' = (A.!) row2 "v2"
                                      in A.fromList [ ("v1", v1')
                                                    , ("v2", v2)
                                                    , ("v3", v2')
                                                    ]
                      )
      joinCond = \row1 row2 ->
                   A.fromList [("v1", (A.!) row1 "v1")] == A.fromList [("v1", (A.!) row2 "v1")]
      joinType = MergeJoinInner
      nullRowgen = id
  let (builder_9, joined) = addNode builder_8 subgraph_1 (JoinSpec distinct_reach_index swapped_edges_index joinType joinCond joiner nullRowgen)

  let mapper2 = Mapper (\row -> let v1 = (A.!) row "v1"
                                    v2 = (A.!) row "v2"
                                    v3 = (A.!) row "v3"
                                 in Right $ A.fromList [("v1", v3), ("v2", v2)]
                       ) -- drop middle
  let (builder_10, without_middle) = addNode builder_9 subgraph_1 (MapSpec joined mapper2)

  let (builder_11, reach) = addNode builder_10 subgraph_1 (UnionSpec edges_1 without_middle)

  let builder_12 = connectLoop builder_11 reach reach_future

  let (builder_13, reach_pop) = addNode builder_12 subgraph_0 (TimestampPopSpec distinct_reach_index)
      (builder_14, reach_out) = addNode builder_13 subgraph_0 (OutputSpec reach_pop)

  let reducer = Reducer (\acc row -> let (String reduced) = (A.!) acc "reduced"
                                         (String v2)      = (A.!) row "v2"
                                      in Right $ A.fromList [("reduced", String (reduced <> v2))]
                        ) -- acc ++ row[1]
      initValue = A.fromList [("reduced", String "")]
      keygen = \row -> let v1 = (A.!) row "v1" in A.fromList [("v1", v1)]
  let (builder_15, reach_summary) = addNode builder_14 subgraph_1 (ReduceSpec distinct_reach_index initValue keygen reducer)
      (builder_16, reach_summary_out) = addNode builder_15 subgraph_1 (OutputSpec reach_summary)

  -- Run the shard

  let graph = buildGraph builder_16
  shard <- buildShard graph

  stop_m <- newEmptyMVar
  forkIO $ run shard stop_m

  forkIO . forever $ popOutput shard reach_out         (threadDelay 1000000) (\dcb -> modifyMVar_ reachOut_m        (\xs -> return $ xs ++ [dcb]))
  forkIO . forever $ popOutput shard reach_summary_out (threadDelay 1000000) (\dcb -> modifyMVar_ reachSummaryOut_m (\xs -> return $ xs ++ [dcb]))

  threadDelay 1000000

  -- Step 1
  putMVar reachOut_m []
  putMVar reachSummaryOut_m []

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "a"), ("v2", String "b")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "b"), ("v2", String "c")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "b"), ("v2", String "d")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "c"), ("v2", String "a")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "b"), ("v2", String "c")]) (Timestamp (1 :: Word32) []) (-1))

  flushInput shard edges
  advanceInput shard edges (Timestamp (1 :: Word32) [])

  threadDelay $ 5 * 1000 * 1000
  putMVar isDone ()

  -- Step 2
  putMVar reachOut_m []
  putMVar reachSummaryOut_m []

  advanceInput shard edges (Timestamp (2 :: Word32) [])

  threadDelay $ 5 * 1000 * 1000
  putMVar isDone ()


spec :: Spec
spec = describe "LoopSpec" $ do
  reach_m        <- runIO newEmptyMVar
  reachSummary_m <- runIO newEmptyMVar
  isDone         <- runIO newEmptyMVar
  runIO . forkIO $ shardBody isDone reach_m reachSummary_m
  checkStep1 isDone reach_m reachSummary_m
  checkStep2 isDone reach_m reachSummary_m

checkStep1 :: MVar () -> MVar [DataChangeBatch Object Word32] -> MVar [DataChangeBatch Object Word32] -> Spec
checkStep1 isDone reach_m reachSummary_m = describe "check reach out" $ do
  it "reach out step 1 (advance to ts=1)" $ do
    readMVar isDone
    ((takeMVar reach_m) >>= (return . (L.map dcbChanges))) `shouldReturn` dcbs1
  it "reach summary step 1 (advance to ts=1)" $ do
    takeMVar isDone
    ((takeMVar reachSummary_m) >>= (return .  (L.map dcbChanges))) `shouldReturn` dcbs2

checkStep2 :: MVar () -> MVar [DataChangeBatch Object Word32] -> MVar [DataChangeBatch Object Word32] -> Spec
checkStep2 isDone reach_m reachSummary_m = describe "check reach summary out" $ do
  it "reach out step 2 (advance to ts=2)" $ do
    readMVar isDone
    ((takeMVar reach_m) >>= (return . (L.map dcbChanges))) `shouldReturn` dcbs3
  it "reach summary step 2 (advance to ts=2)" $ do
    takeMVar isDone
    ((takeMVar reachSummary_m) >>= (return .  (L.map dcbChanges))) `shouldReturn` dcbs4

-- Note: (Ord Object) of aeson<2 and aeson>2 has different implementations
--------------------------------------------------------------------------------
#if MIN_VERSION_aeson(2,0,0)
--------------------------------------------------------------------------------

dcbs1 :: [[DataChange Object Word32]]
dcbs1 = [ [DataChange (A.fromList [("v1", "c"), ("v2", "a")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "b")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "c")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "d")]) (Timestamp 0 []) 1]

        , [DataChange (A.fromList [("v1", "a"), ("v2", "d")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "a")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "c")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "b")]) (Timestamp 0 []) 1]

        , [DataChange (A.fromList [("v1", "a"), ("v2", "a")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "c")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "d")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "b")]) (Timestamp 0 []) 1]
        ]

dcbs2 :: [[DataChange Object Word32]]
dcbs2 = [ [DataChange (A.fromList [("v1", "c"), ("reduced", "a" )]) (Timestamp 0 [1]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "b" )]) (Timestamp 0 [1]) 1]
        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cd")]) (Timestamp 0 [1]) 1]

        , [DataChange (A.fromList [("v1", "c"), ("reduced", "a"  )]) (Timestamp 0 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "c"), ("reduced", "ab" )]) (Timestamp 0 [2]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "b"  )]) (Timestamp 0 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bcd")]) (Timestamp 0 [2]) 1]
        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cd" )]) (Timestamp 0 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cda")]) (Timestamp 0 [2]) 1]

        , [DataChange (A.fromList [("v1", "c"), ("reduced", "ab"  )]) (Timestamp 0 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "c"), ("reduced", "abcd")]) (Timestamp 0 [3]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "bcd" )]) (Timestamp 0 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bcda")]) (Timestamp 0 [3]) 1]
        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cda" )]) (Timestamp 0 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cdab")]) (Timestamp 0 [3]) 1]
        ]

dcbs3 :: [[DataChange Object Word32]]
dcbs3 = [ [DataChange (A.fromList [("v1", "b"), ("v2", "c")]) (Timestamp 1 []) (-1)]

        , [DataChange (A.fromList [("v1", "b"), ("v2", "a")]) (Timestamp 1 []) (-1)]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "c")]) (Timestamp 1 []) (-1)]

        , [DataChange (A.fromList [("v1", "a"), ("v2", "a")]) (Timestamp 1 []) (-1)]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "c")]) (Timestamp 1 []) (-1)]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "b")]) (Timestamp 1 []) (-1)]
        ]

dcbs4 :: [[DataChange Object Word32]]
dcbs4 = [ [DataChange (A.fromList [("v1", "b"), ("reduced", "cd")]) (Timestamp 1 [1]) (-1)
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "d" )]) (Timestamp 1 [1]) 1]

        , [DataChange (A.fromList [("v1", "a"), ("reduced", "bcd")]) (Timestamp 1 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bd" )]) (Timestamp 1 [2]) 1]
        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cd" )]) (Timestamp 1 [2]) 1
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cda")]) (Timestamp 1 [2]) (-1)]

        , [DataChange (A.fromList [("v1", "c"), ("reduced", "abcd")]) (Timestamp 1 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "c"), ("reduced", "abd" )]) (Timestamp 1 [3]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "bcd" )]) (Timestamp 1 [3]) 1
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bcda")]) (Timestamp 1 [3]) (-1)]
        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cda" )]) (Timestamp 1 [3]) 1
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cdab")]) (Timestamp 1 [3]) (-1)]
        ]

--------------------------------------------------------------------------------
#else
--------------------------------------------------------------------------------

dcbs1 :: [[DataChange Object Word32]]
dcbs1 = [ [DataChange (A.fromList [("v1", "b"), ("v2", "c")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "a")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "d")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "b")]) (Timestamp 0 []) 1]

        , [DataChange (A.fromList [("v1", "a"), ("v2", "c")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "b")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "d")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "a")]) (Timestamp 0 []) 1]

        , [DataChange (A.fromList [("v1", "b"), ("v2", "b")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "d")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "c")]) (Timestamp 0 []) 1]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "a")]) (Timestamp 0 []) 1]
        ]

dcbs2 :: [[DataChange Object Word32]]
dcbs2 = [ [DataChange (A.fromList [("v1", "b"), ("reduced", "cd")]) (Timestamp 0 [1]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "b" )]) (Timestamp 0 [1]) 1]
        , [DataChange (A.fromList [("v1", "c"), ("reduced", "a" )]) (Timestamp 0 [1]) 1]

        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cd" )]) (Timestamp 0 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cda")]) (Timestamp 0 [2]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "b"  )]) (Timestamp 0 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bcd")]) (Timestamp 0 [2]) 1]
        , [DataChange (A.fromList [("v1", "c"), ("reduced", "a"  )]) (Timestamp 0 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "c"), ("reduced", "ab" )]) (Timestamp 0 [2]) 1]

        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cda" )]) (Timestamp 0 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cdab")]) (Timestamp 0 [3]) 1]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "bcd" )]) (Timestamp 0 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bcda")]) (Timestamp 0 [3]) 1]
        , [DataChange (A.fromList [("v1", "c"), ("reduced", "ab"  )]) (Timestamp 0 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "c"), ("reduced", "abcd")]) (Timestamp 0 [3]) 1]
        ]

dcbs3 :: [[DataChange Object Word32]]
dcbs3 = [ [DataChange (A.fromList [("v1", "b"), ("v2", "c")]) (Timestamp 1 []) (-1)]

        , [DataChange (A.fromList [("v1", "a"), ("v2", "c")]) (Timestamp 1 []) (-1)]
        , [DataChange (A.fromList [("v1", "b"), ("v2", "a")]) (Timestamp 1 []) (-1)]

        , [DataChange (A.fromList [("v1", "b"), ("v2", "b")]) (Timestamp 1 []) (-1)]
        , [DataChange (A.fromList [("v1", "c"), ("v2", "c")]) (Timestamp 1 []) (-1)]
        , [DataChange (A.fromList [("v1", "a"), ("v2", "a")]) (Timestamp 1 []) (-1)]
        ]

dcbs4 :: [[DataChange Object Word32]]
dcbs4 = [ [DataChange (A.fromList [("v1", "b"), ("reduced", "cd")]) (Timestamp 1 [1]) (-1)
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "d" )]) (Timestamp 1 [1]) 1]

        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cd" )]) (Timestamp 1 [2]) 1
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cda")]) (Timestamp 1 [2]) (-1)]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "bcd")]) (Timestamp 1 [2]) (-1)
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bd" )]) (Timestamp 1 [2]) 1]

        , [DataChange (A.fromList [("v1", "b"), ("reduced", "cda" )]) (Timestamp 1 [3]) 1
        ,  DataChange (A.fromList [("v1", "b"), ("reduced", "cdab")]) (Timestamp 1 [3]) (-1)]
        , [DataChange (A.fromList [("v1", "a"), ("reduced", "bcd" )]) (Timestamp 1 [3]) 1
        ,  DataChange (A.fromList [("v1", "a"), ("reduced", "bcda")]) (Timestamp 1 [3]) (-1)]
        , [DataChange (A.fromList [("v1", "c"), ("reduced", "abcd")]) (Timestamp 1 [3]) (-1)
        ,  DataChange (A.fromList [("v1", "c"), ("reduced", "abd" )]) (Timestamp 1 [3]) 1]
        ]

--------------------------------------------------------------------------------
#endif
--------------------------------------------------------------------------------
