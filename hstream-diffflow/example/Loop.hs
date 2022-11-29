{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Monad
import           Data.Aeson              (Value (..))
import           Data.Word               (Word32)

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import qualified HStream.Utils.Aeson     as A

main :: IO ()
main = do

  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0

  let (builder_2, edges) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, edges_1) = addNode builder_2 subgraph_1 (TimestampPushSpec edges)
      (builder_4, reach_future) = addNode builder_3 subgraph_1 (TimestampIncSpec Nothing)
      (builder_5, reach_index) = addNode builder_4 subgraph_1 (IndexSpec reach_future)
      (builder_6, distinct_reach_index) = addNode builder_5 subgraph_1 (DistinctSpec reach_index)

  let mapper = Mapper (\row -> let v1 = (A.!) row "v1"
                                   v2 = (A.!) row "v2"
                                in A.fromList [("v1", v2), ("v2", v1)])
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
      joinCond = \row1 row2 -> let v1  = (A.!) row1 "v1"
                                   v1' = (A.!) row2 "v1"
                                in v1 == v1'
      joinType = MergeJoinInner
      nullRowgen = \row -> A.fromList (map (\(k,v) -> (k,Null)) (A.toList row))
  let (builder_9, joined) = addNode builder_8 subgraph_1 (JoinSpec distinct_reach_index swapped_edges_index joinType joinCond joiner nullRowgen)

  let mapper2 = Mapper (\row -> let v1 = (A.!) row "v1"
                                    v2 = (A.!) row "v2"
                                    v3 = (A.!) row "v3"
                                 in A.fromList [("v1", v3), ("v2", v2)]
                       ) -- drop middle
  let (builder_10, without_middle) = addNode builder_9 subgraph_1 (MapSpec joined mapper2)

  let (builder_11, reach) = addNode builder_10 subgraph_1 (UnionSpec edges_1 without_middle)

  let builder_12 = connectLoop builder_11 reach reach_future

  let (builder_13, reach_pop) = addNode builder_12 subgraph_0 (TimestampPopSpec distinct_reach_index)
      (builder_14, reach_out) = addNode builder_13 subgraph_0 (OutputSpec reach_pop)

  let reducer = Reducer (\acc row -> let (String reduced) = (A.!) acc "reduced"
                                         (String v2)      = (A.!) row "v2"
                                      in A.fromList [("reduced", String (reduced <> v2))]
                        ) -- acc ++ row[1]
      initValue = A.fromList [("reduced", String "")]
      keygen = \row -> let v1 = (A.!) row "v1" in A.fromList [("v1", v1)]
  let (builder_15, reach_summary) = addNode builder_14 subgraph_1 (ReduceSpec distinct_reach_index initValue keygen reducer)
      (builder_16, reach_summary_out) = addNode builder_15 subgraph_1 (OutputSpec reach_summary)

  ----

  let graph = buildGraph builder_16
  shard <- buildShard graph
  stop_m <- newEmptyMVar
  forkIO $ run shard stop_m

  forkIO . forever $ popOutput shard reach_out (threadDelay 1000000) (\dcb -> print $ "[reach_out        ] ---> Output DataChangeBatch: " <> show dcb)
  forkIO . forever $ popOutput shard reach_summary_out (threadDelay 1000000) (\dcb -> print $ "[reach_summary_out] ---> Output DataChangeBatch: " <> show dcb)

  threadDelay 1000000

  ----

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "a"), ("v2", String "b")]) (Timestamp (0 :: Word32) []) 1 0)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "b"), ("v2", String "c")]) (Timestamp (0 :: Word32) []) 1 1)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "b"), ("v2", String "d")]) (Timestamp (0 :: Word32) []) 1 2)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "c"), ("v2", String "a")]) (Timestamp (0 :: Word32) []) 1 3)

  pushInput shard edges
    (DataChange (A.fromList [("v1", String "b"), ("v2", String "c")]) (Timestamp (1 :: Word32) []) (-1) 4)

  flushInput shard edges

  advanceInput shard edges (Timestamp (1 :: Word32) [])

  threadDelay 5000000
  advanceInput shard edges (Timestamp (2 :: Word32) [])

  threadDelay $ 100 * 1000 * 1000
