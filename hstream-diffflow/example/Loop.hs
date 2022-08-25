{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Monad
import           Data.Aeson         (Value (..))
import qualified Data.Aeson.KeyMap  as KM
import qualified Data.Map           as Map
import           Data.Word          (Word32)

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types

main :: IO ()
main = do

  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0

  let (builder_2, edges) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, edges_1) = addNode builder_2 subgraph_1 (TimestampPushSpec edges)
      (builder_4, reach_future) = addNode builder_3 subgraph_1 (TimestampIncSpec Nothing)
      (builder_5, reach_index) = addNode builder_4 subgraph_1 (IndexSpec reach_future)
      (builder_6, distinct_reach_index) = addNode builder_5 subgraph_1 (DistinctSpec reach_index)

  let mapper = Mapper (\(Row row) -> let (Just v1) = KM.lookup "v1" row
                                         (Just v2) = KM.lookup "v2" row
                                in Row . KM.fromMap $ Map.fromList [("v1", v2), ("v2", v1)])
  let (builder_7, swapped_edges) = addNode builder_6 subgraph_1 (MapSpec edges_1 mapper)
      (builder_8, swapped_edges_index) = addNode builder_7 subgraph_1 (IndexSpec swapped_edges)

  let joiner = Joiner (\(Row row1) (Row row2) -> let (Just v1 ) = KM.lookup "v1" row1
                                                     (Just v2 ) = KM.lookup "v2" row1
                                                     (Just v1') = KM.lookup "v1" row2
                                                     (Just v2') = KM.lookup "v2" row2
                                      in Row . KM.fromMap $ Map.fromList [ ("v1", v1')
                                                                         , ("v2", v2)
                                                                         , ("v3", v2')
                                                                         ]
                      )
      keygen1 = \(Row row) -> let (Just v1) = KM.lookup "v1" row in Row . KM.fromMap $ Map.fromList [("v1", v1)]
      keygen2 = \(Row row) -> let (Just v1) = KM.lookup "v1" row in Row . KM.fromMap $ Map.fromList [("v1", v1)]
  let (builder_9, joined) = addNode builder_8 subgraph_1 (JoinSpec distinct_reach_index swapped_edges_index keygen1 keygen2 joiner)

  let mapper2 = Mapper (\(Row row) -> let (Just v1) = KM.lookup "v1" row
                                          (Just v2) = KM.lookup "v2" row
                                          (Just v3) = KM.lookup "v3" row
                                 in Row . KM.fromMap $ Map.fromList [("v1", v3), ("v2", v2)]
                       ) -- drop middle
  let (builder_10, without_middle) = addNode builder_9 subgraph_1 (MapSpec joined mapper2)

  let (builder_11, reach) = addNode builder_10 subgraph_1 (UnionSpec edges_1 without_middle)

  let builder_12 = connectLoop builder_11 reach reach_future

  let (builder_13, reach_pop) = addNode builder_12 subgraph_0 (TimestampPopSpec distinct_reach_index)
      (builder_14, reach_out) = addNode builder_13 subgraph_0 (OutputSpec reach_pop)

  let reducer = Reducer (\(Row acc) (Row row) -> let (Just (String reduced)) = KM.lookup "reduced" acc
                                                     (Just (String v2)     ) = KM.lookup "v2" row
                                                 in Row . KM.fromMap $ Map.fromList [("reduced", String (reduced <> v2))]
                        ) -- acc ++ row[1]
      initValue = Row . KM.fromMap $ Map.fromList [("reduced", String "")]
      keygen = \(Row row) -> let (Just v1) = KM.lookup "v1" row in Row . KM.fromMap $ Map.fromList [("v1", v1)]
  let (builder_15, reach_summary) = addNode builder_14 subgraph_1 (ReduceSpec distinct_reach_index initValue keygen reducer)
      (builder_16, reach_summary_out) = addNode builder_15 subgraph_1 (OutputSpec reach_summary)

  ----

  let graph = buildGraph builder_16
  shard <- buildShard graph
  forkIO $ run shard

  forkIO . forever $ popOutput shard reach_out (\dcb -> print $ "[reach_out        ] ---> Output DataChangeBatch: " <> show dcb)
  forkIO . forever $ popOutput shard reach_summary_out (\dcb -> print $ "[reach_summary_out] ---> Output DataChangeBatch: " <> show dcb)

  threadDelay 1000000

  ----

  pushInput shard edges
    (DataChange (Row . KM.fromMap $ Map.fromList [("v1", String "a"), ("v2", String "b")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (Row . KM.fromMap $ Map.fromList [("v1", String "b"), ("v2", String "c")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (Row . KM.fromMap $ Map.fromList [("v1", String "b"), ("v2", String "d")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (Row . KM.fromMap $ Map.fromList [("v1", String "c"), ("v2", String "a")]) (Timestamp (0 :: Word32) []) 1)

  pushInput shard edges
    (DataChange (Row . KM.fromMap $ Map.fromList [("v1", String "b"), ("v2", String "c")]) (Timestamp (1 :: Word32) []) (-1))

  flushInput shard edges

  advanceInput shard edges (Timestamp (1 :: Word32) [])

  threadDelay 5000000
  advanceInput shard edges (Timestamp (2 :: Word32) [])

  threadDelay $ 100 * 1000 * 1000
