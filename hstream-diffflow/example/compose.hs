{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Monad
import           Data.Aeson          (Object, Value (..))
import           Data.Word

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import qualified HStream.Utils.Aeson as A

import           Debug.Trace

constantKeygen = \_ -> A.fromList [("key", String "__constant_key__")]

joinMany :: GraphBuilder Object
         -> Subgraph
         -> [Node]
         -> (GraphBuilder Object, Node)
joinMany startBuilder subgraph ins =
  foldl (\(oldBuilder, oldOut) thisNode ->
           let joiner = Joiner (<>)
            in addNode oldBuilder subgraph (JoinSpec oldOut thisNode constantKeygen constantKeygen joiner)
        ) (startBuilder, head ins) (tail ins)

main :: IO ()
main = do
  let mapper_1 = Mapper (\row -> A.fromList [("a", row A.! "a")])
      mapper_2 = Mapper (\row -> A.fromList [("b", row A.! "b")])
      composer = Composer (\rows -> foldl1 (\acc x -> acc <> x) rows)
      reducer = Reducer (\acc row -> let (Number x) = row A.! "b"
                                         [(_, Number acc_n)] = A.toList acc
                                      in A.fromList [("sum", Number (x+acc_n))]
                        )
      initValue = A.fromList [("sum", Number 0)]
      keygen = \row -> A.fromList [("a", row A.! "a")]
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec

      (builder_3, node_map_1) = addNode builder_2 subgraph_0 (MapSpec node_1 mapper_1)

      (builder_4, node_map_2) = addNode builder_3 subgraph_0 (MapSpec node_1 mapper_2)
      (builder_5, node_map_3) = addNode builder_4 subgraph_0 (MapSpec node_1 mapper_1)

      (builder_6, node_pre_index) = addNode builder_5 subgraph_0 (ComposeSpec [node_map_2, node_map_3] composer)
      (builder_7, node_indexed) = addNode builder_6 subgraph_0 (IndexSpec node_pre_index)
      (builder_8, node_reduced) = addNode builder_7 subgraph_0 (ReduceSpec node_indexed initValue keygen reducer)
      (builder_9, node_composed) = addNode builder_8 subgraph_0 (ComposeSpec [node_map_1, node_reduced] composer)

      (builder_10, node_out) = addNode builder_9 subgraph_0  (OutputSpec node_composed)

  let graph = buildGraph builder_10

  shard <- buildShard graph
  forkIO $ run shard
  forkIO . forever $ popOutput shard node_out (\dcb -> print $ "---> Output DataChangeBatch: " <> show dcb)

  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (1 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 3), ("b", Number 4)]) (Timestamp (2 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 5), ("b", Number 6)]) (Timestamp (2 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 7), ("b", Number 8)]) (Timestamp (2 :: Word32) []) 1)
  flushInput shard node_1
  advanceInput shard node_1 (Timestamp 3 [])

  threadDelay 1000000

  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 9), ("b", Number 10)]) (Timestamp (4 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 11), ("b", Number 12)]) (Timestamp (5 :: Word32) []) 1)
  advanceInput shard node_1 (Timestamp 6 [])

  threadDelay 100000000
