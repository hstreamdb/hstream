{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Monad
import           Data.Aeson          (Value (..))
import           Data.Word

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import qualified HStream.Utils.Aeson as A

main :: IO ()
main = do
  let mapper_1 = Mapper (\row -> A.fromList [("a_new", row A.! "a")])
      mapper_2 = Mapper (\row -> A.fromList [("b_new", row A.! "b")])
      composer = Composer (\rows -> foldl1 (\acc x -> acc <> x) rows)
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec

      (builder_3, node_2) = addNode builder_2 subgraph_0 (MapSpec node_1 mapper_1)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (MapSpec node_1 mapper_2)

      (builder_5, node_4) = addNode builder_4 subgraph_0 (ComposeSpec [node_2,node_3] composer)

      (builder_6, node_5) = addNode builder_5 subgraph_0 (OutputSpec node_4)

  let graph = buildGraph builder_6

  shard <- buildShard graph
  forkIO $ run shard
  forkIO . forever $ popOutput shard node_5 (\dcb -> print $ "---> Output DataChangeBatch: " <> show dcb)

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

  threadDelay 10000000
