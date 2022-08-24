{-# LANGUAGE OverloadedStrings #-}

module Main where

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types

import           Control.Concurrent
import           Control.Monad
import           Data.Aeson         (Value (..))
import qualified Data.HashMap.Lazy  as HM
import           Data.Word

main :: IO ()
main = do
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec
      (builder_3, node_2) = addNode builder_2 subgraph_0 (IndexSpec node_1)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (DistinctSpec node_2)
      (builder_5, node_4) = addNode builder_4 subgraph_0 (OutputSpec node_3)

  let graph = buildGraph builder_5

  shard <- buildShard graph
  forkIO $ run shard
  forkIO . forever $ popOutput shard node_4 (\dcb -> print $ "---> Output DataChangeBatch: " <> show dcb)

  pushInput shard node_1
    (DataChange (HM.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (1 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (HM.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (2 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (HM.fromList [("b", Number 1), ("c", Number 2)]) (Timestamp (2 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (HM.fromList [("c", Number 1), ("d", Number 2)]) (Timestamp (2 :: Word32) []) 1)
  flushInput shard node_1
  advanceInput shard node_1 (Timestamp 3 [])

  threadDelay 1000000

  pushInput shard node_1
    (DataChange (HM.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (4 :: Word32) []) 1)
  pushInput shard node_1
    (DataChange (HM.fromList [("c", Number 1), ("d", Number 2)]) (Timestamp (5 :: Word32) []) 1)
  advanceInput shard node_1 (Timestamp 6 [])

  threadDelay 10000000
