{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent
import           Control.Concurrent.MVar
import           Control.Monad
import           Data.Aeson              (Value (..))
import qualified Data.List               as L
import           Data.Word

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import qualified HStream.Utils.Aeson     as A

main :: IO ()
main = do
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec
  let mapper o = A.adjust (\(Number n) -> Number (n+1)) "a" o
      keygen o = A.fromList [("b", (A.!) o "b")]
      reducer value row = let (Number x) = (A.!) row "a"
                           in A.adjust (\(Number n) -> Number (n+x)) "sum" value
      initValue = A.fromList [("sum", Number 0)]

  let (builder_3, node_2) = addNode builder_2 subgraph_0 (IndexSpec node_1)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (ReduceSpec node_2 initValue keygen (Reducer reducer))
      (builder_5, node_4) = addNode builder_4 subgraph_0 (OutputSpec node_3)
  let graph = buildGraph builder_5

  shard <- buildShard graph
  stop_m <- newEmptyMVar
  forkIO $ run shard stop_m
  forkIO . forever $ popOutput shard node_4 (\dcb -> print $ "---> Output DataChangeBatch: " <> show dcb)

  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (0 :: Word32) []) 1 0)
  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (0 :: Word32) []) 1 1)
  flushInput shard node_1

  advanceInput shard node_1 (Timestamp 1 [])

  pushInput shard node_1
    (DataChange (A.fromList [("a", Number 1), ("b", Number 2)]) (Timestamp (2 :: Word32) []) 1 2)

  advanceInput shard node_1 (Timestamp 4 [])

  threadDelay 10000000
