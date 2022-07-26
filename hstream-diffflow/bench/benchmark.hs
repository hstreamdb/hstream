{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

import           Criterion.Main
import           Data.Aeson
import qualified Data.HashMap.Lazy as HM
import           Data.Word         (Word32)

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types

reducingGraph :: Graph
reducingGraph =
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0 in
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec in
  let mapper o = HM.adjust (\(Number n) -> (Number (n+1))) "a" o
      keygen o = HM.fromList $ [("b", (HM.!) o "b")]
      reducer value row = let (Number x) = (HM.!) row "a"
                           in HM.adjust (\(Number n) -> (Number (n+x))) "sum" value
      initValue = HM.fromList [("sum", Number 0)] in

  let (builder_3, node_2) = addNode builder_2 subgraph_0 (IndexSpec node_1)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (ReduceSpec node_2 initValue keygen (Reducer reducer))
      (builder_5, node_4) = addNode builder_4 subgraph_0 (OutputSpec node_3) in
    buildGraph builder_5


main = defaultMain [
  bench "buildShard: reducing shard" $ nfIO (buildShard @Word32 reducingGraph)
  ]
