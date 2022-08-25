{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

import           Criterion.Main
import           Data.Aeson
import qualified Data.Aeson.KeyMap as KM
import qualified Data.HashMap.Lazy as HM
import           Data.Maybe        (fromJust)
import           Data.Word         (Word32)

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types

reducingGraph :: Graph Object
reducingGraph =
  let subgraph_0 = Subgraph 0
      (builder_1, subgraph_1) = addSubgraph emptyGraphBuilder subgraph_0 in
  let (builder_2, node_1) = addNode builder_1 subgraph_0 InputSpec in
  let mapper (Row o) = Row . KM.fromHashMap $ HM.adjust (\(Number n) -> (Number (n+1))) "a" (KM.toHashMap o)
      keygen (Row o) = Row . KM.fromList $ [("b", fromJust $ KM.lookup "b" o)]
      reducer (Row value) (Row row) = let (Just (Number x)) = KM.lookup "a" row
                           in Row . KM.fromHashMap $ HM.adjust (\(Number n) -> (Number (n+x))) "sum" (KM.toHashMap value)
      initValue = Row $ KM.fromList [("sum", Number 0)] in

  let (builder_3, node_2) = addNode builder_2 subgraph_0 (IndexSpec node_1)
      (builder_4, node_3) = addNode builder_3 subgraph_0 (ReduceSpec node_2 initValue keygen (Reducer reducer))
      (builder_5, node_4) = addNode builder_4 subgraph_0 (OutputSpec node_3) in
    buildGraph builder_5


main = defaultMain [
  bench "buildShard: reducing shard" $ nfIO (buildShard @Object @Word32 reducingGraph)
  ]
