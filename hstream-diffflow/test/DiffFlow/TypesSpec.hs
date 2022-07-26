{-# LANGUAGE OverloadedStrings #-}

module DiffFlow.TypesSpec where

import           Data.Aeson          (Value (..))
import           Data.Hashable       (Hashable)
import qualified Data.HashMap.Strict as HM
import qualified Data.List           as L
import           Data.MultiSet       (MultiSet)
import qualified Data.MultiSet       as MultiSet
import           Data.Set            (Set)
import qualified Data.Set            as Set
import           DiffFlow.Types
import           Test.Hspec

spec :: Spec
spec = describe "TypesSpec" $ do
  timestampOrder
  timestampLub
  timestampsWithFrontier
  dataChangeBatch
  frontierMove
  frontierOrder
  index

timestampOrderChecker :: (Ord a)
                      => Timestamp a
                      -> Timestamp a
                      -> PartialOrdering
                      -> Bool
timestampOrderChecker ts1 ts2 expectedCausalOrd =
  causalOrd == expectedCausalOrd &&
  reversedCausalOrd == revCausalOrd expectedCausalOrd &&
  (causalOrd == PNONE || causalOrdtoOrd causalOrd == ts1 `compare` ts2) &&
  ts1 <.= leastUpperBound ts1 ts2 &&
  ts2 <.= leastUpperBound ts1 ts2
  where
    causalOrd = ts1 `causalCompare` ts2
    reversedCausalOrd = ts2 `causalCompare` ts1
    revCausalOrd :: PartialOrdering -> PartialOrdering
    revCausalOrd PNONE = PNONE
    revCausalOrd PEQ   = PEQ
    revCausalOrd PLT   = PGT
    revCausalOrd PGT   = PLT
    causalOrdtoOrd :: PartialOrdering -> Ordering
    causalOrdtoOrd PEQ   = EQ
    causalOrdtoOrd PLT   = LT
    causalOrdtoOrd PGT   = GT
    causalOrdtoOrd PNONE = error "unreachable"

updateTimestampsWithFrontierChecker :: (Ord a, Show a)
                                    => MultiSet (Timestamp a) -- initial timestamps
                                    -> (Timestamp a, Int)     -- update
                                    -> Frontier a             -- expected frontier
                                    -> [FrontierChange a]     -- expected frontier changes
                                    -> Bool
updateTimestampsWithFrontierChecker tss (ts,diff) expectedFrontier expectedChanges =
  ((tsfFrontier $ fst actualTsf) == expectedFrontier) && (snd actualTsf == expectedChanges)
  where
    emptyTsf = TimestampsWithFrontier MultiSet.empty Set.empty
    initTsf  = MultiSet.foldOccur (\x n acc -> acc ->> (x,n)) emptyTsf tss
    actualTsf = updateTimestampsWithFrontier initTsf ts diff


mkDataChangeBatchChecker :: (Hashable a, Ord a, Show a)
                         => [DataChange a] -- input data changes
                         -> [DataChange a] -- expected data changes
                         -> Bool
mkDataChangeBatchChecker changes expectedChanges =
  dcbChanges dataChangeBatch == expectedChanges
  where dataChangeBatch = mkDataChangeBatch changes

addChangeBatchToIndexChecker :: (Hashable a, Ord a, Show a)
                             => [DataChangeBatch a]
                             -> Index a
                             -> Bool
addChangeBatchToIndexChecker batches expectedIndex = expectedIndex == actualIndex
  where actualIndex =
          L.foldl addChangeBatchToIndex (Index []) batches

timestampOrder :: Spec
timestampOrder = describe "TimestampOrder" $ do
  it "timestamp should obey CausalOrder; CausalOrder's reverse property; CausalOrder should be compatible with Ord; ts <.= lub" $ do
    timestampOrderChecker
      (Timestamp 0 [])
      (Timestamp 0 [])
      PEQ
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 0 [])
      (Timestamp 1 [])
      PLT
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 1 [])
      (Timestamp 0 [])
      PGT
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 0 [0])
      (Timestamp 0 [0])
      PEQ
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 0 [0])
      (Timestamp 1 [0])
      PLT
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 0 [0])
      (Timestamp 0 [1])
      PLT
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 0 [0])
      (Timestamp 1 [1])
      PLT
      `shouldBe` True
    timestampOrderChecker
      (Timestamp 1 [0])
      (Timestamp 0 [1])
      PNONE
      `shouldBe` True

timestampLub :: Spec
timestampLub = describe "TimestampLub" $ do
  it "Timestamp least upper bound" $ do
    Timestamp 0 [] `leastUpperBound` Timestamp 0 [] `shouldBe` Timestamp 0 []
    Timestamp 0 [] `leastUpperBound` Timestamp 1 [] `shouldBe` Timestamp 1 []
    Timestamp 1 [] `leastUpperBound` Timestamp 0 [] `shouldBe` Timestamp 1 []
    Timestamp 0 [0] `leastUpperBound` Timestamp 0 [0] `shouldBe` Timestamp 0 [0]
    Timestamp 0 [0] `leastUpperBound` Timestamp 1 [0] `shouldBe` Timestamp 1 [0]
    Timestamp 0 [0] `leastUpperBound` Timestamp 0 [1] `shouldBe` Timestamp 0 [1]
    Timestamp 0 [0] `leastUpperBound` Timestamp 1 [1] `shouldBe` Timestamp 1 [1]
    Timestamp 1 [0] `leastUpperBound` Timestamp 0 [1] `shouldBe` Timestamp 1 [1]

timestampsWithFrontier :: Spec
timestampsWithFrontier = describe "TimestampsWithFrontier" $ do
  it "update TimestampsWithFrontier" $ do
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 2 [0], Timestamp 0 [1]])
      (Timestamp 1 [0], 1)
      (Set.fromList [Timestamp 0 [1], Timestamp 1 [0]])
      [FrontierChange (Timestamp 1 [0]) 1, FrontierChange (Timestamp 2 [0]) (-1)]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0]])
      (Timestamp 1 [1], 1)
      (Set.fromList [Timestamp 0 [0]])
      []
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 1 [1]])
      (Timestamp 0 [0], 1)
      (Set.fromList [Timestamp 0 [0]])
      [FrontierChange (Timestamp 0 [0]) 1, FrontierChange (Timestamp 1 [1]) (-1)]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0], Timestamp 1 [1]])
      (Timestamp 0 [0], -1)
      (Set.fromList [Timestamp 1 [1]])
      [FrontierChange (Timestamp 0 [0]) (-1), FrontierChange (Timestamp 1 [1]) 1]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0], Timestamp 0 [0]])
      (Timestamp 0 [0], -1)
      (Set.fromList [Timestamp 0 [0]])
      []
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0]])
      (Timestamp 0 [0], -1)
      (Set.fromList [])
      [FrontierChange (Timestamp 0 [0]) (-1)]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [0]])
      (Timestamp 0 [0], 1)
      (Set.fromList [Timestamp 0 [0]])
      []
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [])
      (Timestamp 0 [0], 1)
      (Set.fromList [Timestamp 0 [0]])
      [FrontierChange (Timestamp 0 [0]) 1]
      `shouldBe` True
    updateTimestampsWithFrontierChecker
      (MultiSet.fromList [Timestamp 0 [], Timestamp 3 [], Timestamp 4 []])
      (Timestamp 0 [], -1)
      (Set.fromList [Timestamp 3 []])
      [ FrontierChange (Timestamp 0 []) (-1)
      , FrontierChange (Timestamp 3 []) 1
      ]
      `shouldBe` True

dataChangeBatch :: Spec
dataChangeBatch = describe "DataChangeBatch" $ do
  it "make DataChangeBatch" $ do
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 0
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 0
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 1
      ]
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 0
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 0
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 0
      ]
      []
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) (-1)
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 1
      ]
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
      , DataChange (HM.fromList [("b", Null)]) (Timestamp  0         []) 1
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 1
      , DataChange (HM.fromList [("b", Null)]) (Timestamp  0         []) (-1)
      ]
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 2]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
      , DataChange (HM.fromList [("b", Null)]) (Timestamp  0         []) 1
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) (-1)
      , DataChange (HM.fromList [("b", Null)]) (Timestamp  0         []) (-1)
      ]
      []
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
      , DataChange (HM.fromList [("b", Null)]) (Timestamp  0         []) 1
      , DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) (-1)
      ]
      [ DataChange (HM.fromList [("b", Null)]) (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatchChecker
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
      ]
      [ DataChange (HM.fromList [("a", Null)]) (Timestamp  0         []) 1]
      `shouldBe` True
    mkDataChangeBatch
      [ DataChange (HM.fromList [("a", Number 1)]) (Timestamp (0 :: Int) []) 1
      , DataChange (HM.fromList [("b", Number 2)]) (Timestamp (0 :: Int) []) 1
      , DataChange (HM.fromList [("c", Number 3)]) (Timestamp (3 :: Int) []) 1]
      `shouldBe`
      DataChangeBatch
      { dcbLowerBound = Set.singleton (Timestamp 0 [])
      , dcbChanges = [ DataChange (HM.fromList [("b", Number 2)]) (Timestamp 0 []) 1
                     , DataChange (HM.fromList [("a", Number 1)]) (Timestamp 0 []) 1
                     , DataChange (HM.fromList [("c", Number 3)]) (Timestamp 3 []) 1
                     ]
      }


frontierMove :: Spec
frontierMove = describe "FrontierMove" $ do
  it "move frontier" $ do
    moveFrontier Set.empty MoveLater (Timestamp 0 [0])
      `shouldBe` ( Set.singleton (Timestamp 0 [0])
                 , [FrontierChange (Timestamp 0 [0]) 1]
                 )
    moveFrontier (Set.singleton (Timestamp 0 [0])) MoveLater (Timestamp 0 [1])
      `shouldBe` ( Set.singleton (Timestamp 0 [1])
                 , [ FrontierChange (Timestamp 0 [1]) 1
                   , FrontierChange (Timestamp 0 [0]) (-1)
                   ]
                 )
    moveFrontier (Set.singleton (Timestamp 0 [1])) MoveEarlier (Timestamp 0 [0])
      `shouldBe` ( Set.singleton (Timestamp 0 [0])
                 , [ FrontierChange (Timestamp 0 [0]) 1
                   , FrontierChange (Timestamp 0 [1]) (-1)
                   ]
                 )
    moveFrontier (Set.fromList [Timestamp 0 [1], Timestamp 1 [0]]) MoveLater (Timestamp 1 [1])
      `shouldBe` ( Set.singleton (Timestamp 1 [1])
                 , [ FrontierChange (Timestamp 1 [1]) 1
                   , FrontierChange (Timestamp 1 [0]) (-1)
                   , FrontierChange (Timestamp 0 [1]) (-1)
                   ]
                 )
    moveFrontier (Set.fromList [Timestamp 0 [0,1], Timestamp 0 [1,0]]) MoveLater (Timestamp 1 [0,0])
      `shouldBe` ( Set.fromList [Timestamp 0 [0,1], Timestamp 0 [1,0], Timestamp 1 [0,0]]
                 , [ FrontierChange (Timestamp 1 [0,0]) 1
                   ]
                 )
    moveFrontier Set.empty MoveEarlier (Timestamp 0 [])
      `shouldBe` (Set.singleton (Timestamp 0 [])
                 , [FrontierChange (Timestamp 0 []) 1])
    moveFrontier (Set.singleton (Timestamp 0 [])) MoveEarlier (Timestamp 3 [])
      `shouldBe` (Set.singleton (Timestamp 0 [])
                 , [])

frontierOrder :: Spec
frontierOrder = describe "FrontierOrder" $ do
  it "frontier causal order with timestamp" $ do
    Set.singleton (Timestamp (0 :: Int) [0])
      `causalCompare` Timestamp (0 :: Int) [0] `shouldBe` PEQ
    Set.singleton (Timestamp (0 :: Int) [0])
      `causalCompare` Timestamp (0 :: Int) [1] `shouldBe` PLT
    Set.singleton (Timestamp (0 :: Int) [1])
      `causalCompare` Timestamp (0 :: Int) [0] `shouldBe` PGT
    Set.singleton (Timestamp (0 :: Int) [1])
      `causalCompare` Timestamp (1 :: Int) [0] `shouldBe` PNONE
    Set.fromList [Timestamp (1 :: Int) [0], Timestamp 0 [1]]
      `causalCompare` Timestamp (0 :: Int) [2] `shouldBe` PLT
    Set.fromList [Timestamp (1 :: Int) [0], Timestamp 0 [1]]
      `causalCompare` Timestamp (2 :: Int) [0] `shouldBe` PLT
    Set.fromList [Timestamp (2 :: Int) [0], Timestamp 0 [2]]
      `causalCompare` Timestamp (1 :: Int) [1] `shouldBe` PNONE

index :: Spec
index = describe "Index" $ do
  it "add DataChangeBatch to Index" $ do
    addChangeBatchToIndexChecker
      [mkDataChangeBatch [DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1]]
      (Index [mkDataChangeBatch [DataChange (HM.fromList [("a", Null)]) (Timestamp 0 []) 1]])
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1]
      , mkDataChangeBatch [DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1]
      ]
      (Index [mkDataChangeBatch [DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 2]])
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1
                          ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1]
      ]
      (Index [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1
                                 ]
             , mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1]
             ]
      )
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1
                          ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) (-1)
                          ]
      ]
      (Index [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 2]
             ]
      )
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("c", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("d", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("e", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("f", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("g", Null)]) (Timestamp (0 :: Int) []) 1 ]
      ]
      (Index [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("c", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("d", Null)]) (Timestamp (0 :: Int) []) 1
                                 ]
             , mkDataChangeBatch [ DataChange (HM.fromList [("e", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("f", Null)]) (Timestamp (0 :: Int) []) 1
                                 ]
             , mkDataChangeBatch [ DataChange (HM.fromList [("g", Null)]) (Timestamp (0 :: Int) []) 1 ]
             ]
      )
      `shouldBe` True
    addChangeBatchToIndexChecker
      [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("c", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("d", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("e", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("f", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("g", Null)]) (Timestamp (0 :: Int) []) 1 ]
      , mkDataChangeBatch [ DataChange (HM.fromList [("h", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("i", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("j", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("k", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("l", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("m", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("n", Null)]) (Timestamp (0 :: Int) []) 1
                          , DataChange (HM.fromList [("o", Null)]) (Timestamp (0 :: Int) []) 1
                          ]
      ]
      (Index [ mkDataChangeBatch [ DataChange (HM.fromList [("a", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("b", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("c", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("d", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("e", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("f", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("g", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("h", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("i", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("j", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("k", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("l", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("m", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("n", Null)]) (Timestamp (0 :: Int) []) 1
                                 , DataChange (HM.fromList [("o", Null)]) (Timestamp (0 :: Int) []) 1
                                 ]
             ]
      )
      `shouldBe` True
