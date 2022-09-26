{-# LANGUAGE DeriveAnyClass        #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE StandaloneDeriving    #-}

module DiffFlow.Types where

import           Control.Exception (throw)
import           Control.Monad
import           Data.Aeson        (Object (..), Value (..))
import qualified Data.Aeson        as Aeson
import           Data.Hashable     (Hashable)
import qualified Data.HashMap.Lazy as HM
import qualified Data.List         as L
import           Data.MultiSet     (MultiSet)
import qualified Data.MultiSet     as MultiSet
import           Data.Set          (Set)
import qualified Data.Set          as Set
import qualified Data.Text         as T
import           Data.Vector       (Vector)
import qualified Data.Vector       as V
import           Data.Word         (Word64)
import           GHC.Generics      (Generic)

import           DiffFlow.Error

type Bag row = MultiSet row

data PartialOrdering = PLT | PEQ | PGT | PNONE deriving (Eq, Show)

data Timestamp a = Timestamp
  { timestampTime   :: a
  , timestampCoords :: [Word64] -- [outermost <--> innermost]
  }

instance (Show a) => Show (Timestamp a) where
  show Timestamp{..} = "<" <> show timestampTime
                           <> "|"
                           <> L.intercalate "," (L.map show timestampCoords) <> ">"

deriving instance (Eq a) => Eq (Timestamp a)
deriving instance (Ord a) => Ord (Timestamp a)
deriving instance Generic (Timestamp a)
deriving instance (Hashable a) => Hashable (Timestamp a)

class CausalOrd a b where
  causalCompare :: a -> b -> PartialOrdering

instance (Ord a) => CausalOrd (Timestamp a) (Timestamp a) where
  causalCompare ts1 ts2 =
    if len1 == len2 then
      if L.all (== EQ) compRes then PEQ
        else if L.all (\x -> x == EQ || x == LT) compRes then PLT
          else if L.all (\x -> x == EQ || x == GT) compRes then PGT
            else PNONE
      else throw . BasicTypesError $ "Trying comparing timestamps with different lengths: " <> T.pack (show len1) <> ", " <> T.pack (show len2)
    where len1 = L.length (timestampCoords ts1)
          len2 = L.length (timestampCoords ts2)
          compRes = (timestampTime ts1 `compare` timestampTime ts2) :
                    L.zipWith compare (timestampCoords ts1) (timestampCoords ts2)

leastTimestamp :: (Bounded a) => Int -> Timestamp a
leastTimestamp coords =
  Timestamp
  { timestampTime = minBound
  , timestampCoords = take coords [0..]
  }

leastUpperBound :: (Ord a) => Timestamp a -> Timestamp a -> Timestamp a
leastUpperBound ts1 ts2 =
  Timestamp { timestampTime = upperTime, timestampCoords = upperCoords }
  where upperTime = max (timestampTime ts1) (timestampTime ts2)
        upperCoords = L.zipWith max (timestampCoords ts1) (timestampCoords ts2)

leastUpperBoundMany :: (Ord a) => [Timestamp a] -> Timestamp a
leastUpperBoundMany tss =
  L.foldl1 (\acc ts -> leastUpperBound acc ts) tss

pushCoord :: (Ord a) => Timestamp a -> Timestamp a
pushCoord ts =
  Timestamp
  { timestampTime = timestampTime ts
  , timestampCoords = timestampCoords ts ++ [0]
  }

incCoord :: (Ord a) => Timestamp a -> Timestamp a
incCoord ts =
  Timestamp
  { timestampTime = timestampTime ts
  , timestampCoords = init (timestampCoords ts) ++ [last (timestampCoords ts) + 1]
  }

popCoord :: (Ord a) => Timestamp a -> Timestamp a
popCoord ts =
  Timestamp
  { timestampTime = timestampTime ts
  , timestampCoords = init (timestampCoords ts)
  }

infix 4 <.=
(<.=) :: (CausalOrd a b) => a -> b -> Bool
(<.=) x y
  | compRes == PLT = True
  | compRes == PEQ = True
  | otherwise      = False
  where compRes = x `causalCompare` y

----
type Frontier a = Set (Timestamp a)

instance (Ord a) => CausalOrd (Frontier a) (Timestamp a) where
  causalCompare ft ts =
    Set.foldl (\acc x -> if acc == PNONE then x `causalCompare` ts else acc) PNONE ft

data MoveDirection = MoveLater | MoveEarlier deriving (Show, Eq, Enum, Read)
data FrontierChange a = FrontierChange
  { frontierChangeTs   :: Timestamp a
  , frontierChangeDiff :: Int
  }

deriving instance (Eq a) => Eq (FrontierChange a)
deriving instance (Ord a) => Ord (FrontierChange a)
instance (Show a) => Show (FrontierChange a) where
  show FrontierChange{..} = "(" <> show frontierChangeTs
                                <> ", " <> show frontierChangeDiff
                                <> ")"



-- Move later: remove timestamps that are earlier than ts.
-- Move earlier: remove timestamps that are later than ts.
-- FIXME: when to stop or error?
moveFrontier :: (Ord a, Show a)
             => Frontier a -> MoveDirection -> Timestamp a
             -> (Frontier a, [FrontierChange a])
moveFrontier ft direction ts =
  if goOn then (Set.insert ts ft', FrontierChange ts 1 : changes)
               else (ft', changes)
  where
    (goOn, changes) = case direction of
      MoveLater   ->
        Set.foldl (\(goOn,acc) x ->
          case goOn of
            False -> (goOn,acc)
            True  -> case x `causalCompare` ts of
              PEQ   -> if L.null acc then (False,acc)
                         else throw . BasicTypesError $
                              "Already moved to " <> T.pack (show ts) <> "? Found " <> T.pack (show x)
              PGT   -> if L.null acc then (False,acc)
                         else throw . BasicTypesError $
                              "Already moved to " <> T.pack (show ts) <> "? Found " <> T.pack (show x)
              PLT   -> let change = FrontierChange x (-1) in (goOn, change:acc)
              PNONE -> (goOn,acc)) (True,[]) ft
      MoveEarlier ->
        Set.foldl (\(goOn,acc) x ->
          case goOn of
            False -> (goOn,acc)
            True  -> case x `causalCompare` ts of
              PEQ   -> if L.null acc then (False,acc)
                         else throw . BasicTypesError $
                              "Already moved to " <> T.pack (show ts) <> "? Found " <> T.pack (show x)
              PLT   -> if L.null acc then (False,acc)
                         else throw . BasicTypesError $
                              "Already moved to " <> T.pack (show ts) <> "? Found " <> T.pack (show x)
              PGT   -> let change = FrontierChange x (-1) in (goOn, change:acc)
              PNONE -> (goOn,acc)) (True,[]) ft
    ft' = L.foldl (\acc FrontierChange{..} -> Set.delete frontierChangeTs acc) ft changes

infixl 7 ~>>
(~>>) :: (Ord a, Show a) => Frontier a -> (MoveDirection, Timestamp a) -> Frontier a
(~>>) ft (direction,ts) = fst $ moveFrontier ft direction ts
----

data TimestampsWithFrontier a = TimestampsWithFrontier
  { tsfTimestamps :: MultiSet (Timestamp a)
  , tsfFrontier   :: Frontier a
  }

instance (Show a) => Show (TimestampsWithFrontier a) where
  show TimestampsWithFrontier{..} = "[\n\tTimestamps: " <> show tsfTimestamps
                                  <> "\n\tFrontier: " <> show tsfFrontier
                                  <> "\n]"

emptyTimestampsWithFrontier :: TimestampsWithFrontier a
emptyTimestampsWithFrontier =
  TimestampsWithFrontier
  { tsfTimestamps = MultiSet.empty
  , tsfFrontier   = Set.empty
  }

-- FIXME: Very weird. Should be replaced with a more functional version
updateTimestampsWithFrontier :: (Ord a)
                             => TimestampsWithFrontier a
                             -> Timestamp a
                             -> Int
                             -> (TimestampsWithFrontier a, [FrontierChange a])
updateTimestampsWithFrontier TimestampsWithFrontier{..} ts diff
  -- an item in tsfTimestamps has been removed
  | MultiSet.occur ts timestampsInserted == 0 =
    case Set.member ts tsfFrontier of
      -- the frontier is unmodified.
      False -> let tsf'   = TimestampsWithFrontier timestampsInserted tsfFrontier
               in (tsf', [])
      -- the item is also removed from the frontier, new items may be required
      -- to be inserted to the frontier to keep [frontier <.= each ts]
      True  -> let change = FrontierChange ts (-1)
                   frontierRemoved = Set.delete ts tsfFrontier

                   (frontierInserted, frontierAdds) =
                     L.foldl (\(curFrontier,cand) x ->
                                       if curFrontier `causalCompare` x == PNONE
                                       then (Set.insert x curFrontier, cand ++ [x])
                                       else (curFrontier,cand)) (frontierRemoved,[])
                     (L.filter (\x -> x `causalCompare` ts == PGT) (MultiSet.toList timestampsInserted))

                   frontierChanges = L.map (\x -> FrontierChange x 1) frontierAdds
                   tsf' = TimestampsWithFrontier timestampsInserted frontierInserted
                in (tsf', change:frontierChanges)
    -- the item was not present but now got inserted. it is new!
  | MultiSet.occur ts timestampsInserted == diff =
    case tsfFrontier `causalCompare` ts of
      -- the invariant [frontier <.= each ts] still keeps
      PLT -> let tsf' = TimestampsWithFrontier timestampsInserted tsfFrontier
              in (tsf', [])
      -- the invariant [frontier <.= each ts] is broken, which means
      -- the new-added item should be added to the frontier to keep
      -- it. However, every item in the frontier is incomparable so
      -- then some redundant items should be deleted from the frontier
      _   -> let change = FrontierChange ts 1
                 frontierInserted = Set.insert ts tsfFrontier
                 frontierRemoves = Set.filter (\x -> x `causalCompare` ts == PGT) frontierInserted
                 frontierChanges = L.map (\x -> FrontierChange x (-1)) (Set.toList frontierRemoves)
                 frontierRemoved = Set.foldl (flip Set.delete) frontierInserted frontierRemoves
                 tsf' = TimestampsWithFrontier timestampsInserted frontierRemoved
              in (tsf', change:frontierChanges)
  | otherwise = let tsf' = TimestampsWithFrontier timestampsInserted tsfFrontier
                 in (tsf', [])
  where timestampsInserted = MultiSet.insertMany ts diff tsfTimestamps

infixl 7 ->>
(->>) :: (Ord a)
      => TimestampsWithFrontier a
      -> (Timestamp a, Int)
      -> TimestampsWithFrontier a
(->>) tsf (ts,diff) = fst $ updateTimestampsWithFrontier tsf ts diff

----

data DataChange row a = DataChange
  { dcRow       :: row
  , dcTimestamp :: Timestamp a
  , dcDiff      :: Int
  }
deriving instance (Eq row, Eq a) => Eq (DataChange row a)
deriving instance (Show row, Show a) => Show (DataChange row a)
instance (Ord row, Ord a) => Ord (DataChange row a) where
  compare dc1 dc2 =
    case dcTimestamp dc1 `compare` dcTimestamp dc2 of
      LT -> LT
      GT -> GT
      EQ -> case dcRow dc1 `compare` dcRow dc2 of
              LT -> LT
              GT -> GT
              EQ -> dcDiff dc1 `compare` dcDiff dc2

compareDataChangeByTimeFirst :: (Ord row, Ord a)
                             => DataChange row a
                             -> DataChange row a
                             -> Ordering
compareDataChangeByTimeFirst dc1 dc2 =
  case dcTimestamp dc1 `causalCompare` dcTimestamp dc2 of
    PLT   -> LT
    PEQ   -> EQ
    PGT   -> GT
    PNONE -> dcRow dc1 `compare` dcRow dc2

data DataChangeBatch row a = DataChangeBatch
  { dcbLowerBound :: Frontier a
  , dcbChanges    :: [DataChange row a] -- sorted and de-duplicated
  }
deriving instance (Eq row, Eq a) => Eq (DataChangeBatch row a)
deriving instance (Ord row, Ord a) => Ord (DataChangeBatch row a)
deriving instance (Show row, Show a) => Show (DataChangeBatch row a)

emptyDataChangeBatch :: DataChangeBatch row a
emptyDataChangeBatch = DataChangeBatch {dcbLowerBound=Set.empty, dcbChanges=[]}

dataChangeBatchLen :: DataChangeBatch row a -> Int
dataChangeBatchLen DataChangeBatch{..} = L.length dcbChanges

mkDataChangeBatch :: (Hashable a, Ord a, Show a,
                      Hashable row, Ord row, Show row)
                  => [DataChange row a]
                  -> DataChangeBatch row a
mkDataChangeBatch changes = DataChangeBatch frontier sortedChanges
  where getKey DataChange{..} = (dcRow, dcTimestamp)
        coalescedChanges = HM.filter (\DataChange{..} -> dcDiff /= 0) $
          L.foldl (\acc x -> HM.insertWith
                    (\new old -> old {dcDiff = dcDiff new + dcDiff old} )
                    (getKey x) x acc) HM.empty changes
        sortedChanges = L.sort $ HM.elems coalescedChanges
        frontier = L.foldl
          (\acc DataChange{..} -> acc ~>> (MoveEarlier,dcTimestamp))
          Set.empty sortedChanges

updateDataChangeBatch :: (Hashable a, Ord a, Show a,
                          Hashable row, Ord row, Show row)
                      => DataChangeBatch row a
                      -> ([DataChange row a] -> [DataChange row a])
                      -> DataChangeBatch row  a
updateDataChangeBatch oldBatch f =
  mkDataChangeBatch $ f (dcbChanges oldBatch)

mergeJoinDataChangeBatch :: (Hashable a, Ord a, Show a,
                             Hashable row, Ord row, Show row)
                         => DataChangeBatch row a
                         -> Frontier a
                         -> DataChangeBatch row a
                         -> (row -> row)
                         -> (row -> row)
                         -> (row -> row -> row)
                         -> DataChangeBatch row a
mergeJoinDataChangeBatch self selfFt other keygen1 keygen2 rowgen =
  L.foldl (\acc (this,that) ->
             let thisKey = keygen1 (dcRow this)
                 thatKey = keygen2 (dcRow that)
              in if thisKey == thatKey && selfFt `causalCompare` dcTimestamp this == PGT then
               let newDataChange =
                     DataChange
                     { dcRow = rowgen (dcRow this) (dcRow that)
                     , dcTimestamp = leastUpperBound (dcTimestamp this) (dcTimestamp that)
                     , dcDiff = dcDiff this * dcDiff that
                     }
                in updateDataChangeBatch acc (\xs -> xs ++ [newDataChange])
             else acc
          ) emptyDataChangeBatch
  [(self_x, other_x) | self_x <- dcbChanges self, other_x <- dcbChanges other]

----

newtype Index row a = Index
  { indexChangeBatches :: [DataChangeBatch row a]
  }
deriving instance (Eq row, Eq a) => Eq (Index row a)
deriving instance (Ord row, Ord a) => Ord (Index row a)
deriving instance (Show row, Show a) => Show (Index row a)

addChangeBatchToIndex :: (Hashable a, Ord a, Show a,
                          Hashable row, Ord row, Show row)
                      => Index row a
                      -> DataChangeBatch row a
                      -> Index row a
addChangeBatchToIndex Index{..} changeBatch =
  Index (adjustBatches $ indexChangeBatches ++ [changeBatch])
  where
    adjustBatches [] = []
    adjustBatches [x] = [x]
    adjustBatches l@(x:y:xs)
      | dataChangeBatchLen lastBatch * 2 <= dataChangeBatchLen secondLastBatch = l
      | otherwise =
        let newBatch = mkDataChangeBatch (dcbChanges lastBatch ++ dcbChanges secondLastBatch)
         in adjustBatches ((L.init . L.init $ l) ++ [newBatch])
      where lastBatch = L.last l
            secondLastBatch = L.last . L.init $ l

-- FIXME: very low performance. Should take advantage of properties of DataChangeBatch
-- WARNING: result is backwards
getChangesForKey :: (Ord row, Ord a) => Index row a -> (row -> Bool) -> [DataChange row a]
getChangesForKey (Index batches) p =
  L.foldl (\acc batch ->
           let resultOfThisBatch =
                 L.foldl (\acc change@DataChange{..} ->
                          if p dcRow then change:acc else acc) [] (dcbChanges batch)
            in resultOfThisBatch ++ acc
          ) [] batches

getCountForKey :: (Ord row, Ord a) => Index row a -> row -> Timestamp a -> Int
getCountForKey (Index batches) row ts =
  L.foldl (\acc batch ->
             let countOfThisBatch =
                   L.foldl (\acc' change@DataChange{..} ->
                              if dcRow == row && dcTimestamp <.= ts
                              then acc' + dcDiff
                              else acc'
                           ) 0 (dcbChanges batch)
              in countOfThisBatch + acc
          ) 0 batches

mergeJoinIndex :: (Hashable a, Ord a, Show a,
                   Hashable row, Ord row, Show row)
               => Index row a
               -> Frontier a
               -> DataChangeBatch row a
               -> (row -> row)
               -> (row -> row)
               -> (row -> row -> row)
               -> DataChangeBatch row a
mergeJoinIndex self selfFt otherChangeBatch keygen1 keygen2 rowgen =
  L.foldl (\acc selfChangeBatch ->
             let newChangeBatch =
                   mergeJoinDataChangeBatch selfChangeBatch selfFt otherChangeBatch keygen1 keygen2 rowgen
              in updateDataChangeBatch acc (\xs -> xs ++ dcbChanges newChangeBatch)
          ) emptyDataChangeBatch (indexChangeBatches self)
