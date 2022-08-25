{-# LANGUAGE RecordWildCards #-}

module DiffFlow.Weird where

import           Data.Hashable     (Hashable)
import qualified Data.HashMap.Lazy as HM
import qualified Data.List         as L
import qualified Data.Set          as Set

import           DiffFlow.Types

-- Quite weird.
-- This one uses only 'dcRow' as key instead of '(dcRow, dcTimestamp)'
-- so records with the same row will be merged when you call 'updateDatachangeBatch''.
-- It is used as a pool to accumulate all changes into a final result.
mkDataChangeBatch' :: (Hashable a, Ord a, Show a,
                       Hashable t, Ord t, Show t)
                  => [DataChange a t]
                  -> DataChangeBatch a t
mkDataChangeBatch' changes = DataChangeBatch frontier sortedChanges
  where getKey DataChange{..} = dcRow
        coalescedChanges = HM.filter (\DataChange{..} -> dcDiff /= 0) $
          L.foldl (\acc x -> HM.insertWith
                    (\new old -> old {dcDiff = dcDiff new + dcDiff old} )
                    (getKey x) x acc) HM.empty changes
        sortedChanges = L.sort $ HM.elems coalescedChanges
        frontier = L.foldl
          (\acc DataChange{..} -> acc ~>> (MoveEarlier,dcTimestamp))
          Set.empty sortedChanges

updateDataChangeBatch' :: (Hashable a, Ord a, Show a,
                           Hashable t, Ord t, Show t)
                      => DataChangeBatch a t
                      -> ([DataChange a t] -> [DataChange a t])
                      -> DataChangeBatch a t
updateDataChangeBatch' oldBatch f =
  mkDataChangeBatch' $ f (dcbChanges oldBatch)
