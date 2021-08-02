{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.SQL.ExecPlan.Utils
  ( allTraverses
  , fuseOnEq
  ) where

import qualified Data.HashMap.Strict                   as HM
import qualified Data.Text                             as T
import           HStream.Processing.Processor.Internal (InternalSourceConfig (iSourceName),
                                                        Task (..))

getStarts :: Task -> [T.Text]
getStarts Task{..} = iSourceName <$> HM.elems taskSourceConfig

fuseOnEq :: Eq a => [a] -> [a] -> (([a], [a]), [a])
fuseOnEq [] ys = (([], ys), [])
fuseOnEq xs [] = ((xs, []), [])
fuseOnEq (x:xs) (y:ys)
  | x == y    = (([], []), x:xs)
  | otherwise = ((x:xs', y:ys'), common')
  where ((xs', ys'), common') = fuseOnEq xs ys

traverseTask :: Task -> T.Text -> [T.Text]
traverseTask Task{..} = go []
  where
    go acc cur =
      case HM.lookup cur taskTopologyForward of
        Just (_, [])    -> acc
        Just (_, nexts) -> head $ map (go (acc ++ [cur])) nexts
        Nothing         -> acc

allTraverses :: Task -> [[T.Text]]
allTraverses task = map (traverseTask task) (getStarts task)
