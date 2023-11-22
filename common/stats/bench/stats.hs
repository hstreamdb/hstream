{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Monad
import           Criterion.Main
import           Data.Atomics   (atomicModifyIORefCAS)
import           Data.IORef

import           HStream.Base   (runConc)
import           HStream.Stats

-- | To run the benchmark:
--
-- > cabal run -- common-bench-stats --output bench.html --regress allocated:iters --regress numGcs:iters +RTS -T
main :: IO ()
main =
  defaultMain
    [ bgroup "counter" [ bench "IORef" $ nfIO (runIORef 100000)
                       , bench "IORefCAS" $ nfIO (runIORef 100000)
                       , bench "StatsHolder" $ nfIO (runStatsHolder 100000)
                       ]
    ]

runIORef :: Int -> IO ()
runIORef n = do
  ref <- newIORef 0
  runConc n $ atomicModifyIORef' ref (\x -> (x+1, ()))
  r <- readIORef ref
  when (r /= n) $ error "Error: wrong result!"

runIORefCAS :: Int -> IO ()
runIORefCAS n = do
  ref <- newIORef 0
  runConc n $ atomicModifyIORefCAS ref (\x -> (x+1, ()))
  r <- readIORef ref
  when (r /= n) $ error "Error: wrong result!"

runStatsHolder :: Int -> IO ()
runStatsHolder n = do
  h <- newStatsHolder True
  runConc n $ stream_stat_add_append_in_bytes h "name" 1
  s <- newAggregateStats h
  r <- fromIntegral <$> stream_stat_get_append_in_bytes s "name"
  when (r /= n) $ error "Error: wrong result!"
