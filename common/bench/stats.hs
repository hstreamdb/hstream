{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Monad
import           Criterion.Main
import           Data.IORef

import           HStream.Stats
import           HStream.Utils  (runConc)

-- | To run the benchmark:
--
-- > cabal run -- common-bench-stats --output bench.html --regress allocated:iters --regress numGcs:iters +RTS -T
main :: IO ()
main =
  defaultMain
    [ bgroup "counter" [ bench "IORef" $ nfIO (runIORef 100000)
                       , bench "StatsHolder" $ nfIO (runStatsHolder 100000)
                       ]
    ]

runIORef :: Int -> IO ()
runIORef n = do
  ref <- newIORef 0
  runConc n $ atomicModifyIORef' ref (\x -> (x+1, ()))
  r <- readIORef ref
  when (r /= n) $ error "Error: wrong result!"

runStatsHolder :: Int -> IO ()
runStatsHolder n = do
  h <- newStatsHolder
  runConc n $ stream_stat_add_append_payload_bytes h "name" 1
  s <- newAggregateStats h
  r <- fromIntegral <$> stream_stat_get_append_payload_bytes s "name"
  when (r /= n) $ error "Error: wrong result!"
