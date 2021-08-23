{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Concurrent
import           Control.Monad
import           Criterion.Main

import           HStream.Stats

-- TODO

-- | To run the benchmark:
--
-- > cabal run -- common-bench-stats --output bench.html --regress allocated:iters +RTS -T
main :: IO ()
main = undefined
