{-# LANGUAGE BangPatterns #-}

module Main where

import           Control.Monad
import           Criterion.Main
import qualified Data.List                    as L
import qualified Data.Vector                  as V
import qualified Data.Vector.Algorithms.Intro as V
import           System.Random                (randomRIO)

sortList :: [Int] -> IO [Int]
sortList xs = pure $! L.sort xs

sortVectorIntro :: V.Vector Int -> IO (V.Vector Int)
sortVectorIntro xs = do
  !mvec <- V.unsafeThaw xs
  V.sort mvec
  V.unsafeFreeze mvec

--ys ::

main :: IO ()
main = do
  let n = 1000000
  let !xs1 = [0..n]
      !ys1 = V.fromList xs1
      !xs2 = [n..0]
      !ys2 = V.fromList xs2

  -- n(1000000) is too many for generate random numbers, so we use 10000 instead.
  !xs3 <- replicateM 10000 $ randomRIO (0, n :: Int)
  let !ys3 = V.fromList xs3

  defaultMain
    [ bgroup "sort1" [ bench "List" $ nfIO (sortList xs1)
                     , bench "Vector.Algorithms.Intro" $ nfIO (sortVectorIntro ys1)
                     ]
    , bgroup "sort2" [ bench "List" $ nfIO (sortList xs2)
                     , bench "Vector.Algorithms.Intro" $ nfIO (sortVectorIntro ys2)
                     ]
    , bgroup "sort3" [ bench "List" $ nfIO (sortList xs3)
                     , bench "Vector.Algorithms.Intro" $ nfIO (sortVectorIntro ys3)
                     ]
    ]
