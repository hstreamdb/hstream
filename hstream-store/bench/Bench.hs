module Main where

import           Criterion.Main      (bgroup, defaultMain)
import           HStream.AppendBench

main :: IO ()
main = defaultMain [ bgroup "append benchmark" appendBench]
