module Main where

import           HStream.Slt.Cli.Parser
import           HStream.Slt.Cli.Runner

main :: IO ()
main = do
  opts <- mainOptsParser
  execMainOpts opts
  pure ()
