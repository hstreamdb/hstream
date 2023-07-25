module Slt.Cli.Runner where

import Slt.Cli.Parser

execMainOpts :: Opts -> IO ()
execMainOpts Opts {globalOpts, globalCmd} =
  case globalCmd of
    CmdParse {} -> undefined
    CmdExec {} -> undefined
    CmdComplete {} -> undefined
