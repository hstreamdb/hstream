module Slt.Cli.Runner where

import           Slt.Cli.Parser
import           Slt.Executor
import           Slt.Executor.Dummy

execMainOpts :: Opts -> IO ()
execMainOpts opts@Opts {globalOpts, globalCmd} = evalExecutorCtx @DummyExecutorCtx $ do
  setOpts globalOpts
  debugPrint opts
  case globalCmd of
    CmdParse {}    -> undefined
    CmdExec {}     -> undefined
    CmdComplete {} -> undefined

execCmdParse :: ParseOpts -> DummyExecutorM ()
execCmdParse = undefined

execCmdExec :: ExecOpts -> DummyExecutorM ()
execCmdExec = undefined

execCmdComplete :: CompleteOpts -> DummyExecutorM ()
execCmdComplete = undefined
