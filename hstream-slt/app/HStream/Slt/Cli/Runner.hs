module Slt.Cli.Runner where

import           Control.Exception
import           Control.Monad.IO.Class
import           Data.Foldable
import           Slt.Cli.Parser
import           Slt.Executor
import           Slt.Executor.Dummy
import           Slt.Plan
import           System.Directory

execMainOpts :: Opts -> IO ()
execMainOpts opts@Opts {globalOpts, globalCmd} = evalExecutorCtx @DummyExecutorCtx $ do
  setOpts globalOpts
  debugPrint opts
  currentDirectory <- liftIO getCurrentDirectory
  debugPutStrLn $ "current directory = " <> currentDirectory
  case globalCmd of
    CmdParse cmd   -> execCmdParse cmd
    CmdExec {}     -> undefined
    CmdComplete {} -> undefined

execCmdParse :: ParseOpts -> DummyExecutorM ()
execCmdParse
  ParseOpts
    { unParseOpts = files
    } = do
    liftIO $ traverse_ (catchFile parseTest) files

execCmdExec :: ExecOpts -> DummyExecutorM ()
execCmdExec = undefined

execCmdComplete :: CompleteOpts -> DummyExecutorM ()
execCmdComplete = undefined

----------------------------------------
catchFile :: (FilePath -> IO a) -> FilePath -> IO a
catchFile xs f =
  xs f `catch` \(e :: SomeException) -> do
    putStrLn $ "error @ " <> f <> ":"
    putStrLn $ "    " <> show e
    error $ show e
