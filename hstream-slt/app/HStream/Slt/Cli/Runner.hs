module Slt.Cli.Runner where

import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Foldable
import           Slt.Cli.Parser
import           Slt.Executor
import           Slt.Executor.Dummy
import           Slt.Executor.HStream       (HStreamExecutorCtx)
import           Slt.Executor.SQLite
import           Slt.Plan
import           Slt.Plan.RandomNoTablePlan
import           System.Directory
import           System.Exit

execMainOpts :: Opts -> IO ()
execMainOpts opts@Opts {globalOpts, globalCmd} = evalExecutorCtx @DummyExecutorCtx $ do
  setOpts globalOpts
  debugPrint opts
  currentDirectory <- liftIO getCurrentDirectory
  debugPutStrLn $ "current directory = " <> currentDirectory
  case globalCmd of
    CmdParse cmd   -> execCmdParse cmd
    CmdExec cmd    -> execCmdExec cmd
    CmdComplete {} -> undefined

----------------------------------------

execCmdParse :: ParseOpts -> DummyExecutorM ()
execCmdParse
  ParseOpts
    { unParseOpts = files
    } = do
    liftIO $ traverse_ (catchFile parseTest) files

----------------------------------------

execCmdExec :: ExecOpts -> DummyExecutorM ()
execCmdExec ExecOpts {files, executors} = do
  plans <- traverse parse files
  forM_ plans $ \(f, x) -> do
    liftIO $ putStrLn $ "[INFO]: Executing " <> f <> " ..."
    execExecutors executors x
  printErrors
  exit
  where
    parse f = liftIO $ do
      x <- catchFile parsePlan f
      pure (f, x)
    exit = do
      ret <- exitCode
      liftIO $ do
        case ret of
          0 -> exitSuccess
          _ -> exitFailure

execExecutors :: [ExecutorKind] -> SltSuite -> DummyExecutorM ()
execExecutors executorKind sltSuite = do
  forM_ executorKind (execSuite sltSuite)

execSuite :: SltSuite -> ExecutorKind -> DummyExecutorM ()
execSuite sltSuite executorKind = do
  case executorKind of
    ExecutorKindSQLite -> evalNewExecutorCtx @SQLiteExecutorCtx $ do
      execSltSuite sltSuite
    ExecutorKindHStream -> evalNewExecutorCtx @HStreamExecutorCtx $ do
      execSltSuite sltSuite

execSltSuite :: SltSuite -> SltExecutor m executor => m executor ()
execSltSuite (SltSuite sltSuite) = forM_ sltSuite $ \plan -> do
  case plan of
    PlanRandomNoTable plan' -> evalRandomNoTablePlan plan'
    _                       -> undefined

----------------------------------------

execCmdComplete :: CompleteOpts -> DummyExecutorM ()
execCmdComplete = undefined

----------------------------------------

catchFile :: (FilePath -> IO a) -> FilePath -> IO a
catchFile xs f =
  xs f `catch` \(e :: SomeException) -> do
    putStrLn $ "error @ " <> f <> ":"
    putStrLn $ "    " <> show e
    error $ show e
