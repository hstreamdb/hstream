module Slt.Cli.Runner where

import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Either
import           Data.Foldable
import qualified Data.Text              as T
import           Slt.Cli.Parser
import           Slt.Executor
import           Slt.Executor.Dummy
import           Slt.Executor.HStream   (HStreamExecutorCtx (HStreamExecutorCtx))
import           Slt.Executor.SQLite
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
  plans <-
    traverse
      ( \f -> liftIO $ do
          x <- catchFile parsePlan f
          pure (f, x)
      )
      files
  xs <-
    traverse
      ( \(f, x) -> do
          y <- execExecutors executors x
          pure (f, y)
      )
      plans
  forM_ xs $ \(f, errors) -> do
    case lefts errors of
      [] -> pure ()
      ys -> forM_ ys $ \errs -> do
        liftIO $ do
          putStrLn $ "errors @ " <> f <> ":"
          forM errs $ \err -> do
            putStrLn $ "    " <> T.unpack err

execExecutors :: [ExecutorKind] -> SltSuite -> DummyExecutorM [Either [T.Text] ()]
execExecutors executorKind sltSuite = do
  forM executorKind (execSuite sltSuite)

execSuite :: SltSuite -> ExecutorKind -> DummyExecutorM (Either [T.Text] ())
execSuite sltSuite executorKind = do
  debug <- isDebug
  if debug
    then pure $ pure ()
    else case executorKind of
      ExecutorKindSQLite -> evalNewExecutorCtx @SQLiteExecutorCtx $ do
        pure undefined
      ExecutorKindHStream -> evalNewExecutorCtx @HStreamExecutorCtx $ do
        pure undefined

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
