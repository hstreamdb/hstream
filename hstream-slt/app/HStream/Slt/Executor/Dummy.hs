module HStream.Slt.Executor.Dummy where

import           Control.Monad.State
import           HStream.Slt.Cli.Parser (GlobalOpts)
import           HStream.Slt.Executor

type DummyExecutorM = DummyExecutorCtx DummyExecutor

data DummyExecutor

newtype DummyExecutorCtx executor a = DummyExecutorCtx
  { unDummyExecutorCtx :: ExecutorM executor a
  }
  deriving (Functor, Applicative, Monad, MonadIO, MonadState (ExecutorState executor))

instance ExecutorCtx DummyExecutorCtx executor where
  evalExecutorCtx = evalExecutorCtx . unDummyExecutorCtx
  setOpts = DummyExecutorCtx . setOpts
  getOpts = DummyExecutorCtx getOpts
  setExecutor = DummyExecutorCtx . setExecutor
  getExecutor = DummyExecutorCtx getExecutor
  isDebug = DummyExecutorCtx isDebug
  pushSql = DummyExecutorCtx . pushSql
  getSql = DummyExecutorCtx getSql
  recover = DummyExecutorCtx . recover
  reportError = DummyExecutorCtx reportError
  clearSql = DummyExecutorCtx clearSql

----------------------------------------

fromGlobalOpts :: GlobalOpts -> DummyExecutorM ()
fromGlobalOpts = setOpts
