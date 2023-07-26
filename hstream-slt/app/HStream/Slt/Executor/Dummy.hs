module Slt.Executor.Dummy where

import           Control.Monad.State
import           Slt.Cli.Parser      (GlobalOpts)
import           Slt.Executor

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

----------------------------------------

fromGlobalOpts :: GlobalOpts -> DummyExecutorM ()
fromGlobalOpts = setOpts
