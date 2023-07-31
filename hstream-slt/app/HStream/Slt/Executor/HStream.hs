module Slt.Executor.HStream where

import           Control.Monad.IO.Class
import           Control.Monad.State
import           Slt.Executor

data HStreamExecutor

newtype HStreamExecutorCtx executor a = HStreamExecutorCtx {unHStreamExecutorCtx :: ExecutorM executor a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadState (ExecutorState executor))

instance ExecutorCtx HStreamExecutorCtx executor where
  setExecutor = HStreamExecutorCtx . setExecutor
  isDebug = HStreamExecutorCtx isDebug
  evalExecutorCtx = evalExecutorCtx . unHStreamExecutorCtx
  setOpts = HStreamExecutorCtx . setOpts
  getOpts = HStreamExecutorCtx getOpts
  getExecutor = HStreamExecutorCtx getExecutor
  pushSql = HStreamExecutorCtx . pushSql
  getSql = HStreamExecutorCtx getSql
  recover = HStreamExecutorCtx . recover
  reportError = HStreamExecutorCtx reportError
  clearSql = HStreamExecutorCtx clearSql

instance SltExecutor HStreamExecutorCtx HStreamExecutor where
  open' = undefined
  selectWithoutFrom = undefined
  insertValues = undefined
  sqlDataTypeToLiteral' = undefined
  sqlDataValueToLiteral = undefined
