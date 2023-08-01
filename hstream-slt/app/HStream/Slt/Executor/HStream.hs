module HStream.Slt.Executor.HStream where

import           Control.Monad.IO.Class
import           Control.Monad.State
import           HStream.Client.Execute (initCliContext)
import           HStream.Slt.Executor

newtype HStreamExecutor = HStreamExecutor
  { conn :: HStreamCliContext
  }

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
  open' = open''
  selectWithoutFrom = undefined
  insertValues = undefined
  sqlDataTypeToLiteral' = undefined
  sqlDataValueToLiteral = undefined

----------------------------------------

open'' :: HStreamExecutorCtx HStreamExecutor
open'' = do
  opts <- getOpts
  let refinedOpts :: RefinedCliConnOpts = undefined
  conn <- liftIO $ initCliContext refinedOpts
  pure $ HStreamExecutor conn
  where
    refineOpts :: GlobalOpts -> RefinedCliConnOpts
    refineOpts = undefined
