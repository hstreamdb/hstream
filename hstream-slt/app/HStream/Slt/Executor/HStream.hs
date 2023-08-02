module HStream.Slt.Executor.HStream where

import           Control.Monad.IO.Class
import           Control.Monad.State
import qualified Data.Text              as T
import           HStream.Client.Execute (initCliContext)
import           HStream.Slt.Executor
import           HStream.Utils.RPC

newtype HStreamExecutor = HStreamExecutor
  { conn :: HStreamCliContext
  }

data HStreamExecutorCtx executor a = HStreamExecutorCtx
  { unHStreamExecutorCtx :: ExecutorM executor a,
    mainStreamName       :: T.Text,
    groupByIndex         :: Int
  }
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
    refineOpts GlobalOpts {debug, executorsAddr} =
      RefinedCliConnOpts
        { addr = undefined,
          clientConfig = undefined,
          retryTimeout = undefined
        }
    getExecutorsAddr [] = SocketAddr "127.0.0.1" 6570
    getExecutorsAddr ((ExecutorKindHStream, addr) : xs) = undefined
    getExecutorsAddr (_ : xs) = getExecutor xs
