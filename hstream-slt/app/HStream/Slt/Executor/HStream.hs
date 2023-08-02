module HStream.Slt.Executor.HStream where

import           Control.Monad.IO.Class
import           Control.Monad.State
import qualified Data.Text              as T
import qualified Data.Text.Encoding     as T
import           HStream.Client.Action
import           HStream.Client.Execute
import           HStream.Client.Types
import           HStream.Slt.Cli.Parser
import           HStream.Slt.Executor
import           HStream.Utils

data HStreamExecutor = HStreamExecutor
  { conn           :: HStreamCliContext,
    mainStreamName :: T.Text,
    groupByIndex   :: Int
  }

newtype HStreamExecutorCtx executor a = HStreamExecutorCtx
  { unHStreamExecutorCtx :: ExecutorM executor a
  }
  deriving (Functor, Applicative, Monad, MonadIO, MonadState (ExecutorState executor))

type HStreamExecutorM = HStreamExecutorCtx HStreamExecutor

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

open'' :: HStreamExecutorM HStreamExecutor
open'' = do
  opts <- getOpts
  debug <- isDebug
  let refinedOpts :: RefinedCliConnOpts = refineOpts opts
  conn <- liftIO $ initCliContext refinedOpts
  streamName <- liftIO $ newRandomText 20
  liftIO $ createByName debug streamName conn
  pure $ HStreamExecutor conn streamName 0
  where
    refineOpts :: GlobalOpts -> RefinedCliConnOpts
    refineOpts GlobalOpts {executorsAddr} =
      RefinedCliConnOpts
        { addr = getExecutorsAddr executorsAddr,
          clientConfig = undefined,
          retryTimeout = undefined
        }
    getExecutorsAddr :: [(ExecutorKind, T.Text)] -> SocketAddr
    getExecutorsAddr [] = SocketAddr "127.0.0.1" 6570
    getExecutorsAddr ((ExecutorKindHStream, addr) : _) = toSocketAddr addr
    getExecutorsAddr (_ : xs) = getExecutorsAddr xs
    toSocketAddr :: T.Text -> SocketAddr
    toSocketAddr addr = case T.splitOn ":" addr of
      [host, port] -> SocketAddr (T.encodeUtf8 host) (read $ T.unpack port)
      [host]       -> SocketAddr (T.encodeUtf8 host) 6570
      _            -> error "invalid server url for ExecutorKindHStream"
    createByName :: Bool -> T.Text -> HStreamCliContext -> IO ()
    createByName debug streamName conn = do
      when debug $ do
        putStrLn $ "[DEBUG]: ExecutorHStream create stream " <> T.unpack streamName
      execute_ conn $ createStream streamName 1 (30 * 60)
