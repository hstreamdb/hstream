module HStream.Slt.Executor.HStream where

import           Control.Monad.IO.Class
import           Control.Monad.State
import qualified Data.Text                 as T
import qualified Data.Text.Encoding        as T
import qualified Data.Text.IO              as T
import qualified Data.Vector               as V
import qualified HStream.Client.Action     as S
import qualified HStream.Client.Execute    as S
import           HStream.Client.Types
import qualified HStream.Server.HStreamApi as API
import           HStream.Slt.Cli.Parser
import           HStream.Slt.Executor
import           HStream.Utils

newtype HStreamExecutor = HStreamExecutor
  { conn :: HStreamCliContext
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
  conn <- liftIO $ S.initCliContext $ refineOpts opts
  pure $ HStreamExecutor conn
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

----------------------------------------

getConn :: HStreamExecutorM HStreamCliContext
getConn = conn <$> getExecutor

execute_ ::
  Format a =>
  T.Text ->
  S.Action a ->
  HStreamExecutorM ()
execute_ msg xs = do
  debug <- isDebug
  if debug
    then liftIO $ T.putStrLn $ "[DEBUG]: " <> msg
    else do
      conn <- getConn
      liftIO $ S.execute_ conn xs

executeWithLookupResource ::
  Format a =>
  Resource ->
  (HStreamClientApi -> IO a) ->
  HStreamExecutorM a
executeWithLookupResource res xs = do
  conn <- getConn
  liftIO $ S.executeWithLookupResource conn res xs

executeViewQuery ::
  T.Text ->
  T.Text ->
  T.Text ->
  HStreamExecutorM API.ExecuteViewQueryResponse
executeViewQuery msg res sql = do
  debug <- isDebug
  if debug
    then do
      liftIO $ T.putStrLn $ "[DEBUG]: " <> msg
      pure $ API.ExecuteViewQueryResponse V.empty
    else do
      x <- executeWithLookupResource (Resource ResView res) $ S.executeViewQuery $ T.unpack sql
      liftIO $ getServerResp x

createStream :: T.Text -> HStreamExecutorM ()
createStream streamName = do
  execute_
    ("ExecutorHStream create stream " <> streamName)
    $ S.createStream streamName 1 (30 * 60)

createView :: T.Text -> HStreamExecutorM ()
createView sql = do
  qName <- ("slt_generated_" <>) <$> newRandomText 10
  S.executeWithLookupResource_ conn (Resource ResQuery qName) (createStreamBySelectWithCustomQueryName sql qName)
