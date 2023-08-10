{-# LANGUAGE CPP #-}

module HStream.Slt.Executor.HStream where

import           Control.Monad.IO.Class
import           Control.Monad.State
import qualified Data.Aeson                as A
import           Data.Functor
import           Data.Maybe
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
import           HStream.Slt.Plan
import           HStream.Slt.Utils
import           HStream.Utils

#ifndef HStreamEnableSchema
#ifndef HStreamUseV2Engine
import           HStream.SQL.Codegen.V1
#endif
#endif

#ifdef HStreamEnableSchema
#ifndef HStreamUseV2Engine
import           HStream.SQL.Codegen.V1New
#endif
#endif

#ifdef HStreamUseV2Engine
import           HStream.SQL.Codegen.V2
#endif

newtype HStreamExecutor = HStreamExecutor
  { conn :: Maybe HStreamCliContext
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
  selectWithoutFrom = selectWithoutFrom'
  insertValues = undefined
  sqlDataTypeToLiteral' = undefined
  sqlDataValueToLiteral = pure . T.pack . show

----------------------------------------

open'' :: HStreamExecutorM HStreamExecutor
open'' = do
  opts <- getOpts
  debug <- isDebug
  if debug
    then pure $ HStreamExecutor Nothing
    else do
      conn <- liftIO $ S.initCliContext $ refineOpts opts
      pure $ HStreamExecutor $ Just conn
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

selectWithoutFrom' :: ColInfo -> [T.Text] -> HStreamExecutorM Kv
selectWithoutFrom' colInfo cols = do
  streamName <- newRandText
  createStream streamName
  viewName <- newRandText
  createView $ buildCreateViewStmt streamName viewName $ buildSelItemsStmt colInfo
  -- undefined
  -- pure $ undefined
  deleteStream streamName
  pure mempty

----------------------------------------

getConn :: HStreamExecutorM HStreamCliContext
getConn = fromJust . conn <$> getExecutor

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
  HStreamExecutorM (V.Vector A.Object)
executeViewQuery msg res sql = do
  debug <- isDebug
  if debug
    then do
      liftIO $ T.putStrLn $ "[DEBUG]: " <> msg
      pure V.empty
    else do
      x <- executeWithLookupResource (Resource ResView res) $ S.executeViewQuery $ T.unpack sql
      API.ExecuteViewQueryResponse resp <- liftIO $ getServerResp x
      let ret = V.map structToJsonObject resp
      liftIO $ putStrLn $ "[DEBUG]: executeViewQuery " <> show ret
      pure ret

createStream :: T.Text -> HStreamExecutorM ()
createStream streamName = do
  execute_
    ("ExecutorHStream create stream " <> streamName)
    $ S.createStream streamName 1 (30 * 60)

createView :: T.Text -> HStreamExecutorM ()
createView sql = do
  qName <- newRandText
  debug <- isDebug
  if debug
    then liftIO $ putStrLn $ "[DEBUG]: createView " <> T.unpack sql <> " " <> T.unpack qName
    else do
      conn <- getConn
      liftIO $ S.executeWithLookupResource_ conn (Resource ResQuery qName) $ S.createStreamBySelectWithCustomQueryName (T.unpack sql) qName

buildSelItemsStmt :: ColInfo -> T.Text
buildSelItemsStmt (ColInfo colInfo) =
  let names = colInfo <&> fst
   in T.intercalate ", " names

buildCreateViewStmt :: T.Text -> T.Text -> T.Text -> T.Text
buildCreateViewStmt streamName viewName selItems =
  "CREATE VIEW "
    <> viewName
    <> " AS SELECT "
    <> selItems
    <> " FROM "
    <> streamName
    <> " GROUP BY "
    <> internalIndex
    <> " ;"

deleteStream :: T.Text -> HStreamExecutorM ()
deleteStream streamName = do
  debug <- isDebug
  if debug
    then liftIO . putStrLn $ "[DEBUG]: deleteStream " <> T.unpack streamName
    else do
      conn <- getConn
      liftIO $ S.executeWithLookupResource_ conn (Resource ResStream streamName) $ S.deleteStream streamName True

deleteView :: T.Text -> HStreamExecutorM ()
deleteView viewName = do
  debug <- isDebug
  if debug
    then liftIO . putStrLn $ "[DEBUG]: deleteView " <> T.unpack viewName
    else do
      conn <- getConn
      liftIO $ S.executeWithLookupResource_ conn (Resource ResView viewName) $ S.dropAction True $ DView viewName

----------------------------------------

internalIndex :: T.Text
internalIndex = "iiiiiiiiiiiiii"

newRandText :: HStreamExecutorM T.Text
newRandText = fmap ("slt_generated_" <>) <$> liftIO $ newRandomText 10
