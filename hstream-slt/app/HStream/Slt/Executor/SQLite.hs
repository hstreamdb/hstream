module Slt.Executor.SQLite (SQLiteExecutor (..)) where

import           Control.Monad.IO.Class
import           Control.Monad.State
import           Data.Maybe
import qualified Data.Text              as T
import qualified Database.SQLite.Simple as S
import           GHC.Stack
import           Slt.Executor
import           Slt.Utils

newtype SQLiteExecutor = SQLiteExecutor
  { connection :: S.Connection
  }

newtype SQLiteExecutorCtx executor a = SQLiteExecutorCtx {unSQLiteExecutorCtx :: ExecutorM executor a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadState (ExecutorState executor))

instance ExecutorCtx SQLiteExecutorCtx executor where
  setExecutor = SQLiteExecutorCtx . setExecutor
  isDebug = SQLiteExecutorCtx isDebug
  evalExecutorCtx = evalExecutorCtx . unSQLiteExecutorCtx
  setOpts = SQLiteExecutorCtx . setOpts
  getOpts = SQLiteExecutorCtx getOpts
  getExecutor = SQLiteExecutorCtx getExecutor

type SQLiteExecutorM = SQLiteExecutorCtx SQLiteExecutor

instance SltExecutor SQLiteExecutorCtx SQLiteExecutor where
  open' = liftIO $ SQLiteExecutor <$> S.open []
  selectWithoutFrom = selectWithoutFrom'
  insertValues = insertValues'
  sqlDataTypeToLiteral' = sqlDataTypeToLiteral''
  sqlDataValueToLiteral = pure . sqlDataTypeToAnsiLiteral

----------------------------------------

selectWithoutFrom' :: [T.Text] -> SQLiteExecutorM Kv
selectWithoutFrom' cols = do
  conn <- getConn
  xss <- liftIO $ S.query_ @[SqlDataValue] conn (buildselectWithoutFromStmt cols)
  pure . sqlDataValuesToKv $ zip cols (head xss)

buildselectWithoutFromStmt :: [T.Text] -> S.Query
buildselectWithoutFromStmt cols =
  S.Query $
    "SELECT " <> T.intercalate ", " cols

----------------------------------------

insertValues' :: T.Text -> Kv -> SQLiteExecutorM ()
insertValues' table kv = do
  conn <- getConn
  kv' <- buildValues kv
  liftIO $
    S.execute_ conn . S.Query $
      "INSERT INTO " <> table <> kv'

----------------------------------------

sqlDataTypeToLiteral'' :: SqlDataType -> SQLiteExecutorM T.Text
sqlDataTypeToLiteral'' typ = pure $ case typ of
  INTEGER   -> "INTEGER"
  FLOAT     -> "REAL"
  BOOLEAN   -> "NUMERIC"
  BYTEA     -> "BLOB"
  STRING    -> "TEXT"
  DATE      -> throwSQLiteUnsupported
  TIME      -> throwSQLiteUnsupported
  TIMESTAMP -> throwSQLiteUnsupported
  INTERVAL  -> throwSQLiteUnsupported
  JSONB     -> throwSQLiteUnsupported
  NULL      -> "NULL"

throwSQLiteUnsupported :: HasCallStack => a
throwSQLiteUnsupported = error "SQLiteUnsupported"

----------------------------------------

getConn :: SQLiteExecutorM S.Connection
getConn = gets $ connection . fromJust . executorStateExecutor
