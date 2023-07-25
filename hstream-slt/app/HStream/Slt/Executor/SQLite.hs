module Slt.Executor.SQLite (SQLiteExecutor (..)) where

import Control.Monad.IO.Class
import Control.Monad.State
import Data.Maybe
import Data.Text qualified as T
import Database.SQLite.Simple qualified as S
import GHC.Stack
import Slt.Executor
import Slt.Utils

newtype SQLiteExecutor = SQLiteExecutor
  { connection :: S.Connection
  }

type SQLiteExecutorM = ExecutorM SQLiteExecutor

getConn :: SQLiteExecutorM S.Connection
getConn = gets $ connection . fromJust . executorStateExecutor

instance SltExecutor (ExecutorM SQLiteExecutor) where
  open = do
    executor <- liftIO $ SQLiteExecutor <$> S.open []
    setExecutor executor
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
  INTEGER -> "INTEGER"
  FLOAT -> "REAL"
  BOOLEAN -> "NUMERIC"
  BYTEA -> "BLOB"
  STRING -> "TEXT"
  DATE -> throwSQLiteUnsupported
  TIME -> throwSQLiteUnsupported
  TIMESTAMP -> throwSQLiteUnsupported
  INTERVAL -> throwSQLiteUnsupported
  JSONB -> throwSQLiteUnsupported
  NULL -> "NULL"

throwSQLiteUnsupported :: HasCallStack => a
throwSQLiteUnsupported = error "SQLiteUnsupported"
