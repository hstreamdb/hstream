module Slt.Executor.SQLite (SQLiteExecutor (..)) where

import qualified Data.Text              as T
import qualified Database.SQLite.Simple as S
import           GHC.Stack
import           Slt.Executor
import           Slt.Utils

newtype SQLiteExecutor = SQLiteExecutor
  { connection :: S.Connection
  }

instance SltExecutor SQLiteExecutor where
  open = SQLiteExecutor <$> S.open []
  selectWithoutFrom = selectWithoutFrom'
  insertValues = insertValues'
  sqlDataTypeToLiteral' = sqlDataTypeToLiteral''
  sqlDataValueToLiteral value _ = sqlDataTypeToAnsiLiteral value

----------------------------------------

selectWithoutFrom' :: [T.Text] -> SQLiteExecutor -> IO Kv
selectWithoutFrom' cols SQLiteExecutor {connection} = do
  xss <- S.query_ @[SqlDataValue] connection (buildselectWithoutFromStmt cols)
  pure . sqlDataValuesToKv $ zip cols (head xss)

buildselectWithoutFromStmt :: [T.Text] -> S.Query
buildselectWithoutFromStmt cols =
  S.Query $
    "SELECT " <> T.intercalate ", " cols

----------------------------------------

insertValues' :: T.Text -> Kv -> SQLiteExecutor -> IO ()
insertValues' table kv executor@SQLiteExecutor {connection} = do
  S.execute_ connection . S.Query $
    "INSERT INTO " <> table <> buildValues kv executor

----------------------------------------

sqlDataTypeToLiteral'' :: SqlDataType -> SQLiteExecutor -> T.Text
sqlDataTypeToLiteral'' typ _ = case typ of
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
