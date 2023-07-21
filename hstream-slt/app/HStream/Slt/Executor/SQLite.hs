module Slt.Executor.SQLite (SQLiteExecutor (..)) where

import Data.Text qualified as T
import Database.SQLite.Simple qualified as S
import Slt.Executor
import Slt.Utils

newtype SQLiteExecutor = SQLiteExecutor
  { connection :: S.Connection
  }

instance SltExecutor SQLiteExecutor where
  open = SQLiteExecutor <$> S.open []
  selectWithoutFrom = selectWithoutFrom'
  insertValues = insertValues'
  sqlDataTypeToLiteral = undefined
  sqlDataValueToLiteral = undefined

----------------------------------------

selectWithoutFrom' :: [T.Text] -> SQLiteExecutor -> IO Kv
selectWithoutFrom' cols SQLiteExecutor {connection} = do
  xss <- S.query_ @[SqlDataValue] connection (buildselectWithoutFromStmt cols)
  pure . sqlDataValuesToKv $ zip cols (head xss)

buildselectWithoutFromStmt :: [T.Text] -> S.Query
buildselectWithoutFromStmt cols = S.Query undefined

----------------------------------------

insertValues' :: T.Text -> Kv -> SQLiteExecutor -> IO ()
insertValues' table kv executor@SQLiteExecutor {connection} = do
  S.execute_ connection . S.Query $
    "INSERT INTO " <> table <> " VALUES " <> buildValues kv executor
