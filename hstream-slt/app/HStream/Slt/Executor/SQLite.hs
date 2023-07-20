module Slt.Executor.SQLite where

import qualified Database.SQLite.Simple as S
import           Slt.Executor

newtype SQLiteExecutor = SQLiteExecutor
  { connection :: S.Connection
  }

instance SltExecutor SQLiteExecutor where
  open = SQLiteExecutor <$> S.open []
  selectWithoutFrom = undefined
  insertValues = undefined
  sqlDataTypeToText = undefined
