module Slt.Executor.HStream where

import Slt.Executor

data HStreamExecutor

instance SltExecutor (ExecutorM HStreamExecutor) where
  open = undefined
  selectWithoutFrom = undefined
  insertValues = undefined
  sqlDataTypeToLiteral' = undefined
  sqlDataValueToLiteral = undefined
