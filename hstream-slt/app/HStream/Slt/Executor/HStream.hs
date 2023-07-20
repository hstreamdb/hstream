module Slt.Executor.HStream where

import           Slt.Executor

data HStreamExecutor

instance SltExecutor HStreamExecutor where
  open = undefined
  selectWithoutFrom = undefined
  insertValues = undefined
  sqlDataTypeToText = undefined
