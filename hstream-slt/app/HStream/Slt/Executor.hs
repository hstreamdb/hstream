module Slt.Executor where

import Data.Text qualified as T
import Slt.Utils

class SltExecutor a where
  open :: IO a
  insertValues :: T.Text -> Kv -> a -> IO ()
  selectWithoutFrom :: [T.Text] -> a -> IO Kv
  sqlDataTypeToLiteral :: SqlDataType -> a -> T.Text
  sqlDataValueToLiteral :: SqlDataValue -> a -> T.Text

buildValues :: SltExecutor a => Kv -> a -> T.Text
buildValues = undefined
