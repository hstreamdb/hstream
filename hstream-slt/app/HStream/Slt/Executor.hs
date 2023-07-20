module Slt.Executor where

import qualified Data.Text as T
import           Slt.Utils

class SltExecutor a where
  open :: IO a
  insertValues :: Kv -> a -> IO ()
  selectWithoutFrom :: T.Text -> a -> IO Kv
  sqlDataTypeToText :: SqlDataType -> a -> T.Text
