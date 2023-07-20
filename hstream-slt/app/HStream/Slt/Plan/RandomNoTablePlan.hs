module Slt.Plan.RandomNoTablePlan where

import           Control.Monad
import           Data.Function
import           Data.Maybe    (fromMaybe)
import qualified Data.Text     as T
import           Slt.Executor
import           Slt.Plan
import           Slt.Utils

defaultRowNum :: Int
defaultRowNum = 200

evalRandomNoTablePlan :: SltExecutor executor => executor -> RandomNoTablePlan -> IO [Kv]
evalRandomNoTablePlan executor RandomNoTablePlan {colInfo = ColInfo info, rowNum, sql} = do
  forM [0 .. fromMaybe defaultRowNum rowNum] $ \_ -> do
    executor & selectWithoutFrom (randInstantiateSelectWithoutFromSql info sql)

randInstantiateSelectWithoutFromSql :: [(T.Text, SqlDataType)] -> T.Text -> T.Text
randInstantiateSelectWithoutFromSql = undefined
