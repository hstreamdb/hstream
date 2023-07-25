module Slt.Plan.RandomNoTablePlan where

import Control.Monad
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Maybe (fromJust, fromMaybe)
import Data.Text qualified as T
import Slt.Executor
import Slt.Format.Sql
import Slt.Plan
import Slt.Utils
import Text.Megaparsec qualified as P

defaultRowNum :: Int
defaultRowNum = 200

evalRandomNoTablePlan :: SltExecutor m => RandomNoTablePlan -> m [Kv]
evalRandomNoTablePlan RandomNoTablePlan {colInfo = ColInfo info, rowNum, sql} = do
  forM [0 .. fromMaybe defaultRowNum rowNum] $ \_ -> do
    values <- randInstantiateSelectWithoutFromSql info sql
    selectWithoutFrom values

randInstantiateSelectWithoutFromSql :: SltExecutor m => [(T.Text, SqlDataType)] -> T.Text -> m [T.Text]
randInstantiateSelectWithoutFromSql info sql = do
  -- FIXME: check consistency of info
  let select = case P.parse pSelectNoTable mempty sql of
        Right select' -> select'
        Left err -> error $ show err
  randInstantiate select
  where
    randInstantiate :: SltExecutor m => SelectNoTable -> m [T.Text]
    randInstantiate SelectNoTable {selectNoTableItems} = do
      values <- genAllMeta
      let lookupValue x = sqlDataValueToLiteral (fromJust $ lookup x values)
      forM selectNoTableItems $ \case
        SelectNoTableItemColName x -> lookupValue x
        SelectNoTableItemFnApp f xs -> do
          xs' <- mapM lookupValue xs
          pure $ f <> "(" <> T.intercalate ", " xs' <> ")"

    genAllMeta :: SltExecutor m => m [(T.Text, SqlDataValue)]
    genAllMeta =
      mapM
        ( \(x, typ) -> do
            v <- liftIO $ randSqlDataValue typ
            pure (x, v)
        )
        info
