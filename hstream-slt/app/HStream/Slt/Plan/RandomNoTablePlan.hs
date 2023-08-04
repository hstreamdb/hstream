module HStream.Slt.Plan.RandomNoTablePlan where

import           Control.Monad
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Data.Maybe             (fromJust, fromMaybe)
import qualified Data.Text              as T
import qualified Data.Text.IO           as T
import           HStream.Slt.Executor
import           HStream.Slt.Format.Sql
import           HStream.Slt.Plan
import           HStream.Slt.Utils
import qualified Text.Megaparsec        as P

defaultRowNum :: Int
defaultRowNum = 200

evalRandomNoTablePlan :: SltExecutor m executor => RandomNoTablePlan -> m executor [Kv]
evalRandomNoTablePlan RandomNoTablePlan {colInfo = ColInfo info, rowNum, sql} = do
  xs <- forM [0 .. fromMaybe defaultRowNum rowNum] $ \_ -> do
    values <- randInstantiateSelectWithoutFromSql info sql
    selectWithoutFrom values
  debug <- isDebug
  when debug $ do
    sqls <- getSql
    liftIO $ do
      putStrLn "[DEBUG]: begin show SQLs"
      forM_ sqls $ \x -> do
        T.putStrLn x
      putStrLn "[DEBUG]: end show SQLs"
  clearSql
  pure xs

randInstantiateSelectWithoutFromSql :: SltExecutor m executor => [(T.Text, SqlDataType)] -> T.Text -> m executor [T.Text]
randInstantiateSelectWithoutFromSql info sql = do
  -- FIXME: check consistency of info
  let select = case P.parse pSelectNoTable mempty sql of
        Right select' -> select'
        Left err      -> error $ show err
  randInstantiate select
  where
    randInstantiate :: SltExecutor m executor => SelectNoTable -> m executor [T.Text]
    randInstantiate SelectNoTable {selectNoTableItems} = do
      values <- genAllMeta
      let lookupValue x = sqlDataValueToLiteral (fromJust $ lookup x values)
      forM selectNoTableItems $ \case
        SelectNoTableItemColName x -> lookupValue x
        SelectNoTableItemFnApp f xs -> do
          xs' <- mapM lookupValue xs
          pure $ f <> "(" <> T.intercalate ", " xs' <> ")"

    genAllMeta :: SltExecutor m executor => m executor [(T.Text, SqlDataValue)]
    genAllMeta =
      mapM
        ( \(x, typ) -> do
            v <- liftIO $ randSqlDataValue typ
            pure (x, v)
        )
        info