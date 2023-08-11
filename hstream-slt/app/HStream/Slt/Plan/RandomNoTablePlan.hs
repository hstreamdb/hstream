module HStream.Slt.Plan.RandomNoTablePlan where

import           Control.Monad
import           Control.Monad.IO.Class (MonadIO (liftIO))
import           Data.Functor
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
    selectWithoutFrom (ColInfo info) sql
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

-- de facto
randInstantiateSelectWithoutFromSql :: SltExecutor m executor => ColInfo -> T.Text -> m executor [T.Text]
randInstantiateSelectWithoutFromSql (ColInfo info) sql = do
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

randInstantiateInsertIntoValuesSql :: SltExecutor m executor => ColInfo -> T.Text -> Int -> m executor T.Text
randInstantiateInsertIntoValuesSql (ColInfo info) tableName ix = do
  -- FIXME: check consistency of info
  allMeta <- genAllMeta
  values' <- values allMeta
  let names' = names allMeta
  pure $ "INSERT INTO " <> tableName <> " " <> names' <> " VALUES " <> values' <> " ;"
  where
    names :: [(T.Text, SqlDataValue)] -> T.Text
    names xs =
      let ys = fst <$> xs
       in "( " <> T.intercalate ", " (internalIndex : ys) <> " )"

    values :: SltExecutor m executor => [(T.Text, SqlDataValue)] -> m executor T.Text
    values xs = do
      let ys = snd <$> xs
      zs <- mapM sqlDataValueToLiteral ys
      pure $ "( " <> T.intercalate ", " (T.pack (show ix) : zs) <> " )"

    genAllMeta :: SltExecutor m executor => m executor [(T.Text, SqlDataValue)]
    genAllMeta =
      mapM
        ( \(x, typ) -> do
            v <- liftIO $ randSqlDataValue typ
            pure (x, v)
        )
        info

randInstantiateSelectFromSql :: SltExecutor m executor => T.Text -> T.Text -> Int -> m executor T.Text
randInstantiateSelectFromSql sql tableName ix = do
  -- FIXME: check consistency of info
  let select = case P.parse pSelectNoTable mempty sql of
        Right select' -> select'
        Left err      -> error $ show err
  pure $ "SELECT FROM " <> tableName <> " " <> T.intercalate ", " (cols select) <> " WHERE " <> internalIndex <> " = " <> T.pack (show ix) <> " ;"
  where
    cols :: SelectNoTable -> [T.Text]
    cols (SelectNoTable selectNoTableItems) =
      selectNoTableItems <&> \case
        SelectNoTableItemColName x  -> x
        SelectNoTableItemFnApp f xs -> f <> "(" <> T.intercalate ", " xs <> ")"
