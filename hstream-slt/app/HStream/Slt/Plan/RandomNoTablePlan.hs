module Slt.Plan.RandomNoTablePlan where

import           Control.Monad
import           Data.Function
import           Data.Functor
import           Data.Maybe      (fromJust, fromMaybe)
import qualified Data.Text       as T
import           Slt.Executor
import           Slt.Format.Sql
import           Slt.Plan
import           Slt.Utils
import qualified Text.Megaparsec as P

defaultRowNum :: Int
defaultRowNum = 200

evalRandomNoTablePlan :: SltExecutor executor => executor -> RandomNoTablePlan -> IO [Kv]
evalRandomNoTablePlan executor RandomNoTablePlan {colInfo = ColInfo info, rowNum, sql} = do
  forM [0 .. fromMaybe defaultRowNum rowNum] $ \_ -> do
    values <- randInstantiateSelectWithoutFromSql executor info sql
    executor & selectWithoutFrom values

randInstantiateSelectWithoutFromSql :: SltExecutor executor => executor -> [(T.Text, SqlDataType)] -> T.Text -> IO [T.Text]
randInstantiateSelectWithoutFromSql executor info sql = do
  -- FIXME: check consistency of info
  let select = case P.parse pSelectNoTable mempty sql of
        Right select' -> select'
        Left err      -> error $ show err
  randInstantiate select
  where
    randInstantiate :: SelectNoTable -> IO [T.Text]
    randInstantiate SelectNoTable {selectNoTableItems} = do
      values <- genAllMeta
      let lookupValue x = executor & sqlDataValueToLiteral (fromJust $ lookup x values)
      pure $
        selectNoTableItems <&> \case
          SelectNoTableItemColName x -> lookupValue x
          SelectNoTableItemFnApp f xs ->
            let xs' = lookupValue <$> xs
             in f <> "(" <> T.intercalate ", " xs' <> ")"

    genAllMeta :: IO [(T.Text, SqlDataValue)]
    genAllMeta =
      mapM
        ( \(x, typ) -> do
            v <- randSqlDataValue typ
            pure (x, v)
        )
        info
