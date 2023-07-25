module Slt.Format.Sql where

import Control.Applicative
import Data.Text qualified as T
import Slt.Format
import Text.Megaparsec qualified as P

newtype SelectNoTable = SelectNoTable
  { selectNoTableItems :: [SelectNoTableItem]
  }
  deriving (Show)

data SelectNoTableItem
  = SelectNoTableItemColName T.Text
  | SelectNoTableItemFnApp T.Text [T.Text]
  deriving (Show)

pMetaCol :: Parser T.Text
pMetaCol = P.satisfy (== '$') *> pColIdent

pFnApp :: Parser (T.Text, [T.Text])
pFnApp = do
  fn <- lexeme pColIdent
  -- FIXME: nested
  args <- P.between (symbol "(") (symbol ")") (pArgList <|> pure [])
  pure (fn, args)
  where
    pArgList = lexeme $ pArg `P.sepBy` symbol ","
    pArg = lexeme pMetaCol

pSelectNoTableItem, pSelectNoTableItemColName, pSelectNoTableItemFnApp :: Parser SelectNoTableItem
pSelectNoTableItem = pSelectNoTableItemColName <|> pSelectNoTableItemFnApp
pSelectNoTableItemColName = SelectNoTableItemColName <$> pMetaCol
pSelectNoTableItemFnApp = uncurry SelectNoTableItemFnApp <$> pFnApp

pSelectNoTable :: Parser SelectNoTable
pSelectNoTable =
  SelectNoTable <$> do
    _ <- symbol' "SELECT"
    pSelectNoTableItem `P.sepBy` symbol ","
