module Slt.Format where

import           Data.Char                  (isSpace)
import           Data.Foldable
import           Data.Functor
import qualified Data.Text                  as T
import           Data.Void                  (Void)
import           Slt.Utils
import qualified Text.Megaparsec            as P
import           Text.Megaparsec            (many, some, (<|>))
import qualified Text.Megaparsec.Char       as C
import qualified Text.Megaparsec.Char.Lexer as L

----------------------------------------
-- Common Parser Utils
----------------------------------------

type Parser = P.Parsec Void T.Text

scn :: Parser ()
scn =
  L.space
    C.space1
    (L.skipLineComment "#")
    (L.skipBlockComment "/*" "*/")

sc :: Parser ()
sc =
  L.space
    (P.satisfy isSpaceButNotNewline $> ())
    (L.skipLineComment "#")
    (L.skipBlockComment "/*" "*/")

lexeme :: Parser a -> Parser a
lexeme = L.lexeme sc

symbol :: T.Text -> Parser T.Text
symbol = L.symbol sc

takeToNewline1 :: Parser T.Text
takeToNewline1 = P.takeWhile1P Nothing (/= '\n') <* scn

requireNewline :: Parser ()
requireNewline = C.newline *> scn $> ()

----------------------------------------
-- Slt Parser Utils
----------------------------------------

pColIdent :: Parser T.Text
pColIdent = T.pack <$> do ((: []) <$> C.letterChar) <> many C.alphaNumChar

----------------------------------------

pTypeAnn :: Parser (T.Text, SqlDataType)
pTypeAnn = do
  x <- lexeme pColIdent
  _ <- symbol "::"
  a <- h
  pure (x, a)
  where
    h =
      foldl'
        (\acc x -> textToSqlDataType <$> P.try (symbol x) <|> acc)
        P.empty
        typeLiterals

pTypeInfo :: Parser [(T.Text, SqlDataType)]
pTypeInfo = many $ P.try (pTypeAnn <* requireNewline)

----------------------------------------

pIntOpt :: T.Text -> Parser Int
pIntOpt x = read @Int <$> do symbol x *> symbol "=" *> lexeme (some C.digitChar)

----------------------------------------
-- Misc
----------------------------------------

isSpaceButNotNewline :: Char -> Bool
isSpaceButNotNewline '\n' = False
isSpaceButNotNewline x    = isSpace x
