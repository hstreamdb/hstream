module HStream.SQL.Parse
  ( parse
  , parseAndRefine
  ) where

import           Data.Functor           ((<&>))
import           Data.Text              (Text)
import           HStream.SQL.AST
import           HStream.SQL.Abs
import           HStream.SQL.Lex        (tokens)
import           HStream.SQL.Par        (pSQL)
import           HStream.SQL.Preprocess
import           HStream.SQL.Validate

parse :: Text -> Either String (SQL Position)
parse input = (pSQL . tokens . preprocess $ input) >>= validate

parseAndRefine :: Text -> Either String RSQL
parseAndRefine input = parse input <&> refine
