module HStream.SQL.Parse
  ( parse
  , parseAndRefine
  ) where

import           Control.Exception      (throw)
import           Data.Functor           ((<&>))
import           Data.Text              (Text)
import           GHC.Stack              (HasCallStack)
import           HStream.SQL.AST        (RSQL, Refine (refine))
import           HStream.SQL.Abs        (SQL)
import           HStream.SQL.Exception  (Position, SomeSQLException (..),
                                         throwSQLException)
import           HStream.SQL.Lex        (tokens)
import           HStream.SQL.Par        (pSQL)
import           HStream.SQL.Preprocess (preprocess)
import           HStream.SQL.Validate   (Validate (validate))

parse :: HasCallStack => Text -> IO SQL
parse input = do
  let sql' = pSQL . tokens . preprocess $ input
  case sql' of
    Left err  -> throwSQLException ParseException Nothing err
    Right sql ->
      case validate sql of
        Left err   -> throw err
        Right vsql -> return vsql

parseAndRefine :: HasCallStack => Text -> IO RSQL
parseAndRefine input = parse input <&> refine
