{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.SQL.Parse
  ( parse
  , parseAndRefine
  ) where

import           Control.Exception             (throw, try)
import           Data.Functor                  ((<&>))
import           Data.Text                     (Text)
import           GHC.Stack                     (HasCallStack)
import           HStream.SQL.Abs               (SQL)
import           HStream.SQL.AST               (RSQL, Refine (refine))
import           HStream.SQL.Exception         (SomeSQLException (..),
                                                getSQLExceptionInfo,
                                                throwSQLException)
import           HStream.SQL.Internal.Validate (Validate (validate))
import           HStream.SQL.Lex               (tokens)
import           HStream.SQL.Par               (pSQL)
import           HStream.SQL.Preprocess        (preprocess)

parse :: HasCallStack => Text -> IO SQL
parse input = do
  let sql' = pSQL . tokens . preprocess $ input
  case sql' of
    Left err  -> do
      throwSQLException ParseException Nothing err
    Right sql ->
      case validate sql of
        Left exception -> throw exception
        Right vsql     -> return vsql

parseAndRefine' :: HasCallStack => Text -> IO RSQL
parseAndRefine' input = parse input <&> refine

parseAndRefine :: HasCallStack => Text -> IO RSQL
parseAndRefine input = do
  xs <- try @SomeSQLException $ parseAndRefine' input
  case xs of
    Right xs -> pure xs
    Left err -> do
      xs <- try @SomeSQLException $ parseAndRefine' $ input <> " ;"
      case xs of
        Right xs -> throw . UnclosedStmtException $ getSQLExceptionInfo err
        Left err -> throw err
