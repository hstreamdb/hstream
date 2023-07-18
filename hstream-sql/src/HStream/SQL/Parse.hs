{-# LANGUAGE CPP #-}

-- This module is only compiled when 'hstream_enable_schema' is disabled.
-- If enabled, use HStream.SQL.ParseNew instead.
module HStream.SQL.Parse where

#ifndef HStreamEnableSchema
import           Control.Exception             (throw)
import           Data.Functor                  ((<&>))
import           Data.Text                     (Text)
import           GHC.Stack                     (HasCallStack)
import           HStream.SQL.Abs               (SQL)
import           HStream.SQL.AST               (RSQL, Refine (refine))
import           HStream.SQL.Exception         (SomeSQLException (..),
                                                throwSQLException)
import           HStream.SQL.Internal.Check    (Check (check))
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

parseAndRefine :: HasCallStack => Text -> IO RSQL
parseAndRefine input = do
  rsql <- parse input <&> refine
  case check rsql of Left e -> throw e; Right _ -> return rsql
#endif
