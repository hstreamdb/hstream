{-# LANGUAGE CPP #-}

-- This module is only compiled when 'hstream_enable_schema' is enabled.
-- If disabled, use HStream.SQL.Parse instead.
module HStream.SQL.ParseNew where

#ifdef HStreamEnableSchema
import           Control.Exception             (throw)
import           Control.Monad.Reader
import           Control.Monad.State
import           Data.Functor                  ((<&>))
import           Data.Text                     (Text)
import           GHC.Stack                     (HasCallStack)
import           HStream.SQL.Abs               (SQL)
import           HStream.SQL.Binder
import           HStream.SQL.Exception         (SomeSQLException (..),
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

parseAndBind :: HasCallStack => Text -> (Text -> IO (Maybe Schema)) -> IO BoundSQL
parseAndBind input getSchema = do
  sql <- parse input
  boundSql <- evalStateT (runReaderT (bind sql) getSchema) defaultBindContext
  return boundSql
#endif
