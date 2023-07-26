{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.PlannerNew.Expr where

import           Control.Applicative           ((<|>))
import           Control.Monad.State
import           Data.Function                 (on)
import           Data.Functor                  ((<&>))
import           Data.Int                      (Int64)
import           Data.IntMap                   (IntMap)
import qualified Data.IntMap                   as IntMap
import           Data.Kind                     (Type)
import qualified Data.List                     as L
import qualified Data.Map                      as Map
import           Data.Maybe                    (fromJust, fromMaybe)
import           Data.Text                     (Text)
import qualified Data.Text                     as T
import           GHC.Stack

import           HStream.SQL.Binder            hiding (lookupColumn)
import           HStream.SQL.Exception
import           HStream.SQL.PlannerNew.Extra
import           HStream.SQL.PlannerNew.Types

import           HStream.SQL.PlannerNew.Pretty

type instance PlannedType (Aggregate BoundExpr) = (Aggregate ScalarExpr, ColumnCatalog)
instance Plan (Aggregate BoundExpr) where
  plan agg = case agg of
    Nullary nullary -> do
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack $ show agg
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (Nullary nullary, catalog)
    Unary op e      -> do
      (e',c') <- plan e
      let catalog = c' { columnName = T.pack $ show agg } -- FIXME
      return (Unary op e', catalog)
    Binary op e1 e2 -> do
      (e1',c1') <- plan e1
      (e2',_) <- plan e2
      let catalog = c1' { columnName = T.pack $ show agg } -- FIXME
      return (Binary op e1' e2', catalog)

type instance PlannedType BoundExpr = (ScalarExpr, ColumnCatalog)
instance Plan BoundExpr where
  plan expr = case expr of
    BoundExprCast name e typ -> do
      (e', c') <- plan e
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = typ
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallCast e' typ, catalog)
    BoundExprArray name es -> do
      tups <- mapM plan es
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (ValueArray (map fst tups), catalog)
    BoundExprAccessArray name e rhs -> do
      (e', c') <- plan e
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (AccessArray e' rhs, catalog)
    BoundExprCol name stream col colId -> do
      ctx <- get
      case lookupColumn ctx stream colId of
        Nothing -> throwSQLException PlanException Nothing $ "column " <> T.unpack stream <> ".#" <> show colId <> " not found in ctx " <> show ctx
        Just (streamId,columnId,catalog) ->
          return (ColumnRef streamId columnId, catalog)
    BoundExprConst name constant -> do
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (Literal constant, catalog)
    BoundExprAggregate name agg  -> do
      ctx <- get
      case lookupColumnName ctx (T.pack name) of
        Nothing -> throwSQLException PlanException Nothing $ "column " <> name <> " not found in ctx " <> show ctx
        Just (streamId,columnId,catalog) ->
          return (ColumnRef streamId columnId, catalog)
    BoundExprAccessJson name op e1 e2 -> do
      (e1', c1') <- plan e1
      (e2', c2') <- plan e2
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallJson op e1' e2', catalog)
    BoundExprBinOp name op e1 e2      -> do
      (e1', c1') <- plan e1
      (e2', c2') <- plan e2
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallBinary op e1' e2', catalog)
    BoundExprUnaryOp name op e        -> do
      (e', c') <- plan e
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallUnary op e', catalog)
    BoundExprTerOp name op e1 e2 e3   -> do
      (e1', c1') <- plan e1
      (e2', c2') <- plan e2
      (e3', c3') <- plan e3
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger -- FIXME!!!
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallTernary op e1' e2' e3', catalog)
