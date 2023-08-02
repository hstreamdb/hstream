{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE RecordWildCards      #-}
{-# LANGUAGE ScopedTypeVariables  #-}
{-# LANGUAGE TypeFamilies         #-}
{-# LANGUAGE TypeSynonymInstances #-}

module HStream.SQL.PlannerNew.Expr where

import           Control.Monad.State
import qualified Data.List                    as L
import           Data.List.Extra              (allSame)
import qualified Data.Set                     as Set
import qualified Data.Text                    as T

import           HStream.SQL.Binder           hiding (lookupColumn)
import           HStream.SQL.Exception
import           HStream.SQL.PlannerNew.Types

type instance PlannedType (Aggregate BoundExpr) = (Aggregate ScalarExpr, ColumnCatalog)
instance Plan (Aggregate BoundExpr) where
  plan agg = case agg of
    Nullary nullary -> do
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack $ show agg
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeInteger
                                  -- FIXME: if we have more nullary aggs other than `count(*)`,
                                  --        it may not be `integer`.
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
      let types = map (columnType . snd) tups
      let elemType = case allSame types of
            False -> throwSQLException PlanException Nothing $
              "Array elements should have the same type: " <> show es
            True  -> if L.null types then BTypeInteger -- FIXME: type for empty array?
                                          else L.head types
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = BTypeArray elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (ValueArray (map fst tups), catalog)
    BoundExprAccessArray name e rhs -> do
      (e', c') <- plan e
      let elemType = case columnType c' of
            BTypeArray typ -> typ
            _              -> throwSQLException PlanException Nothing $
              "Accessing array element from non-array type: " <> show c'
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (AccessArray e' rhs, catalog)
    BoundExprCol name stream col colId -> do
      ctx <- get
      case lookupColumn ctx stream col of
        Nothing -> throwSQLException PlanException Nothing $ "column " <> T.unpack stream <> ".#" <> show colId <> " not found in ctx " <> show ctx
        Just (_,columnId,catalog) ->
          return (ColumnRef (columnStreamId catalog) columnId, catalog)
    BoundExprConst name constant -> do
      let elemType = nResType constant
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (Literal constant, catalog)
    BoundExprAggregate name agg  -> do
      ctx <- get
      case lookupColumnName ctx (T.pack name) of
        Nothing -> throwSQLException PlanException Nothing $ "column " <> name <> " not found in ctx " <> show ctx
        Just (_,columnId,catalog) ->
          return (ColumnRef (columnStreamId catalog) columnId, catalog)
    BoundExprAccessJson name op e1 e2 -> do
      (e1', c1') <- plan e1
      (e2', c2') <- plan e2
      let elemType = if columnType c1' `Set.member` bOp1Type op &&
                        columnType c2' `Set.member` bOp2Type op
                        then bResType op (columnType c1') (columnType c2') else
                        throwSQLException PlanException Nothing $
                        "Using JSON operator on incorrect type: " <> show e1 <> show op <> show e2
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallJson op e1' e2', catalog)
    BoundExprBinOp name op e1 e2      -> do
      (e1', c1') <- plan e1
      (e2', c2') <- plan e2
      let elemType = if columnType c1' `Set.member` bOp1Type op &&
                        columnType c2' `Set.member` bOp2Type op
                        then bResType op (columnType c1') (columnType c2') else
                        throwSQLException PlanException Nothing $
                        "Using binary operator on incorrect type: " <> show e1 <> show op <> show e2
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallBinary op e1' e2', catalog)
    BoundExprUnaryOp name op e        -> do
      (e', c') <- plan e
      let elemType = if columnType c' `Set.member` uOpType op
                        then uResType op (columnType c') else
                        throwSQLException PlanException Nothing $
                        "Using unary operator on incorrect type: " <> show op <> show e
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallUnary op e', catalog)
    BoundExprTerOp name op e1 e2 e3   -> do
      (e1', c1') <- plan e1
      (e2', c2') <- plan e2
      (e3', c3') <- plan e3
      let elemType = if columnType c1' `Set.member` tOp1Type op &&
                        columnType c2' `Set.member` tOp2Type op &&
                        columnType c3' `Set.member` tOp3Type op
                        then tResType op (columnType c1') (columnType c2') (columnType c3') else
                        throwSQLException PlanException Nothing $
                        "Using ternary operator on incorrect type: " <> show e1 <> show op <> show e2 <> show e3
      let catalog = ColumnCatalog { columnId = 0
                                  , columnName = T.pack name
                                  , columnStreamId = 0
                                  , columnStream = ""
                                  , columnType = elemType
                                  , columnIsNullable = True
                                  , columnIsHidden = False
                                  }
      return (CallTernary op e1' e2' e3', catalog)
