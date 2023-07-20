{-# LANGUAGE CPP #-}

-- This module is only compiled when 'hstream_enable_schema' is disabled.
module HStream.SQL.Internal.Check where

#ifndef HStreamEnableSchema
import           GHC.Stack             (HasCallStack)

import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeSQLException (..),
                                        buildSQLException)

class Check a where
  check :: HasCallStack => a -> Either SomeSQLException ()

instance Check RSelect where
  check (RSelect (RSel sels) _RFrom _RWhere (RGroupBy groupBys _) _RHaving) = mapM_ (checkGroupBySel groupBys) sels
  check _ = Right ()

checkGroupBySel :: Foldable t
                => t (Maybe StreamName, FieldName)
                -> RSelectItem
                -> Either SomeSQLException ()
checkGroupBySel gbs (RSelectItemProject expr _)  = checkGroupBySelExpr gbs expr
  where
    checkGroupBySelExpr gbs (RExprCast        _ expr _RDataType)       = checkGroupBySelExpr gbs expr
    checkGroupBySelExpr gbs (RExprArray       _ exprs)                 = mapM_ (checkGroupBySelExpr gbs) exprs
    checkGroupBySelExpr gbs (RExprAccessArray _ expr _RArrayAccessRhs) = checkGroupBySelExpr gbs expr
    checkGroupBySelExpr gbs (RExprCol         _ maybeStreamName name) =
      if (maybeStreamName,name) `elem` gbs then Right () else Left $ buildSQLException ParseException Nothing "Select item does not appear in the GROUP BY clause"
    checkGroupBySelExpr _   (RExprAggregate   _ _)                     = Right ()
    checkGroupBySelExpr gbs (RExprAccessJson  _ _JsonOp expr _expr)    = checkGroupBySelExpr gbs expr
    checkGroupBySelExpr gbs (RExprBinOp       _ _BinaryOp expr1 expr2) = checkGroupBySelExpr gbs expr1 >> checkGroupBySelExpr gbs expr2
    checkGroupBySelExpr gbs (RExprUnaryOp     _ _UnaryOp  expr)        = checkGroupBySelExpr gbs expr
    checkGroupBySelExpr _   (RExprConst       _ _)                     = Right ()
    checkGroupBySelExpr _ _ = Left $ buildSQLException ParseException Nothing "Invalid GROUP BY clause"
checkGroupBySel _ RSelectProjectQualifiedAll{} = Left $ buildSQLException ParseException Nothing "GROUP BY clause does not allow wildcards"
checkGroupBySel _ RSelectProjectAll            = Left $ buildSQLException ParseException Nothing "GROUP BY clause does not allow wildcards"


instance Check RSQL where
  check (RQSelect rselect) = check rselect
  check _                  = Right ()
#endif
