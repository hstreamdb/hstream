module HStream.SQL.Internal.Check where

import qualified Data.Map.Strict       as Map
import           GHC.Stack             (HasCallStack)

import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeSQLException (..),
                                        buildSQLException)

class Check a where
  check :: HasCallStack => a -> Either SomeSQLException ()

instance Check RSelect where
  check (RSelect (RSel sels) _RFrom _RWhere (RGroupBy groupBys _) _RHaving) = mapM_ (checkGroupBySel groupBys) sels
  check _ = Right ()

checkGroupBySel gbs (RSelectItemProject expr _)  = checkGroupBySelExpr gbs expr
checkGroupBySel gbs RSelectProjectQualifiedAll{} = Left $ buildSQLException ParseException Nothing "GROUP BY clause does not allow wildcards"
checkGroupBySel gbs RSelectProjectAll            = Left $ buildSQLException ParseException Nothing "GROUP BY clause does not allow wildcards"

checkGroupBySelExpr gbs (RExprCast        _ expr _RDataType)       = checkGroupBySelExpr gbs expr
checkGroupBySelExpr gbs (RExprArray       _ exprs)                 = mapM_ (checkGroupBySelExpr gbs) exprs
checkGroupBySelExpr gbs (RExprMap         _ eMap)                  = mapM_ (checkGroupBySelExpr gbs) (Map.keys eMap)
checkGroupBySelExpr gbs (RExprAccessMap   _ expr _expr)            = checkGroupBySelExpr gbs expr
checkGroupBySelExpr gbs (RExprAccessArray _ expr _RArrayAccessRhs) = checkGroupBySelExpr gbs expr
checkGroupBySelExpr gbs (RExprCol         _ maybeStreamName name) =
  if (maybeStreamName,name) `elem` gbs then Right () else Left $ buildSQLException ParseException Nothing "Select item does not appear in the GROUP BY clause"
checkGroupBySelExpr gbs (RExprAggregate   _ _)                     = Right ()
checkGroupBySelExpr gbs (RExprAccessJson  _ _JsonOp expr _expr)    = checkGroupBySelExpr gbs expr
checkGroupBySelExpr gbs (RExprBinOp       _ _BinaryOp expr1 expr2) = checkGroupBySelExpr gbs expr1 >> checkGroupBySelExpr gbs expr2
checkGroupBySelExpr gbs (RExprUnaryOp     _ _UnaryOp  expr)        = checkGroupBySelExpr gbs expr
checkGroupBySelExpr gbs (RExprConst       _ _)                     = Right ()

instance Check RSQL where
  check (RQSelect rselect) = check rselect
  check _                  = Right ()
