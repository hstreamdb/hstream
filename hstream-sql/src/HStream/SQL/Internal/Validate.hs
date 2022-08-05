{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.SQL.Internal.Validate
  ( Validate (..)
  ) where

import           Control.Monad              (unless, void, when)
import qualified Data.Aeson                 as Aeson
import qualified Data.ByteString.Lazy       as BSL
import qualified Data.List                  as L
import           Data.List.Extra            (anySame)
import           Data.Text                  (Text)
import qualified Data.Text                  as Text
import           Data.Text.Encoding         (encodeUtf8)
import           Data.Time.Calendar         (isLeapYear)
import           GHC.Stack                  (HasCallStack)
import           HStream.SQL.Abs
import           HStream.SQL.Exception      (SomeSQLException (..),
                                             buildSQLException)
import           HStream.SQL.Extra          (extractCondRefNames,
                                             extractPNInteger, extractRefNames,
                                             extractSelRefNames)
import           HStream.SQL.Validate.Utils

------------------------------ TypeClass Definition ----------------------------
class Validate t where
  validate :: HasCallStack => t -> Either SomeSQLException t
  {-# MINIMAL validate #-}

--------------------------------- Basic Types ----------------------------------
instance Validate PNInteger where
  validate = return

instance Validate PNDouble where
  validate = return

instance Validate SString where
  validate = return

instance Validate RawIdent where
  validate = return

instance Validate Boolean where
  validate e@(BoolTrue  _) = return e
  validate e@(BoolFalse _) = return e

-- 1. 0 <= year <= 9999
-- 2. 1 <= month <= 12
-- 3. 1 <= day <= real days(30, 31 or other ones)
instance Validate Date where
  validate date@(DDate pos y' m' d') = do
    unless (y >= 0 && y <= 9999)     (Left $ buildSQLException ParseException pos "Year must be between 0 and 9999")
    unless (m >= 1 && m <= 12)       (Left $ buildSQLException ParseException pos "Month must be between 1 and 12")
    unless (d >= 1 && d <= realDays) (Left $ buildSQLException ParseException pos ("Day must be between 1 and " <> show realDays))
    return date
    where y = extractPNInteger y'
          m = extractPNInteger m'
          d = extractPNInteger d'
          daysOfMonth = [31,28 + if isLeapYear y then 1 else 0,31,30,31,30,31,31,30,31,30,31]
          realDays = daysOfMonth !! (fromInteger m - 1)

-- 1. 0 <= hour   <= 23
-- 2. 0 <= minute <= 59
-- 3. 0 <= second <= 59
instance Validate Time where
  validate time@(DTime pos h' m' s') = do
    let h = extractPNInteger h'
        m = extractPNInteger m'
        s = extractPNInteger s'
    unless (h >= 0 && h <= 23) (Left $ buildSQLException ParseException pos "Hour must be between 0 and 23")
    unless (m >= 0 && m <= 59) (Left $ buildSQLException ParseException pos "Minute must be between 0 and 59")
    unless (s >= 0 && s <= 59) (Left $ buildSQLException ParseException pos "Second must be between 0 and 59")
    return time

-- 1. number > 0
instance Validate Interval where
  validate i@(DInterval pos n' _) = do
    let n = extractPNInteger n'
    unless (n > 0) (Left $ buildSQLException ParseException pos "Interval must be positive")
    return i

-- 1. only supports "col" and "stream.col"
-- TODO: "col[n]" and "col.x" are not supported yet
instance Validate ColName where
  validate c = case c of
    (ColNameSimple _ (Ident _)) -> Right c
    (ColNameStream _ (Ident _) (Ident _)) -> Right c
    (ColNameInner pos _ _) -> Left $ buildSQLException ParseException pos "Nested column name is not supported yet"
    (ColNameIndex pos _ _) -> Left $ buildSQLException ParseException pos "Nested column name is not supported yet"

-- 1. Aggregate functions can not be nested
instance Validate SetFunc where
  validate f = case f of
    (SetFuncCountAll _) -> Right f
    (SetFuncCount pos (ExprSetFunc _ _)) -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncCount _ e) -> validate e >> validate e >> return f
    (SetFuncAvg pos (ExprSetFunc _ _))   -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncAvg _ e) -> isNumExpr e  >> validate e >> return f
    (SetFuncSum pos (ExprSetFunc _ _))   -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncSum _ e) -> isNumExpr e  >> validate e >> return f
    (SetFuncMax pos (ExprSetFunc _ _))   -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncMax _ e) -> isOrdExpr e  >> validate e >> return f
    (SetFuncMin pos (ExprSetFunc _ _))   -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncMin _ e) -> isOrdExpr e  >> validate e >> return f
    (SetFuncTopK         pos (ExprSetFunc _ _) _) -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncTopK         _ e _) -> isOrdExpr e  >> validate e >> return f
    (SetFuncTopKDistinct pos (ExprSetFunc _ _) _) -> Left $ buildSQLException ParseException pos "Nested set functions are not supported"
    (SetFuncTopKDistinct _ e _) -> isOrdExpr e  >> validate e >> return f

-- 1. numeral expressions only
-- 2. scalar functions should not be applied to aggregates
instance Validate ScalarFunc where
  validate f
    | argType == intMask    = isIntExpr    expr >> validate expr >> notAggregateExpr expr >> return f
    | argType == floatMask  = isFloatExpr  expr >> validate expr >> notAggregateExpr expr >> return f
    | argType == numMask    = isNumExpr    expr >> validate expr >> notAggregateExpr expr >> return f
    | argType == ordMask    = isOrdExpr    expr >> validate expr >> notAggregateExpr expr >> return f
    | argType == boolMask   = isBoolExpr   expr >> validate expr >> notAggregateExpr expr >> return f
    | argType == stringMask = isStringExpr expr >> validate expr >> notAggregateExpr expr >> return f
    | argType == anyMask    = validate expr >> notAggregateExpr expr >> return f
    | otherwise             = Left $ buildSQLException ParseException (getPos f) "impossible happened"
    where expr    = getValueExpr f
          argType = getScalarArgType f

--------------------------------------- ValueExpr ------------------------------

-- 1. Add, Sub and Mul: exprs should be Num
-- 2. Constants should be legal
-- 3. Map and Arr are legal if all elements of them are legal (However Codegen does not support them yet)
--    And Map requires that all keys are unique
-- 4. Cols and Aggs should be legal
-- 5. Scalar functions should not be applied to aggs
instance Validate ValueExpr where
  validate expr@ExprAdd{}    = isNumExpr expr
  validate expr@ExprSub{}    = isNumExpr expr
  validate expr@ExprMul{}    = isNumExpr expr
  validate expr@ExprAnd{}    = isBoolExpr expr
  validate expr@ExprOr{}     = isBoolExpr expr
  validate expr@ExprInt{}    = Right expr
  validate expr@ExprNum{}    = Right expr
  validate expr@ExprString{} = Right expr
  validate expr@ExprRaw{}    = Right expr
  validate expr@ExprNull{}   = Right expr
  validate expr@ExprBool{}   = Right expr
  validate expr@(ExprDate _ date) = validate date >> return expr
  validate expr@(ExprTime _ time) = validate time >> return expr
  validate expr@(ExprInterval _ interval) = validate interval >> return expr
  validate expr@(ExprArr _ es) = mapM_ validate es >> return expr
  validate expr@(ExprMap pos es) = do
    mapM_ helper es
    when (anySame $ extractLabel <$> es) (Left $ buildSQLException ParseException pos "An map can not contain same keys")
    return expr
    where helper (DLabelledValueExpr _ _ e)           = validate e
          extractLabel (DLabelledValueExpr _ label _) = label
  validate expr@(ExprColName _ col) = validate col   >> return expr
  validate expr@(ExprSetFunc _ func) = validate func >> return expr
  validate expr@(ExprScalarFunc _ func) = validate func >> return expr

isNumExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isNumExpr expr = case expr of
  (ExprAdd _ e1 e2)    -> isNumExpr e1 >> isNumExpr e2 >> return expr
  (ExprSub _ e1 e2)    -> isNumExpr e1 >> isNumExpr e2 >> return expr
  (ExprMul _ e1 e2)    -> isNumExpr e1 >> isNumExpr e2 >> return expr
  (ExprAnd pos _ _)    -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a boolean"
  (ExprOr pos _ _ )    -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a boolean"
  (ExprInt _ _)        -> Right expr
  (ExprNum _ _)        -> Right expr
  (ExprString pos _)   -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a String"
  (ExprNull _)         -> Right expr
  (ExprBool pos _)     -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a boolean"
  (ExprDate pos _)     -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a Date"
  (ExprTime pos _)     -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a Time"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got an Interval"
  (ExprArr pos _)      -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got an Array"
  (ExprMap pos _)      -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a Map"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
  (ExprRaw _ _)        -> Right expr -- TODO: Use schema to decide this
  (ExprSetFunc _ (SetFuncCountAll _)) -> Right expr
  (ExprSetFunc _ (SetFuncCount _ _))  -> Right expr
  (ExprSetFunc _ (SetFuncAvg _ _))    -> return expr
  (ExprSetFunc _ (SetFuncSum _ _))    -> return expr
  (ExprSetFunc _ (SetFuncMax _ e))    -> isNumExpr e >> return expr
  (ExprSetFunc _ (SetFuncMin _ e))    -> isNumExpr e >> return expr
  (ExprSetFunc _ (SetFuncTopK         _ e1 e2)) -> isNumExpr e1 >> isNumExpr e2 >> return expr
  (ExprSetFunc _ (SetFuncTopKDistinct _ e1 e2)) -> isNumExpr e1 >> isNumExpr e2 >> return expr
  (ExprScalarFunc _ f) ->
    let funcType = getScalarFuncType f
     in if isTypeNum funcType then return expr
                              else Left $ buildSQLException ParseException (getPos f) "Argument type mismatched"

isFloatExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isFloatExpr expr = case expr of
  (ExprAdd _ e1 e2)    -> isFloatExpr e1 >> isFloatExpr e2 >> return expr
  (ExprSub _ e1 e2)    -> isFloatExpr e1 >> isFloatExpr e2 >> return expr
  (ExprMul _ e1 e2)    -> isFloatExpr e1 >> isFloatExpr e2 >> return expr
  (ExprAnd pos _ _)    -> Left $ buildSQLException ParseException pos "Expected a float expression but got a boolean"
  (ExprOr pos _ _ )    -> Left $ buildSQLException ParseException pos "Expected a float expression but got a boolean"
  (ExprInt pos _)        -> Left $ buildSQLException ParseException pos "Expected a float expression but got an Integral"
  (ExprNum _ _)        -> Right expr
  (ExprString pos _)   -> Left $ buildSQLException ParseException pos "Expected a float expression but got a String"
  (ExprNull _)         -> Right expr
  (ExprBool pos _)     -> Left $ buildSQLException ParseException pos "Expected a float expression but got a boolean"
  (ExprDate pos _)     -> Left $ buildSQLException ParseException pos "Expected a float expression but got a Date"
  (ExprTime pos _)     -> Left $ buildSQLException ParseException pos "Expected a float expression but got a Time"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected a float expression but got an Interval"
  (ExprArr pos _)      -> Left $ buildSQLException ParseException pos "Expected a float expression but got an Array"
  (ExprMap pos _)      -> Left $ buildSQLException ParseException pos "Expected a float expression but got a Map"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
  (ExprRaw _ _)        -> Right expr -- TODO: Use schema to decide this
  (ExprSetFunc pos (SetFuncCountAll _)) -> Left $ buildSQLException ParseException pos "Expected a float expression but got an Integral"
  (ExprSetFunc pos (SetFuncCount _ _))  -> Left $ buildSQLException ParseException pos "Expected a float expression but got an Integral"
  (ExprSetFunc _ (SetFuncAvg _ _))    -> return expr
  (ExprSetFunc _ (SetFuncSum _ e))    -> isFloatExpr e >> return expr
  (ExprSetFunc _ (SetFuncMax _ e))    -> isFloatExpr e >> return expr
  (ExprSetFunc _ (SetFuncMin _ e))    -> isFloatExpr e >> return expr
  (ExprSetFunc _ (SetFuncTopK         _ e1 e2)) -> isFloatExpr e1 >> isFloatExpr e2 >> return expr
  (ExprSetFunc _ (SetFuncTopKDistinct _ e1 e2)) -> isFloatExpr e1 >> isFloatExpr e2 >> return expr
  (ExprScalarFunc _ f) ->
    let funcType = getScalarFuncType f
     in if isTypeFloat funcType then return expr
                                else Left $ buildSQLException ParseException (getPos f) "Argument type mismatched"

isOrdExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isOrdExpr expr = case expr of
  ExprAdd{}    -> isNumExpr expr
  ExprSub{}    -> isNumExpr expr
  ExprMul{}    -> isNumExpr expr
  (ExprAnd pos _ _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got a boolean"
  (ExprOr  pos _ _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got a boolean"
  ExprInt{}    -> Right expr
  ExprNum{}    -> Right expr
  ExprString{} -> Right expr
  (ExprNull _)         -> Right expr
  (ExprBool pos _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got a boolean"
  (ExprDate _ date) -> validate date >> return expr
  (ExprTime _ time) -> validate time >> return expr
  (ExprInterval _ interval) -> validate interval >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got an Array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got a Map"
  (ExprColName _ _) -> Right expr-- inaccurate
  (ExprRaw _ _)     -> Right expr -- TODO: Use schema to decide this
  (ExprSetFunc _ (SetFuncCountAll _)) -> Right expr
  (ExprSetFunc _ (SetFuncCount _ _))  -> Right expr
  (ExprSetFunc _ (SetFuncAvg _ _))    -> return expr
  (ExprSetFunc _ (SetFuncSum _ _))    -> return expr
  (ExprSetFunc _ (SetFuncMax _ _))    -> return expr
  (ExprSetFunc _ (SetFuncMin _ _))    -> return expr
  (ExprSetFunc _ (SetFuncTopK         _ _ _)) -> return expr
  (ExprSetFunc _ (SetFuncTopKDistinct _ _ _)) -> return expr
  (ExprScalarFunc _ f) ->
    let funcType = getScalarFuncType f
     in if isTypeOrd funcType then return expr
                              else Left $ buildSQLException ParseException (getPos f) "Argument type mismatched"

isBoolExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isBoolExpr expr = case expr of
  (ExprAdd pos _ _)  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSub pos _ _)  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprMul pos _ _)  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprAnd _ e1 e2)    -> isBoolExpr e1 >> isBoolExpr e2 >> return expr
  (ExprOr  _ e1 e2)    -> isBoolExpr e1 >> isBoolExpr e2 >> return expr
  (ExprInt pos _)      -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprNum pos _)      -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprString pos _)   -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprNull _)         -> Right expr
  (ExprBool _ _)       -> Right expr
  (ExprDate pos _)     -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprTime pos _)     -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprArr pos _)      -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprMap pos _)      -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
  (ExprRaw _ _)        -> Right expr -- TODO: Use schema to decide this
  (ExprSetFunc pos (SetFuncCountAll _)) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncCount _ _))  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncAvg _ _))    -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncSum _ _))    -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncMax _ _))    -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncMin _ _))    -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncTopK         _ _ _)) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSetFunc pos (SetFuncTopKDistinct _ _ _)) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprScalarFunc pos (ScalarFuncSin _ _)) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprScalarFunc pos (ScalarFuncAbs _ _)) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprScalarFunc _ f) ->
    let funcType = getScalarFuncType f
     in if isTypeBool funcType then return expr
                               else Left $ buildSQLException ParseException (getPos f) "Argument type mismatched"

isIntExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isIntExpr expr = case expr of
  (ExprAdd _ e1 e2)    -> isIntExpr e1 >> isIntExpr e2 >> return expr
  (ExprSub _ e1 e2)    -> isIntExpr e1 >> isIntExpr e2 >> return expr
  (ExprMul _ e1 e2)    -> isIntExpr e1 >> isIntExpr e2 >> return expr
  (ExprAnd pos _ _)    -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a boolean"
  (ExprOr pos _ _ )    -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a boolean"
  (ExprInt _ _)        -> Right expr
  (ExprNum pos _)        -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a numeric"
  (ExprString pos _)   -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a String"
  (ExprNull _)         -> Right expr
  (ExprBool pos _)     -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a boolean"
  (ExprDate pos _)     -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a Date"
  (ExprTime pos _)     -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a Time"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected an integral expression but got an Interval"
  (ExprArr pos _)      -> Left $ buildSQLException ParseException pos "Expected an integral expression but got an Array"
  (ExprMap pos _)      -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a Map"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
  (ExprRaw _ _)        -> Right expr -- TODO: Use schema to decide this
  (ExprSetFunc _ (SetFuncCountAll _))    -> Right expr
  (ExprSetFunc _ (SetFuncCount _ _))     -> Right expr
  (ExprSetFunc _ (SetFuncAvg _ e))       -> isIntExpr e >> return expr -- not precise
  (ExprSetFunc _ (SetFuncSum _ e))       -> isIntExpr e >> return expr
  (ExprSetFunc _ (SetFuncMax _ e))       -> isIntExpr e >> return expr
  (ExprSetFunc _ (SetFuncMin _ e))       -> isIntExpr e >> return expr
  (ExprSetFunc _ (SetFuncTopK         _ e1 e2)) -> isIntExpr e1 >> isIntExpr e2 >> return expr
  (ExprSetFunc _ (SetFuncTopKDistinct _ e1 e2)) -> isIntExpr e1 >> isIntExpr e2 >> return expr
  (ExprScalarFunc _ f) ->
    let funcType = getScalarFuncType f
     in if isTypeInt funcType then return expr
                              else Left $ buildSQLException ParseException (getPos f) "Argument type mismatched"

isStringExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isStringExpr expr = case expr of
  (ExprAdd pos _ _)    -> Left $ buildSQLException ParseException pos "Expected an String expression but got a numeric"
  (ExprSub pos _ _)    -> Left $ buildSQLException ParseException pos "Expected an String expression but got a numeric"
  (ExprMul pos _ _)    -> Left $ buildSQLException ParseException pos "Expected an String expression but got a numeric"
  (ExprAnd pos _ _)    -> Left $ buildSQLException ParseException pos "Expected an String expression but got a boolean"
  (ExprOr pos _ _ )    -> Left $ buildSQLException ParseException pos "Expected an String expression but got a boolean"
  (ExprInt pos _)      -> Left $ buildSQLException ParseException pos "Expected an String expression but got an Integer"
  (ExprNum pos _)      -> Left $ buildSQLException ParseException pos "Expected an String expression but got a numeric"
  (ExprString _ _)     -> return expr
  (ExprNull _)         -> Right expr
  (ExprBool pos _)     -> Left $ buildSQLException ParseException pos "Expected an String expression but got a boolean"
  (ExprDate pos _)     -> Left $ buildSQLException ParseException pos "Expected an String expression but got a Date"
  (ExprTime pos _)     -> Left $ buildSQLException ParseException pos "Expected an String expression but got a Time"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected an String expression but got an Interval"
  (ExprArr pos _)      -> Left $ buildSQLException ParseException pos "Expected an String expression but got an Array"
  (ExprMap pos _)      -> Left $ buildSQLException ParseException pos "Expected an String expression but got a Map"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
  (ExprRaw _ _)        -> Right expr -- TODO: Use schema to decide this
  (ExprSetFunc pos (SetFuncCountAll _))    -> Left $ buildSQLException ParseException pos "Expected an String expression but got an Integer"
  (ExprSetFunc pos (SetFuncCount _ _))     -> Left $ buildSQLException ParseException pos "Expected an String expression but got an Integer"
  (ExprSetFunc pos (SetFuncAvg _ _))       -> Left $ buildSQLException ParseException pos "Expected an String expression but got a numeric"
  (ExprSetFunc pos (SetFuncSum _ _))       -> Left $ buildSQLException ParseException pos "Expected an String expression but got a numeric"
  (ExprSetFunc _ (SetFuncMax _ e))       -> isStringExpr e >> return expr
  (ExprSetFunc _ (SetFuncMin _ e))       -> isStringExpr e >> return expr
  (ExprSetFunc _ (SetFuncTopK         _ e1 e2)) -> isStringExpr e1 >> isStringExpr e2 >> return expr
  (ExprSetFunc _ (SetFuncTopKDistinct _ e1 e2)) -> isStringExpr e1 >> isStringExpr e2 >> return expr
  (ExprScalarFunc _ f) ->
    let funcType = getScalarFuncType f
     in if isTypeString funcType then return expr
                                 else Left $ buildSQLException ParseException (getPos f) "Argument type mismatched"

-- For validating SearchCond
notAggregateExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
notAggregateExpr (ExprSetFunc pos _) = Left $ buildSQLException ParseException pos "Aggregate functions are not allowed in WHERE clause, HAVING clause and JOIN condition"
notAggregateExpr (ExprScalarFunc _ (ScalarFuncSin _ e)) = notAggregateExpr e
notAggregateExpr (ExprScalarFunc _ (ScalarFuncAbs _ e)) = notAggregateExpr e
notAggregateExpr expr@(ExprAdd _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprSub _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprMul _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprAnd _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprOr  _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprArr _ es)    = mapM_ notAggregateExpr es >> return expr
notAggregateExpr expr@(ExprMap _ es)    = mapM_ (notAggregateExpr . extractExpr) es >> return expr
  where extractExpr (DLabelledValueExpr _ _ e) = e
notAggregateExpr expr = return expr

-- For validating Insert
isConstExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isConstExpr expr@ExprInt{}      = Right expr
isConstExpr expr@ExprNum{}      = Right expr
isConstExpr expr@ExprString{}   = Right expr
isConstExpr expr@ExprNull{}     = Right expr
isConstExpr expr@ExprBool{}     = Right expr
isConstExpr expr@ExprDate{}     = Right expr
isConstExpr expr@ExprTime{}     = Right expr
isConstExpr expr@ExprInterval{} = Right expr
isConstExpr expr@ExprArr{}      = isConstExprArr expr
isConstExpr expr@ExprMap{}      = isConstExprMap expr
isConstExpr _ = Left $ buildSQLException ParseException Nothing "INSERT only supports constant values"

-- If all elements in an array are const expr, the array is a const expr.
isConstExprArr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isConstExprArr expr@(ExprArr a xs) = h xs where
  h :: [ValueExpr] -> Either SomeSQLException ValueExpr
  h [] = pure expr
  h (x : xs) = do
    x  <- isConstExpr x
    xs <- h xs
    case xs of
      ExprArr _ xs -> pure $ ExprArr a (x : xs)
      _ -> Left $ buildSQLException ParseException Nothing "Impossible happened"
isConstExprArr _ = Left $ buildSQLException ParseException Nothing "Impossible happened"

-- If all elements in an map are const expr, the map is a const expr.
isConstExprMap :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isConstExprMap expr@(ExprMap a xs) = h xs where
  h :: [LabelledValueExpr] -> Either SomeSQLException ValueExpr
  h [] = pure expr
  h (x : xs) = do
    let DLabelledValueExpr _ _ val = x
    val <- isConstExpr val
    xs  <- h xs
    case xs of
      ExprMap _ xs -> pure $ ExprMap a (x : xs)
      _ -> Left $ buildSQLException ParseException Nothing "Impossible happened"
isConstExprMap _ = Left $ buildSQLException ParseException Nothing "Impossible happened"

------------------------------------- SELECT -----------------------------------
-- Sel
-- 1. SelList should be legal
instance Validate Sel where
  validate sel@(DSel _ l) = validate l >> return sel

-- 1. Column expressions should be all legal
-- 2. Aliases (if exists) should be all unique
-- 3. aggCindition: if there exists an aggregate expression, there can not be any other field
instance Validate SelList where
  validate l@(SelListAsterisk _) = Right l
  validate l@(SelListSublist pos dcols) = do
    mapM_ validate dcols
    when (anySame $ extractAlias dcols)
      (Left $ buildSQLException ParseException pos "An SELECT clause can not contain the same column aliases")
    return l
    where
      anyAgg = anyAggInSelList l
      extractAlias []                                   = []
      extractAlias ((DerivedColSimpl _ _) : xs)         = extractAlias xs
      extractAlias ((DerivedColAs _ _ (Ident as)) : xs) = as : extractAlias xs

instance Validate DerivedCol where
  validate dcol@(DerivedColSimpl _ e) = validate e >> return dcol
  validate dcol@(DerivedColAs _ e _)  = validate e >> return dcol

-- From
instance Validate From where
  validate (DFrom pos []) = Left $ buildSQLException ParseException pos "FROM clause should specify at least one stream"
  validate from@(DFrom pos refs) = do
    mapM_ validate refs
    return from

instance Validate TableRef where
  validate r@(TableRefSimple _ _) = Right r
  validate r@(TableRefSubquery _ select) = validate select >> return r
  validate r@(TableRefUnion _ ref1 ref2) = validate ref1 >> validate ref2 >> return r
  validate r@(TableRefAs _ ref _) = validate ref >> return r

-- 1. Exprs should be legal
-- 2. No aggregate Expr
-- 3. For LT, GT, LEQ, GEQ and Between SearchConds, every Expr should be comparable
instance Validate SearchCond where
  validate cond@(CondOr _ c1 c2)    = validate c1 >> validate c2 >> return cond
  validate cond@(CondAnd _ c1 c2)   = validate c1 >> validate c2 >> return cond
  validate cond@(CondNot _ c)       = validate c  >> return cond
  validate cond@(CondOp _ e1 op e2) = do
    void $ notAggregateExpr e1 >> notAggregateExpr e2
    case op of
      CompOpEQ _ -> validate e1  >> validate e2  >> return cond
      CompOpNE _ -> validate e1  >> validate e2  >> return cond
      _          -> isOrdExpr e1 >> isOrdExpr e2 >> return cond
  validate cond@(CondBetween _ e1 e e2) = do
    void $ notAggregateExpr e1 >> notAggregateExpr e2 >> notAggregateExpr e
    void $ isOrdExpr e1 >> isOrdExpr e2 >> isOrdExpr e
    return cond

-- Where
-- 1. SearchCond in it should be legal
instance Validate Where where
  validate whr@(DWhereEmpty _) = Right whr
  validate whr@(DWhere _ cond) = validate cond >> return whr

-- GroupBy
-- 1. GROUP BY onlu supports:
--    - a single column
--    - a column and a window
-- 2. Column and/or window should be legal
instance Validate GroupBy where
  validate grp = case grp of
    (DGroupByEmpty _) -> Right grp
    (DGroupBy pos []) -> Left $ buildSQLException ParseException pos "Impossible happened"
    (DGroupBy _   [GrpItemCol _ col]) -> validate col >> return grp
    (DGroupBy _   [GrpItemCol _ col, GrpItemWin _ win]) -> validate col >> validate win >> return grp
    (DGroupBy pos _) -> Left $ buildSQLException ParseException pos "An GROUP BY clause can only contain one column name with/without an window"

-- 1. Intervals should be legal
-- 2. For HoppingWindow, length >= hop
instance Validate Window where
  validate win@(TumblingWindow _ interval)  = validate interval >> return win
  validate win@(HoppingWindow pos i1 i2)    = do
    void $ validate i1
    void $ validate i2
    unless (i1 >= i2) (Left $ buildSQLException ParseException pos "Hopping interval can not be larger than the size of the window")
    return win
  validate win@(SlidingWindow pos interval) = validate interval >> return win

-- Having
-- 1. SearchCond in it should be legal
instance Validate Having where
  validate hav@(DHavingEmpty _) = Right hav
  validate hav@(DHaving _ cond) = validate cond >> return hav

---- Select
instance Validate Select where
  validate select@(DSelect _ sel@(DSel selPos selList) frm@(DFrom _ refs) whr grp hav) = do
    void $ validate sel
    void $ validate frm
    void $ validate whr
    void $ validate grp
    void $ validate hav
    matchSelWithFrom
    matchWhrWithFrom
    matchSelWithGrp
    return select
      where
      matchSelWithFrom =
        case selList of
          SelListAsterisk _        -> Right ()
          SelListSublist pos' cols -> do
            let (anySimpleRef, selRefNames) = extractSelRefNames cols
                refNames                    = extractRefNames refs
            unless (all (`L.elem` refNames) selRefNames)
              (Left $ buildSQLException ParseException pos' "All stream names in SELECT clause have to be explicitly specified in FROM clause")
            return ()
      matchWhrWithFrom =
        case whr of
          DWhereEmpty _    -> Right ()
          DWhere pos' cond -> do
            let (anySimpleRef, whrRefNames) = extractCondRefNames cond
                refNames                    = extractRefNames refs
            unless (all (`L.elem` refNames) whrRefNames)
              (Left $ buildSQLException ParseException pos' "All stream names in WHERE clause have to be explicitly specified in FROM clause")
            return ()
      -- TODO: groupby has to match aggregate function
      matchSelWithGrp =
        let anyAgg = anyAggInSelList selList
         in case grp of
              DGroupByEmpty _ -> case anyAgg of
                True  -> Left $ buildSQLException ParseException selPos "An aggregate function has to be with an GROUP BY clause"
                False -> Right ()
              DGroupBy pos  _ -> case anyAgg of
                True  -> Right ()
                False -> Left $ buildSQLException ParseException pos "There should be an aggregate function in the SELECT clause when GROUP BY clause exists"
      -- TODO: matchHavWithSel

----------------------------------- SELECTVIEW ---------------------------------
instance Validate SelectView where
  validate sv@(DSelectView _ sel frm whr) = do
    validate sel >> validate frm >> validate whr
    validateSel sel >> validateFrm frm
    return sv
    where
      validateSel sel@(DSel _ (SelListAsterisk _)) = return sel
      validateSel sel@(DSel _ (SelListSublist _ dcols)) = mapM_ validate dcols >> return sel

      validateFrm frm@(DFrom _ refs) = mapM_ validateRef refs >> return frm

      validateRef ref@(TableRefSimple _ _) = return ref
      validateRef ref = Left $ buildSQLException ParseException (getPos ref) "Only a view name is allowed in FROM clause when selecting from a VIEW"

------------------------------------- EXPLAIN ----------------------------------
instance Validate Explain where
  validate explain@(ExplainSelect _   select) = validate select >> return explain
  validate explain@(ExplainCreate pos create) =
    case create of
      CreateAs{}   -> validate create >> return explain
      CreateAsOp{} -> validate create >> return explain
      CreateView{} -> validate create >> return explain
      DCreate{}    -> Left $ buildSQLException ParseException pos
        "EXPLAIN can not give any execution plan for CREATE STREAM without a SELECT clause"
      CreateOp{}   -> Left $ buildSQLException ParseException pos
        "EXPLAIN can not give any execution plan for CREATE STREAM without a SELECT clause"
      -- FIXME: not for wildcard
      _            -> Left $ buildSQLException ParseException pos
        "EXPLAIN can not give any execution plan for CREATE CONNECTOR"

------------------------------------- CREATE -----------------------------------
instance Validate Create where
  validate create@(DCreate _ _) = return create
  validate create@(CreateOp _ _ options) = validate (StreamOptions options) >> return create
  validate create@(CreateAs _ _ select) = validate select >> return create
  validate create@(CreateAsOp _ _ select options) =
    validate select >> validate (StreamOptions options) >> return create
  validate create@(CreateSourceConnector _ _ _ options) = validate (ConnectorOptions options) >> return create
  validate create@(CreateSourceConnectorIf _ _ _ options) = validate (ConnectorOptions options) >> return create
  validate create@(CreateSinkConnector _ _ _ options) = validate (ConnectorOptions options) >> return create
  validate create@(CreateSinkConnectorIf _ _ _ options) = validate (ConnectorOptions options) >> return create
  validate create@(CreateView _ _ select@(DSelect _ _ _ _ grp _)) = validate select >> return create

instance Validate StreamOption where
  validate op@(OptionRepFactor pos n') = do
    let n = extractPNInteger n'
    unless (n > 0) (Left $ buildSQLException ParseException pos "Replicate factor can only be positive integers")
    return op

newtype StreamOptions = StreamOptions [StreamOption]

instance Validate StreamOptions where
  validate (StreamOptions options) = do
    mapM_ validate options
    case options of
      [OptionRepFactor{}] -> return $ StreamOptions options
      _                                   ->
        Left $ buildSQLException ParseException Nothing "There should be one and only one REPLICATE option"

newtype ConnectorOptions = ConnectorOptions [ConnectorOption]

instance Validate ConnectorOptions where
  validate ops@(ConnectorOptions options) = return ops
  --   if any (\case PropertyConnector _ _ -> True; _ -> False) options && any (\case PropertyStreamName _ _ -> True; _ -> False) options
  --   then mapM_ validate options >> return ops
  --   else Left $ buildSQLException ParseException Nothing "Options STREAM (name) or TYPE (of Connector) missing"

instance Validate ConnectorOption where
  -- validate op@(PropertyAny _ _ expr) = isConstExpr expr >> return op
  validate op                        = return op

------------------------------------- INSERT -----------------------------------
instance Validate Insert where
  validate insert@(DInsert pos _ fields exprs) = do
    unless (L.length fields == L.length exprs) (Left $ buildSQLException ParseException pos "Number of fields should match expressions")
    mapM_ validate exprs
    mapM_ isConstExpr exprs
    return insert
  validate insert@InsertBinary {} = return insert
  validate insert@(InsertJson pos _ (SString text)) = do
    let serialized = BSL.fromStrict . encodeUtf8 . Text.init . Text.tail $ text
    let (o' :: Maybe Aeson.Object) = Aeson.decode serialized
    case o' of
      Nothing -> Left $ buildSQLException ParseException pos "Invalid JSON text"
      Just _  -> return insert

------------------------------------- SHOW -------------------------------------
instance Validate ShowQ where
  validate = return

------------------------------------- DROP -------------------------------------
instance Validate Drop where
  validate = return

------------------------------------- Terminate --------------------------------
instance Validate Terminate where
  validate = return

------------------------------------- SQL --------------------------------------
instance Validate SQL where
  validate sql@(QSelect      _   select) = validate select   >> return sql
  validate sql@(QSelectView  _  selView) = validate selView  >> return sql
  validate sql@(QCreate      _   create) = validate create   >> return sql
  validate sql@(QInsert      _   insert) = validate insert   >> return sql
  validate sql@(QShow        _    show_) = validate show_    >> return sql
  validate sql@(QDrop        _    drop_) = validate drop_    >> return sql
  validate sql@(QTerminate   _     term) = validate term     >> return sql
  validate sql@(QExplain     _  explain) = validate explain  >> return sql
  validate sql@(QPause _ _)              = return sql
  validate sql@(QResume _ _)             = return sql
