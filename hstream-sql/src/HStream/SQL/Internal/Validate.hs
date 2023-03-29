{-# LANGUAGE CPP                 #-}
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

import           Control.Monad              (Monad (return), unless, void, when)
import qualified Data.Aeson                 as Aeson
import qualified Data.ByteString.Lazy       as BSL
import           Data.Char                  (isNumber)
import qualified Data.List                  as L
import           Data.List.Extra            (anySame)
import qualified Data.Text                  as Text
import           Data.Text.Encoding         (encodeUtf8)
import           Data.Time.Calendar         (isLeapYear)
import           GHC.Stack                  (HasCallStack)
import           HStream.SQL.Abs
import           HStream.SQL.Abs            (SelectItem)
import           HStream.SQL.Exception      (SomeSQLException (..),
                                             buildSQLException)
import           HStream.SQL.Extra          (extractPNInteger)
import           HStream.SQL.Validate.Utils

------------------------------ TypeClass Definition ----------------------------
class Validate t where
  validate :: HasCallStack => t -> Either SomeSQLException t
  {-# MINIMAL validate #-}

--------------------------------- Basic Types ----------------------------------
maxIdentifierLength :: Int
maxIdentifierLength = 255

identifierLetters :: [Char]
identifierLetters =
  ['A'..'Z'] ++ ['a'..'z']
  ++ (['\192'..'\255'] L.\\ ['\215', '\247'])

identifierChars :: [Char]
identifierChars =
  '_' : '-' : '\'' : ['0'..'9'] ++ identifierLetters

instance Validate DataType where
  validate = return

instance Validate PNInteger where
  validate = return

instance Validate PNDouble where
  validate = return

instance Validate SString where
  validate = return

instance Validate HyphenIdent where
  validate = return

instance Validate QuotedRaw where
  validate = return

instance Validate HIdent where
  validate ident@(HIdentNormal pos (HyphenIdent text)) = do
    unless (Text.length text <= maxIdentifierLength) (Left $ buildSQLException ParseException pos ("The length of an identifier should be equal to or less than " <> show maxIdentifierLength))
    return ident
  validate ident@(HIdentRaw pos (QuotedRaw text')) = do
    let text = Text.tail . Text.init $ text'
    unless (isValidIdent text) (Left $ buildSQLException ParseException pos ("Invalid identifier " <> Text.unpack text' <> ", please refer to the document"))
    unless (Text.length text <= maxIdentifierLength) (Left $ buildSQLException ParseException pos ("The length of an identifier should be equal to or less than " <> show maxIdentifierLength))
    return ident
    where
      isValidIdent :: Text.Text -> Bool
      isValidIdent ts =
        Text.head ts `elem` identifierLetters &&
        Text.foldl (\acc x -> if acc then
                                (x `elem` identifierChars) && acc else
                                acc
                   ) True (Text.tail ts)

instance Validate ColumnIdent where
  validate ident@(ColumnIdentNormal pos (HyphenIdent text)) = do
    unless (Text.length text <= maxIdentifierLength) (Left $ buildSQLException ParseException pos ("The length of an identifier should be equal to or less than " <> show maxIdentifierLength))
    return ident
  validate ident@(ColumnIdentRaw pos (QuotedRaw text')) = do
    let text = Text.tail . Text.init $ text'
    unless (Text.length text <= maxIdentifierLength) (Left $ buildSQLException ParseException pos ("The length of an identifier should be equal to or less than " <> show maxIdentifierLength))
    return ident

instance Validate Boolean where
  validate e@(BoolTrue  _) = return e
  validate e@(BoolFalse _) = return e

instance Validate DateStr where
  validate date@(DDateStr pos y m d) = do
    unless (y >= 0 && y <= 9999)     (Left $ buildSQLException ParseException pos "Year must be between 0 and 9999")
    unless (m >= 1 && m <= 12)       (Left $ buildSQLException ParseException pos "Month must be between 1 and 12")
    unless (d >= 1 && d <= realDays) (Left $ buildSQLException ParseException pos ("Day must be between 1 and " <> show realDays))
    return date
    where daysOfMonth = [31,28 + if isLeapYear y then 1 else 0,31,30,31,30,31,31,30,31,30,31]
          realDays = daysOfMonth !! (fromInteger m - 1)

instance Validate TimeStr where
  validate time@(TimeStrWithoutMicroSec pos h m s) = do
    unless (h >= 0 && h <= 23) (Left $ buildSQLException ParseException pos "Hour must be between 0 and 23")
    unless (m >= 0 && m <= 59) (Left $ buildSQLException ParseException pos "Minute must be between 0 and 59")
    unless (s >= 0 && s <= 59) (Left $ buildSQLException ParseException pos "Second must be between 0 and 59")
    return time
  validate time@(TimeStrWithMicroSec pos h m s ms) = do
    unless (h >= 0 && h <= 23) (Left $ buildSQLException ParseException pos "Hour must be between 0 and 23")
    unless (m >= 0 && m <= 59) (Left $ buildSQLException ParseException pos "Minute must be between 0 and 59")
    unless (s >= 0 && s <= 59) (Left $ buildSQLException ParseException pos "Second must be between 0 and 59")
    unless (ms >= 0 && ms <= 999) (Left $ buildSQLException ParseException pos "Microsecond must be between 0 and 999")
    return time

instance Validate DateTimeStr where
  validate datetime@(DDateTimeStr _ dateStr timeStr) =
    validate dateStr >> validate timeStr >> return datetime

instance Validate Timezone where
  validate zone@(TimezoneZ _) = return zone
  validate zone@(TimezonePositive pos h m) = do
    unless (h >= 0 && h <= 13 && m >= 0 && m <= 59) (Left $ buildSQLException ParseException pos "Timezone must be between -12:59 and +13:59")
    return zone
  validate zone@(TimezoneNegative pos h m) = do
    unless (h >= 0 && h <= 12 && m >= 0 && m <= 59) (Left $ buildSQLException ParseException pos "Timezone must be between -12:59 and +13:59")
    return zone

instance Validate TimestampStr where
  validate str@(DTimestampStr _ dateStr timeStr zone) =
    validate dateStr >> validate timeStr >> validate zone >> return str

instance Validate Date where
  validate date@(DDate _ dateStr) = validate dateStr >> return date
instance Validate Time where
  validate time@(DTime _ timeStr) = validate timeStr >> return time
instance Validate Timestamp where
  validate ts@(TimestampWithoutZone _ datetimeStr) = validate datetimeStr >> return ts
  validate ts@(TimestampWithZone _ tsStr) = validate tsStr >> return ts
instance Validate IntervalUnit where
  validate = return
instance Validate Interval where
  validate interval@(Interval pos (SString x) iUnit) = do
    unless (all isNumber $ Text.unpack $ Text.dropAround (=='\'') x) $ Left $ buildSQLException ParseException pos "Invalid interval value, only integer values are supported"
    validate iUnit >> return interval

instance Validate ColName where
  validate col@(ColNameSimple _ colIdent) = validate colIdent >> return col
  validate col@(ColNameStream _ hIdent colIdent) =
    validate hIdent >> validate colIdent >> return col

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

instance Validate LabelledValueExpr where
  validate expr@(DLabelledValueExpr _ e1 e2) = validate e1 >> validate e2 >> return expr

--------------------------------------- ValueExpr ------------------------------

-- 1. Add, Sub and Mul: exprs should be Num
-- 2. Constants should be legal
-- 3. Map and Arr are legal if all elements of them are legal (However Codegen does not support them yet)
--    And Map requires that all keys are unique
-- 4. Cols and Aggs should be legal
-- 5. Scalar functions should not be applied to aggs
instance Validate ValueExpr where
  validate expr@ExprCast1{} = return expr
  validate expr@ExprCast2{} = return expr
  validate expr@(ExprArr _ es) = mapM_ validate es >> return expr
  validate expr@(ExprMap _ les) = mapM_ validate les >> return expr
  validate expr@ExprEQ{} = isBoolExpr expr
  validate expr@ExprNEQ{} = isBoolExpr expr
  validate expr@ExprLT{} = isBoolExpr expr
  validate expr@ExprGT{} = isBoolExpr expr
  validate expr@ExprLEQ{} = isBoolExpr expr
  validate expr@ExprGEQ{} = isBoolExpr expr
  validate expr@(ExprAccessMap _ e1 e2) = validate e1 >> validate e2 >> return expr
  validate expr@(ExprAccessArray _ e _) = validate e >> return expr
  validate expr@(ExprSubquery _ select) = validate select >> return expr

  validate expr@ExprAdd{}    = isNumExpr expr
  validate expr@ExprSub{}    = isNumExpr expr
  validate expr@ExprMul{}    = isNumExpr expr
  validate expr@ExprAnd{}    = isBoolExpr expr
  validate expr@ExprOr{}     = isBoolExpr expr
  validate expr@ExprInt{}    = Right expr
  validate expr@ExprNum{}    = Right expr
  validate expr@ExprString{} = Right expr
  validate expr@ExprNull{}   = Right expr
  validate expr@ExprBool{}   = Right expr
  validate expr@(ExprDate _ date) = validate date >> return expr
  validate expr@(ExprTime _ time) = validate time >> return expr
  validate expr@(ExprTimestamp _ ts) = validate ts >> return expr
  validate expr@(ExprInterval _ interval) = validate interval >> return expr
  validate expr@(ExprColName _ col) = validate col   >> return expr
  validate expr@(ExprSetFunc _ func) = validate func >> return expr
  validate expr@(ExprScalarFunc _ func) = validate func >> return expr

isNumExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isNumExpr expr = case expr of
  (ExprCast1 _ e typ) -> validate e >> isNumType typ >> return expr
  (ExprCast2 _ e typ) -> validate e >> isNumType typ >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got an array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a map"
  (ExprEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprNEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprLT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprLEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprAccessMap _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprAccessArray _ e _) -> validate e >> return expr
  (ExprSubquery _ select) -> validate select >> return expr

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
  (ExprTimestamp pos _) -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got a Timestamp"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected a numeric expression but got an Interval"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
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
  where
    isNumType :: DataType -> Either SomeSQLException DataType
    isNumType typ = case typ of
      TypeInteger{} -> return typ
      TypeFloat{} -> return typ
      TypeNumeric{} -> return typ
      _ -> Left $ buildSQLException ParseException (getPos typ) "Argument type mismatched (not a numeric type)"

isFloatExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isFloatExpr expr = case expr of
  (ExprCast1 _ e typ) -> validate e >> isFloatType typ >> return expr
  (ExprCast2 _ e typ) -> validate e >> isFloatType typ >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected a float expression but got an array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected a float expression but got a map"
  (ExprEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprNEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprLT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprLEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprAccessMap _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprAccessArray _ e _) -> validate e >> return expr
  (ExprSubquery _ select) -> validate select >> return expr

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
  (ExprTimestamp pos _) -> Left $ buildSQLException ParseException pos "Expected a float expression but got a Timestamp"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected a float expression but got an Interval"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
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
  where
    isFloatType :: DataType -> Either SomeSQLException DataType
    isFloatType typ = case typ of
      TypeFloat{} -> return typ
      TypeNumeric{} -> return typ
      _ -> Left $ buildSQLException ParseException (getPos typ) "Argument type mismatched (not a float type)"

isOrdExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isOrdExpr expr = case expr of
  (ExprCast1 _ e typ) -> validate e >> isOrdType typ >> return expr
  (ExprCast2 _ e typ) -> validate e >> isOrdType typ >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got an array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected a comparable expression but got a map"
  (ExprEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprNEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprLT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprLEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprAccessMap _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprAccessArray _ e _) -> validate e >> return expr
  (ExprSubquery _ select) -> validate select >> return expr

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
  (ExprTimestamp _ ts) -> validate ts >> return expr
  (ExprInterval _ interval) -> validate interval >> return expr
  (ExprColName _ _) -> Right expr-- inaccurate
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
  where
    isOrdType :: DataType -> Either SomeSQLException DataType
    isOrdType typ = case typ of
      TypeInteger{} -> return typ
      TypeFloat{} -> return typ
      TypeNumeric{} -> return typ
      TypeText{} -> return typ
      TypeDate{} -> return typ
      TypeTime{} -> return typ
      TypeTimestamp{} -> return typ
      TypeInterval{} -> return typ
      _ -> Left $ buildSQLException ParseException (getPos typ) "Argument type mismatched (not a comparable type)"

isBoolExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isBoolExpr expr = case expr of
  (ExprCast1 _ e typ) -> validate e >> isBoolType typ >> return expr
  (ExprCast2 _ e typ) -> validate e >> isBoolType typ >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got an array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a map"
  (ExprEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprNEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprLT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprLEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprAccessMap _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprAccessArray _ e _) -> validate e >> return expr
  (ExprSubquery _ select) -> validate select >> return expr

  (ExprAdd pos _ _)  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprSub pos _ _)  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprMul pos _ _)  -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprAnd _ e1 e2)    -> isBoolExpr e1 >> isBoolExpr e2 >> return expr
  (ExprOr  _ e1 e2)    -> isBoolExpr e1 >> isBoolExpr e2 >> return expr
  (ExprInt pos _)      -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprNum pos _)      -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a numeric"
  (ExprString pos _)   -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a string"
  (ExprNull _)         -> Right expr
  (ExprBool _ _)       -> Right expr
  (ExprDate pos _)     -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a date"
  (ExprTime pos _)     -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a time"
  (ExprTimestamp pos _) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a timestamp"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected a boolean expression but got a interval"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
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
  where
    isBoolType :: DataType -> Either SomeSQLException DataType
    isBoolType typ = case typ of
      TypeBoolean{} -> return typ
      _ -> Left $ buildSQLException ParseException (getPos typ) "Argument type mismatched (not a boolean type)"


isIntExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isIntExpr expr = case expr of
  (ExprCast1 _ e typ) -> validate e >> isIntType typ >> return expr
  (ExprCast2 _ e typ) -> validate e >> isIntType typ >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected an integer expression but got an array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected an integer expression but got a map"
  (ExprEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprNEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprLT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprLEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprAccessMap _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprAccessArray _ e _) -> validate e >> return expr
  (ExprSubquery _ select) -> validate select >> return expr

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
  (ExprTimestamp pos _) -> Left $ buildSQLException ParseException pos "Expected an integral expression but got a Timestamp"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected an integral expression but got an Interval"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
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
  where
    isIntType :: DataType -> Either SomeSQLException DataType
    isIntType typ = case typ of
      TypeInteger{} -> return typ
      TypeNumeric{} -> return typ
      _ -> Left $ buildSQLException ParseException (getPos typ) "Argument type mismatched (not an integer type)"

isStringExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isStringExpr expr = case expr of
  (ExprCast1 _ e typ) -> validate e >> isStringType typ >> return expr
  (ExprCast2 _ e typ) -> validate e >> isStringType typ >> return expr
  (ExprArr pos _) -> Left $ buildSQLException ParseException pos "Expected a string expression but got an array"
  (ExprMap pos _) -> Left $ buildSQLException ParseException pos "Expected a string expression but got a map"
  (ExprEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprNEQ _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprLT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGT _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprLEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprGEQ _ e1 e2) -> isOrdExpr e1 >> isOrdExpr e2 >> return expr
  (ExprAccessMap _ e1 e2) -> validate e1 >> validate e2 >> return expr
  (ExprAccessArray _ e _) -> validate e >> return expr
  (ExprSubquery _ select) -> validate select >> return expr

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
  (ExprTimestamp pos _) -> Left $ buildSQLException ParseException pos "Expected an String expression but got a Timestamp"
  (ExprInterval pos _) -> Left $ buildSQLException ParseException pos "Expected an String expression but got an Interval"
  (ExprColName _ _)    -> Right expr -- TODO: Use schema to decide this
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
  where
    isStringType :: DataType -> Either SomeSQLException DataType
    isStringType typ = case typ of
      TypeText{} -> return typ
      _ -> Left $ buildSQLException ParseException (getPos typ) "Argument type mismatched (not a string type)"


-- For validating SearchCond
notAggregateExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
notAggregateExpr expr@(ExprCast1 _ e _) = notAggregateExpr e >> return expr
notAggregateExpr expr@(ExprCast2 _ e _) = notAggregateExpr e >> return expr
notAggregateExpr expr@(ExprArr _ es) = mapM_ notAggregateExpr es >> return expr
notAggregateExpr expr@(ExprMap _ les) = mapM_ (\le@(DLabelledValueExpr _ e1 e2) -> notAggregateExpr e1 >> notAggregateExpr e2 >> return le) les >> return expr
notAggregateExpr expr@(ExprEQ _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprNEQ _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprLT _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprGT _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprLEQ _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprGEQ _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprAccessMap _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprAccessArray _ e _) = notAggregateExpr e >> return expr
notAggregateExpr expr@(ExprSubquery _ _) = return expr

notAggregateExpr (ExprSetFunc pos _) = Left $ buildSQLException ParseException pos "Aggregate functions are not allowed in WHERE clause, HAVING clause and JOIN condition"
notAggregateExpr (ExprScalarFunc _ (ScalarFuncSin _ e)) = notAggregateExpr e
notAggregateExpr (ExprScalarFunc _ (ScalarFuncAbs _ e)) = notAggregateExpr e
notAggregateExpr expr@(ExprAdd _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprSub _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprMul _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprAnd _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr@(ExprOr  _ e1 e2) = notAggregateExpr e1 >> notAggregateExpr e2 >> return expr
notAggregateExpr expr = return expr

-- For validating Insert
isConstExpr :: HasCallStack => ValueExpr -> Either SomeSQLException ValueExpr
isConstExpr expr@(ExprCast1 _ e _) = isConstExpr e >> return expr
isConstExpr expr@(ExprCast2 _ e _) = isConstExpr e >> return expr
isConstExpr expr@(ExprArr _ es) = mapM_ isConstExpr es >> return expr
isConstExpr expr@(ExprMap _ les) = mapM_ (\le@(DLabelledValueExpr _ e1 e2) -> isConstExpr e1 >> isConstExpr e2 >> return le) les >> return expr
isConstExpr expr@(ExprEQ _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr
isConstExpr expr@(ExprNEQ _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr
isConstExpr expr@(ExprLT _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr
isConstExpr expr@(ExprGT _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr
isConstExpr expr@(ExprLEQ _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr
isConstExpr expr@(ExprGEQ _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr
isConstExpr expr@(ExprAccessArray _ e _) = isConstExpr e >> return expr
isConstExpr expr@(ExprAccessMap _ e1 e2) = isConstExpr e1 >> isConstExpr e2 >> return expr

isConstExpr expr@ExprInt{}      = Right expr
isConstExpr expr@ExprNum{}      = Right expr
isConstExpr expr@ExprString{}   = Right expr
isConstExpr expr@ExprNull{}     = Right expr
isConstExpr expr@ExprBool{}     = Right expr
isConstExpr expr@ExprDate{}     = Right expr
isConstExpr expr@ExprTime{}     = Right expr
isConstExpr expr@ExprTimestamp{} = Right expr
isConstExpr expr@ExprInterval{} = Right expr
isConstExpr _ = Left $ buildSQLException ParseException Nothing "INSERT only supports constant values"

------------------------------------- SELECT -----------------------------------
-- Sel
-- 1. SelList should be legal
instance Validate Sel where
  validate sel@(DSel _ l) = validate l >> return sel

instance Validate [SelectItem] where
  validate items = mapM_ validate items >> return items

instance Validate SelectItem where
  validate item@(SelectItemUnnamedExpr _ expr) = validate expr >> return item
  validate item@(SelectItemExprWithAlias _ expr colIdent) = validate expr >> validate colIdent >> return item
  validate item@(SelectItemQualifiedWildcard _ hIdent) = validate hIdent >> return item
  validate item@(SelectItemWildcard _) = return item

-- From
instance Validate From where
#ifdef HStreamUseV2Engine
  validate from@(DFrom _ tableRefs) = mapM_ validate tableRefs >> return from
#else
  validate from@(DFrom _ tableRef)  = validate tableRef >> return from
#endif

#ifdef HStreamUseV2Engine
instance Validate TableRef where
  validate r@(TableRefTumbling _ ref interval) = validate ref >> validate interval >> return r
  validate r@(TableRefHopping _ ref interval1 interval2) = validate ref >> validate interval1 >> validate interval2 >> return r
  validate r@(TableRefSliding _ ref interval) = validate ref >> validate interval >> return r
  validate r@(TableRefAs _ ref hIdent) = validate ref >> validate hIdent >> return r
  validate r@(TableRefCrossJoin _ ref1 _ ref2) = validate ref1 >> validate ref2 >> return r
  validate r@(TableRefNaturalJoin _ ref1 _ ref2) = validate ref1 >> validate ref2 >> return r
  validate r@(TableRefJoinOn _ ref1 jointype ref2 expr) = validate ref1 >> validate ref2 >> validate expr >> return r
  validate r@(TableRefJoinUsing _ ref1 jointype ref2 cols) = do
    validate ref1
    validate ref2
    mapM_ (\col -> case col of
              ColNameSimple{} -> return col
              ColNameStream pos _ _ ->
                Left $ buildSQLException ParseException pos "JOIN USING can only use column names without stream name"
          ) cols
    return r
  validate r@(TableRefIdent _ hIdent) = validate hIdent >> Right r
  validate r@(TableRefSubquery _ select) = validate select >> return r
#else
instance Validate TableRef where
  validate r@(TableRefTumbling _ ref interval) = validate ref >> validate interval >> return r
  validate r@(TableRefHopping _ ref interval1 interval2) = validate ref >> validate interval1 >> validate interval2 >> return r
  validate r@(TableRefSession _ ref interval) = validate ref >> validate interval >> return r
  validate r@(TableRefAs _ ref hIdent) = validate ref >> validate hIdent >> return r
  validate r@(TableRefCrossJoin _ ref1 _ ref2 i) = validate ref1 >> validate ref2 >> validate i >> return r
  validate r@(TableRefNaturalJoin _ ref1 _ ref2 i) = validate ref1 >> validate ref2 >> validate i >> return r
  validate r@(TableRefJoinOn _ ref1 jointype ref2 expr i) = validate ref1 >> validate ref2 >> validate expr >> validate i >> return r
  validate r@(TableRefJoinUsing _ ref1 jointype ref2 cols i) = do
    validate ref1
    validate ref2
    mapM_ (\col -> case col of
              ColNameSimple{} -> return col
              ColNameStream pos _ _ ->
                Left $ buildSQLException ParseException pos "JOIN USING can only use column names without stream name"
          ) cols
    validate i
    return r
  validate r@(TableRefIdent _ hIdent) = validate hIdent >> Right r
  validate r@(TableRefSubquery _ select) = validate select >> return r
#endif

-- Where
-- 1. ValueExpr in it should be legal
instance Validate Where where
  validate whr@(DWhereEmpty _) = Right whr
  validate whr@(DWhere _ expr) = validate expr >> return whr

-- GroupBy
#ifdef HStreamUseV2Engine
instance Validate GroupBy where
  validate grp = case grp of
    (DGroupByEmpty _) -> return grp
    (DGroupBy _ cols) -> mapM_ validate cols >> return grp
#else
instance Validate GroupBy where
  validate grp = case grp of
    (DGroupByEmpty _) -> return grp
    (DGroupBy _ cols) -> mapM_ validate cols >> return grp
#endif
-- Having
-- 1. ValueExpr in it should be legal
instance Validate Having where
  validate hav@(DHavingEmpty _) = Right hav
  validate hav@(DHaving _ expr) = validate expr >> return hav

---- Select

instance Validate Select where
  validate select@(DSelect _ sel@(DSel selPos selList) frm@(DFrom _ refs) whr grp hav) = do
#ifndef HStreamUseV2Engine
    case grp of
      DGroupByEmpty pos -> case refs of
        TableRefTumbling {} -> Left $ buildSQLException ParseException pos
          "Time window function `TUMBLE` requires a `GROUP BY` CLAUSE"
        TableRefHopping {} -> Left $ buildSQLException ParseException pos
          "Time window function `HOP` requires a `GROUP BY` CLAUSE"
        TableRefSession {} -> Left $ buildSQLException ParseException pos
          "Time window function `SESSION` requires a `GROUP BY` CLAUSE"
        _ -> pure ()
      _ -> pure ()
#endif
    void $ validate sel
    void $ validate frm
    void $ validate whr
    void $ validate grp
    void $ validate hav
    return select

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
      _            -> Left $ buildSQLException ParseException pos
        "EXPLAIN can not give any execution plan for CREATE CONNECTOR"

------------------------------------- CREATE -----------------------------------
instance Validate Create where
  validate create@(DCreate _ hIdent) = validate hIdent >> return create
  validate create@(CreateOp _ hIdent options) = validate hIdent >> validate (StreamOptions options) >> return create
  validate create@(CreateAs _ hIdent select) = validate hIdent >> validate select >> return create
  validate create@(CreateAsOp _ hIdent select options) =
    validate hIdent >> validate select >>
    validate (StreamOptions options) >> return create
  validate create@(CreateSourceConnector _ i1 i2 options) =
    validate i1 >> validate i2 >>
    validate (ConnectorOptions options) >> return create
  validate create@(CreateSourceConnectorIf _ i1 i2 options) =
    validate i1 >> validate i2 >>
    validate (ConnectorOptions options) >> return create
  validate create@(CreateSinkConnector _ i1 i2 options) =
    validate i1 >> validate i2 >>
    validate (ConnectorOptions options) >> return create
  validate create@(CreateSinkConnectorIf _ i1 i2 options) =
    validate i1 >> validate i2 >>
    validate (ConnectorOptions options) >> return create
  validate create@(CreateView _ hIdent select@(DSelect _ _ _ _ grp _)) =
    validate hIdent >> validate select >> return create

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

instance Validate Pause where
  validate pause@(PauseConnector _ hIdent) = validate hIdent >> return pause

instance Validate Resume where
  validate resume@(ResumeConnector _ hIdent) = validate hIdent >> return resume

------------------------------------- INSERT -----------------------------------
instance Validate Insert where
  validate insert@(DInsert pos hIdent fields exprs) = do
    unless (L.length fields == L.length exprs) (Left $ buildSQLException ParseException pos "Number of fields should match expressions")
    validate hIdent
    mapM_ validate fields
    mapM_ validate exprs
    mapM_ isConstExpr exprs
    return insert
  validate insert@(InsertBinary _ hIdent _) = validate hIdent >> return insert
  validate insert@(InsertJson pos hIdent (SString text)) = do
    validate hIdent
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
  validate d@(DDrop _ _ hIdent)  = validate hIdent >> return d
  validate d@(DropIf _ _ hIdent) = validate hIdent >> return d

------------------------------------- Terminate --------------------------------
instance Validate Terminate where
  validate t@(TerminateQuery _ hIdent) = validate hIdent >> return t
  validate t@(TerminateAll _)          = return t

------------------------------------- SQL --------------------------------------
instance Validate SQL where
  validate sql@(QSelect      _   select) = validate select   >> return sql
  validate sql@(QPushSelect  _   select) = validate select   >> return sql
  validate sql@(QCreate      _   create) = validate create   >> return sql
  validate sql@(QInsert      _   insert) = validate insert   >> return sql
  validate sql@(QShow        _    show_) = validate show_    >> return sql
  validate sql@(QDrop        _    drop_) = validate drop_    >> return sql
  validate sql@(QTerminate   _     term) = validate term     >> return sql
  validate sql@(QExplain     _  explain) = validate explain  >> return sql
  validate sql@(QPause _ _)              = return sql
  validate sql@(QResume _ _)             = return sql
