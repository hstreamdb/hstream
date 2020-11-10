{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}

module Language.SQL.Validatable where

import Language.SQL.Abs
import Language.SQL.Extra

import Data.Text (Text, pack)
import qualified Data.List as L
import Data.List.Extra (anySame)
import Data.Time.Calendar (isLeapYear)
import Control.Monad

------------------------------ TypeClass Definition ----------------------------
class Validatable a where
  validate :: a -> Either String a
  {-# MINIMAL validate #-}

--------------------------------- Basic Types ----------------------------------
instance Validatable Date where
  validate date@(DDate y m d) = do
    unless (y >= 0 && y <= 9999) (Left "Year must be between 0 and 9999")
    unless (m >= 1 && m <= 12) (Left "Month must be between 1 and 12")
    unless (d >= 1 && d <= realDays) (Left $ "Day must be between 1 and " <> show realDays)
    return date
    where daysOfMonth = [31,28 + if isLeapYear y then 1 else 0,31,30,31,30,31,31,30,31,30,31]
          realDays = daysOfMonth !! (fromInteger m - 1)

instance Validatable Time where
  validate time@(DTime h m s) = do
    unless (h >= 0 && h <= 23) (Left "Hour must be between 0 and 23")
    unless (m >= 0 && m <= 59) (Left "Minute must be between 0 and 59")
    unless (s >= 0 && s <= 59) (Left "Second must be between 0 and 59")
    return time

instance Validatable Interval where
  validate i@(DInterval n unit) = do
    unless (n > 0) (Left "Interval must be positive")
    return i

instance Validatable ColName where
  validate c@(ColNameSimple (Ident col)) = Right c
  validate c@(ColNameStream (Ident s) (Ident col)) = Right c
  validate _ = Left "Nested column name is not supported yet"

instance Validatable SetFunc where
  validate f@(SetFuncCountAll) = Right f
  validate f@(SetFuncCount (ExprSetFunc _)) = Left "Nested set functions are not supported"
  validate f@(SetFuncCount e) = validate e >> return f
  validate f@(SetFuncAvg (ExprSetFunc _))   = Left "Nested set functions are not supported"
  validate f@(SetFuncAvg e) = isNumExpr e  >> return f
  validate f@(SetFuncSum (ExprSetFunc _))   = Left "Nested set functions are not supported"
  validate f@(SetFuncSum e) = isNumExpr e  >> return f
  validate f@(SetFuncMax (ExprSetFunc _))   = Left "Nested set functions are not supported"
  validate f@(SetFuncMax e) = isOrdExpr e  >> return f
  validate f@(SetFuncMin (ExprSetFunc _))   = Left "Nested set functions are not supported"
  validate f@(SetFuncMin e) = isOrdExpr e  >> return f

--------------------------------------- ValueExpr ------------------------------
instance Validatable ValueExpr where
  validate expr@(ExprAdd _ _)   = isNumExpr expr
  validate expr@(ExprSub _ _)   = isNumExpr expr
  validate expr@(ExprMul _ _)   = isNumExpr expr
  validate expr@(ExprDiv _ _)   = isNumExpr expr
  validate expr@(ExprInt _)     = Right expr
  validate expr@(ExprNum _)     = Right expr
  validate expr@(ExprString _)  = Right expr
  validate expr@(ExprDate date) = validate date >> return expr
  validate expr@(ExprTime time) = validate time >> return expr
  validate expr@(ExprInterval interval) = validate interval >> return expr
  validate expr@(ExprArr es) = mapM_ validate es >> return expr
  validate expr@(ExprMap es) = do
    mapM_ helper es
    when (anySame $ extractLabel <$> es) (Left "An map can not contain same keys")
    return expr
    where helper (DLabelledValueExpr _ e)           = validate e
          extractLabel (DLabelledValueExpr label _) = label
  validate expr@(ExprColName col) = validate col   >> return expr
  validate expr@(ExprSetFunc func) = validate func >> return expr

isNumExpr :: ValueExpr -> Either String ValueExpr
isNumExpr expr@(ExprAdd e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprSub e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprMul e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprDiv e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprInt _)      = Right expr
isNumExpr expr@(ExprNum _)      = Right expr
isNumExpr expr@(ExprString _)   = Left "Expected a numeric expression but got a String"
isNumExpr expr@(ExprDate _)     = Left "Expected a numeric expression but got a Date"
isNumExpr expr@(ExprTime _)     = Left "Expected a numeric expression but got a Time"
isNumExpr expr@(ExprInterval _) = Left "Expected a numeric expression but got an Interval"
isNumExpr expr@(ExprArr _)      = Left "Expected a numeric expression but got an Array"
isNumExpr expr@(ExprMap _)      = Left "Expected a numeric expression but got a Map"
isNumExpr expr@(ExprColName _)  = Right expr -- inaccurate
isNumExpr expr@(ExprSetFunc SetFuncCountAll)  = Right expr
isNumExpr expr@(ExprSetFunc (SetFuncCount _)) = Right expr
isNumExpr expr@(ExprSetFunc (SetFuncAvg e))   = isNumExpr e >> return expr
isNumExpr expr@(ExprSetFunc (SetFuncSum e))   = isNumExpr e >> return expr
isNumExpr expr@(ExprSetFunc (SetFuncMax e))   = isOrdExpr e >> return expr
isNumExpr expr@(ExprSetFunc (SetFuncMin e))   = isOrdExpr e >> return expr

isOrdExpr :: ValueExpr -> Either String ValueExpr
isOrdExpr expr@(ExprAdd _ _)   = isNumExpr expr
isOrdExpr expr@(ExprSub _ _)   = isNumExpr expr
isOrdExpr expr@(ExprMul _ _)   = isNumExpr expr
isOrdExpr expr@(ExprDiv _ _)   = isNumExpr expr
isOrdExpr expr@(ExprInt _)     = Right expr
isOrdExpr expr@(ExprNum _)     = Right expr
isOrdExpr expr@(ExprString _)  = Right expr
isOrdExpr expr@(ExprDate date) = validate date >> return expr
isOrdExpr expr@(ExprTime time) = validate time >> return expr
isOrdExpr expr@(ExprInterval interval) = validate interval >> return expr
isOrdExpr expr@(ExprArr _) = Left "Expected a comparable expression but got an Array"
isOrdExpr expr@(ExprMap _) = Left "Expected a comparable expression but got a Map"
isOrdExpr expr@(ExprColName _) = Right expr-- inaccurate
isOrdExpr expr@(ExprSetFunc SetFuncCountAll)  = Right expr
isOrdExpr expr@(ExprSetFunc (SetFuncCount _)) = Right expr
isOrdExpr expr@(ExprSetFunc (SetFuncAvg e))   = isNumExpr e >> return expr
isOrdExpr expr@(ExprSetFunc (SetFuncSum e))   = isNumExpr e >> return expr
isOrdExpr expr@(ExprSetFunc (SetFuncMax e))   = isOrdExpr e >> return expr
isOrdExpr expr@(ExprSetFunc (SetFuncMin e))   = isOrdExpr e >> return expr

------------------------------------- SELECT -----------------------------------
-- Sel
instance Validatable Sel where
  validate sel@(DSel l) = validate l >> return sel

instance Validatable SelList where
  validate l@SelListAsterisk = Right l
  validate l@(SelListSublist dcols) = do
    mapM_ helper dcols
    when (anySame $ extractAlias dcols)
      (Left "An SELECT clause can not contain the same column aliases")
    return l
    where
      helper (DerivedColSimpl e) = validate e
      helper (DerivedColAs e _)  = validate e
      extractAlias [] = []
      extractAlias ((DerivedColSimpl _) : xs) = extractAlias xs
      extractAlias ((DerivedColAs _ (Ident as)) : xs) = (pack as) : extractAlias xs

-- From
instance Validatable From where
  validate from@(DFrom refs) = do
    mapM_ validate refs
    when (anySame refNames) (Left "An FROM clause can not contain the same stream names")
    when (anyJoin refs && anySimpleRef)
      (Left "Stream name of column in JOIN ON clause has to be explicitly specified when joining exists")
    unless (all (`L.elem` refNames) condRefNames)
      (Left "One or more stream name in joining condition is not specified in FROM clause")
    return from
    where refNames = extractRefNames refs -- Stream names and aliases
          ext :: TableRef -> (Bool, [Text])
          ext (TableRefJoin _ _ _ _ (DJoinCond cond)) = extractCondRefNames cond
          ext _ = (False, [])
          -- Stream names in joining conditions
          (anySimpleRef, condRefNames) = let tups = ext <$> refs
                                          in (any (== True) (fst <$> tups), L.concat (snd <$> tups))

instance Validatable TableRef where
  validate r@(TableRefSimple _) = Right r
  validate r@(TableRefAs ref _) = validate ref >> return r
  validate r@(TableRefJoin (TableRefJoin _ _ _ _ _) _ _ _ _) = Left "Joining more than 2 streams is not supported"
  validate r@(TableRefJoin _ _ (TableRefJoin _ _ _ _ _) _ _) = Left "Joining more than 2 streams is not supported"
  validate r@(TableRefJoin ref1 joinType ref2 win joinCond) =
    validate ref1 >> validate ref2 >> validate win >> validate joinCond >> return r

instance Validatable JoinWindow where
  validate win@(DJoinWindow interval) = validate interval >> return win

instance Validatable JoinCond where
  validate joinCond@(DJoinCond (CondOp _ op _))     = do
    unless (op == CompOpEQ) (Left "JOIN ON clause does not support operator other than =")
    return joinCond
  validate joinCond@(DJoinCond (CondBetween _ _ _)) = Left "JOIN ON clause does not support BETWEEN condition"
  validate joinCond@(DJoinCond (CondOr _ _))        = Left "JOIN ON clause does not support OR condition"
  validate joinCond@(DJoinCond (CondAnd c1 c2))     = validate c1 >> validate c2 >> return joinCond
  validate joinCond@(DJoinCond (CondNot _))         = Left "JOIN ON clause does not support NOT condition"

instance Validatable SearchCond where
  validate cond@(CondOr c1 c2)    = validate c1 >> validate c2 >> return cond
  validate cond@(CondAnd c1 c2)   = validate c1 >> validate c2 >> return cond
  validate cond@(CondNot c)       = validate c  >> return cond
  validate cond@(CondOp e1 op e2) =
    case op of
      CompOpEQ -> validate e1  >> validate e2  >> return cond
      CompOpNE -> validate e1  >> validate e2  >> return cond
      _        -> isOrdExpr e1 >> isOrdExpr e2 >> return cond
  validate cond@(CondBetween e1 e e2) = isOrdExpr e1 >> isOrdExpr e2 >> isOrdExpr e >> return cond

-- Where
instance Validatable Where where
  validate whr@DWhereEmpty   = Right whr
  validate whr@(DWhere cond) = validate cond >> return whr

-- GroupBy
instance Validatable GroupBy where
  validate grp@DGroupByEmpty    = Right grp
  validate grp@(DGroupBy items) = do
    mapM_ validate cols
    when (anySame cols) (Left "An GROUP BY clause can not contain same column names")
    validateWins wins
    return grp
    where
      (cols, wins) = extractColsAndWins items
      extractColsAndWins [] = ([], [])
      extractColsAndWins ((GrpItemCol col) : xs) = let (cols, wins) = extractColsAndWins xs in (col:cols, wins)
      extractColsAndWins ((GrpItemWin win) : xs) = let (cols, wins) = extractColsAndWins xs in (cols, win:wins)
      validateWins []    = Right []
      validateWins [win] = validate win >> return [win]
      validateWins _     = Left "There can be no more than 1 window in GROUP BY clause"

instance Validatable Window where
  validate win@(TumblingWindow interval) = validate interval >> return win
  validate win@(HoppingWindow i1 i2) = do
    validate i1
    validate i2
    unless (i1 >= i2) (Left "Hopping interval can not be larger than the size of the window")
    return win
  validate win@(SessionWindow i1 i2) = do
    validate i1
    validate i2
    unless (i1 >= i2) (Left "Timeout interval can not be larger than the size of the window")
    return win

-- Having
instance Validatable Having where
  validate hav@DHavingEmpty   = Right hav
  validate hav@(DHaving cond) = validate cond >> return hav

---- Select
instance Validatable Select where
  validate select@(DSelect sel@(DSel selList) frm@(DFrom refs) whr grp hav) = do
    validate sel
    validate frm
    validate whr
    validate grp
    validate hav
    matchSelWithFrom
    matchWhrWithFrom
    return select
    where
      matchSelWithFrom =
        case selList of
          SelListAsterisk     -> Right ()
          SelListSublist cols -> do
            let (anySimpleRef, selRefNames) = extractSelRefNames cols
                refNames                    = extractRefNames refs
            when (anySimpleRef && anyJoin refs)
              (Left "Stream name of column in SELECT clause has to be explicitly specified when joining exists")
            unless (all (`L.elem` refNames) selRefNames)
              (Left "All stream names in SELECT clause have to be explicitly specified in FROM clause")
            return ()
      matchWhrWithFrom =
        case whr of
          DWhereEmpty -> Right ()
          DWhere cond -> do
            let (anySimpleRef, whrRefNames) = extractCondRefNames cond
                refNames                    = extractRefNames refs
            when (anySimpleRef && anyJoin refs)
              (Left "Stream name of column in WHERE clause has to be explicitly specified when joining exists")
            unless (all (`L.elem` refNames) whrRefNames)
              (Left "All stream names in WHERE clause have to be explicitly specified in FROM clause")
            return ()
      -- TODO: matchHavWithSel

------------------------------------- CREATE -----------------------------------
instance Validatable Create where
  validate create@(CreateAs _ select) = validate select >> return create
  validate create@(DCreate _ schemaElems options) =
    validate schemaElems >> validate options >> return create

instance Validatable SchemaElem where
  validate = Right

instance Validatable [SchemaElem] where
  validate schemaElems = do
    when (anySame $ extractFieldName <$> schemaElems)
      (Left "There are fields with the same name in schema")
    return schemaElems
    where extractFieldName (DSchemaElem (Ident name) _) = pack name

instance Validatable StreamOption where
  validate op@(OptionSource s) = Right op
  validate op@(OptionSink s)   = Right op
  validate op@(OptionFormat s) = do
    unless (s `L.elem` ["JSON", "json"]) (Left "Stream format can only support JSON yet")
    return op

instance Validatable [StreamOption] where
  validate options = do
    mapM_ validate options
    unless (fmt == 1) (Left "There should be one and only one FORMAT option")
    unless ((src == 1 && snk == 0) || src == 0 && snk == 1)
      (Left "There should be one and only one SOURCE or SINK option")
    return options
    where
      (src, snk, fmt) = foldr (\option (src', snk', fmt') ->
                                 case option of
                                   OptionSource _ -> (src'+1, snk', fmt')
                                   OptionSink   _ -> (src', snk'+1, fmt')
                                   OptionFormat _ -> (src', snk', fmt'+1)) (0, 0, 0) options

------------------------------------- INSERT -----------------------------------
instance Validatable Insert where
  validate insert@(DInsert _ exprs) = mapM_ validate exprs >> return insert
  validate insert@(IndertWithSchema _ fields exprs) = do
    unless (L.length fields == L.length exprs) (Left "Schema does not match values")
    mapM_ validate exprs
    return insert

------------------------------------- SQL --------------------------------------
instance Validatable SQL where
  validate sql@(QSelect select) = validate select >> return sql
  validate sql@(QCreate create) = validate create >> return sql
  validate sql@(QInsert insert) = validate insert >> return sql
  -- validate _ = Left "SQL validation only supports SELECT, CREATE and INSERT query yet"
