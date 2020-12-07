{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE DeriveFunctor #-}

module Language.SQL.Validatable where

import Language.SQL.Abs
import Language.SQL.Extra

import Data.Text (Text, pack)
import qualified Data.List as L
import Data.List.Extra (anySame)
import Data.Time.Calendar (isLeapYear)
import Control.Monad

------------------------------ TypeClass Definition ----------------------------
class (Functor f) => Validatable f where
  validate :: f LineCol -> Either String (f LineCol)
  {-# MINIMAL validate #-}

--------------------------------- Basic Types ----------------------------------
instance Validatable Date where
  validate date@(DDate pos y m d) = do
    unless (y >= 0 && y <= 9999) (Left $ errWithPos pos "Year must be between 0 and 9999")
    unless (m >= 1 && m <= 12) (Left $ errWithPos pos "Month must be between 1 and 12")
    unless (d >= 1 && d <= realDays) (Left $ errWithPos pos $ "Day must be between 1 and " <> show realDays)
    return date
    where daysOfMonth = [31,28 + if isLeapYear y then 1 else 0,31,30,31,30,31,31,30,31,30,31]
          realDays = daysOfMonth !! (fromInteger m - 1)

instance Validatable Time where
  validate time@(DTime pos h m s) = do
    unless (h >= 0 && h <= 23) (Left $ errWithPos pos "Hour must be between 0 and 23")
    unless (m >= 0 && m <= 59) (Left $ errWithPos pos "Minute must be between 0 and 59")
    unless (s >= 0 && s <= 59) (Left $ errWithPos pos "Second must be between 0 and 59")
    return time

instance Validatable Interval where
  validate i@(DInterval pos n unit) = do
    unless (n > 0) (Left $ errWithPos pos "Interval must be positive")
    return i

instance Validatable ColName where
  validate c@(ColNameSimple _ (Ident col)) = Right c
  validate c@(ColNameStream _ (Ident s) (Ident col)) = Right c
  validate c@(ColNameInner pos _ _) = Left $ errWithPos pos "Nested column name is not supported yet"
  validate c@(ColNameIndex pos _ _) = Left $ errWithPos pos "Nested column name is not supported yet"

instance Validatable SetFunc where
  validate f@(SetFuncCountAll _) = Right f
  validate f@(SetFuncCount pos (ExprSetFunc _ _)) = Left $ errWithPos pos "Nested set functions are not supported"
  validate f@(SetFuncCount _ e) = validate e >> return f
  validate f@(SetFuncAvg pos (ExprSetFunc _ _))   = Left $ errWithPos pos "Nested set functions are not supported"
  validate f@(SetFuncAvg _ e) = isNumExpr e  >> return f
  validate f@(SetFuncSum pos (ExprSetFunc _ _))   = Left $ errWithPos pos "Nested set functions are not supported"
  validate f@(SetFuncSum _ e) = isNumExpr e  >> return f
  validate f@(SetFuncMax pos (ExprSetFunc _ _))   = Left $ errWithPos pos "Nested set functions are not supported"
  validate f@(SetFuncMax _ e) = isOrdExpr e  >> return f
  validate f@(SetFuncMin pos (ExprSetFunc _ _))   = Left $ errWithPos pos "Nested set functions are not supported"
  validate f@(SetFuncMin _ e) = isOrdExpr e  >> return f

--------------------------------------- ValueExpr ------------------------------
instance Validatable ValueExpr where
  validate expr@(ExprAdd _ _ _)   = isNumExpr expr
  validate expr@(ExprSub _ _ _)   = isNumExpr expr
  validate expr@(ExprMul _ _ _)   = isNumExpr expr
  validate expr@(ExprDiv _ _ _)   = isNumExpr expr
  validate expr@(ExprInt _ _)     = Right expr
  validate expr@(ExprNum _ _)     = Right expr
  validate expr@(ExprString _ _)  = Right expr
  validate expr@(ExprDate _ date) = validate date >> return expr
  validate expr@(ExprTime _ time) = validate time >> return expr
  validate expr@(ExprInterval _ interval) = validate interval >> return expr
  validate expr@(ExprArr _ es) = mapM_ validate es >> return expr
  validate expr@(ExprMap pos es) = do
    mapM_ helper es
    when (anySame $ extractLabel <$> es) (Left $ errWithPos pos "An map can not contain same keys")
    return expr
    where helper (DLabelledValueExpr _ _ e)           = validate e
          extractLabel (DLabelledValueExpr _ label _) = label
  validate expr@(ExprColName _ col) = validate col   >> return expr
  validate expr@(ExprSetFunc _ func) = validate func >> return expr

isNumExpr :: ValueExpr LineCol -> Either String (ValueExpr LineCol)
isNumExpr expr@(ExprAdd _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprSub _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprMul _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprDiv _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprInt _ _)      = Right expr
isNumExpr expr@(ExprNum _ _)      = Right expr
isNumExpr expr@(ExprString pos _)   = Left $ errWithPos pos "Expected a numeric expression but got a String"
isNumExpr expr@(ExprDate pos _)     = Left $ errWithPos pos "Expected a numeric expression but got a Date"
isNumExpr expr@(ExprTime pos _)     = Left $ errWithPos pos "Expected a numeric expression but got a Time"
isNumExpr expr@(ExprInterval pos _) = Left $ errWithPos pos "Expected a numeric expression but got an Interval"
isNumExpr expr@(ExprArr pos _)      = Left $ errWithPos pos "Expected a numeric expression but got an Array"
isNumExpr expr@(ExprMap pos _)      = Left $ errWithPos pos "Expected a numeric expression but got a Map"
isNumExpr expr@(ExprColName _ _)  = Right expr -- inaccurate
isNumExpr expr@(ExprSetFunc _ (SetFuncCountAll _))  = Right expr
isNumExpr expr@(ExprSetFunc _ (SetFuncCount _ _)) = Right expr
isNumExpr expr@(ExprSetFunc _ (SetFuncAvg _ e))   = isNumExpr e >> return expr
isNumExpr expr@(ExprSetFunc _ (SetFuncSum _ e))   = isNumExpr e >> return expr
isNumExpr expr@(ExprSetFunc _ (SetFuncMax _ e))   = isOrdExpr e >> return expr
isNumExpr expr@(ExprSetFunc _ (SetFuncMin _ e))   = isOrdExpr e >> return expr

isOrdExpr :: ValueExpr LineCol -> Either String (ValueExpr LineCol)
isOrdExpr expr@(ExprAdd _ _ _)   = isNumExpr expr
isOrdExpr expr@(ExprSub _ _ _)   = isNumExpr expr
isOrdExpr expr@(ExprMul _ _ _)   = isNumExpr expr
isOrdExpr expr@(ExprDiv _ _ _)   = isNumExpr expr
isOrdExpr expr@(ExprInt _ _)     = Right expr
isOrdExpr expr@(ExprNum _ _)     = Right expr
isOrdExpr expr@(ExprString _ _)  = Right expr
isOrdExpr expr@(ExprDate _ date) = validate date >> return expr
isOrdExpr expr@(ExprTime _ time) = validate time >> return expr
isOrdExpr expr@(ExprInterval _ interval) = validate interval >> return expr
isOrdExpr expr@(ExprArr pos _) = Left $ errWithPos pos "Expected a comparable expression but got an Array"
isOrdExpr expr@(ExprMap pos _) = Left $ errWithPos pos "Expected a comparable expression but got a Map"
isOrdExpr expr@(ExprColName _ _) = Right expr-- inaccurate
isOrdExpr expr@(ExprSetFunc _ (SetFuncCountAll _))  = Right expr
isOrdExpr expr@(ExprSetFunc _ (SetFuncCount _ _)) = Right expr
isOrdExpr expr@(ExprSetFunc _ (SetFuncAvg _ e))   = isNumExpr e >> return expr
isOrdExpr expr@(ExprSetFunc _ (SetFuncSum _ e))   = isNumExpr e >> return expr
isOrdExpr expr@(ExprSetFunc _ (SetFuncMax _ e))   = isOrdExpr e >> return expr
isOrdExpr expr@(ExprSetFunc _ (SetFuncMin _ e))   = isOrdExpr e >> return expr

------------------------------------- SELECT -----------------------------------
-- Sel
instance Validatable Sel where
  validate sel@(DSel _ l) = validate l >> return sel

instance Validatable SelList where
  validate l@(SelListAsterisk _) = Right l
  validate l@(SelListSublist pos dcols) = do
    mapM_ helper dcols
    when (anySame $ extractAlias dcols)
      (Left $ errWithPos pos "An SELECT clause can not contain the same column aliases")
    return l
    where
      helper (DerivedColSimpl _ e) = validate e
      helper (DerivedColAs _ e _)  = validate e
      extractAlias [] = []
      extractAlias ((DerivedColSimpl _ _) : xs) = extractAlias xs
      extractAlias ((DerivedColAs _ _ (Ident as)) : xs) = as : extractAlias xs

-- From
instance Validatable From where
  validate from@(DFrom pos refs) = do
    mapM_ validate refs
    when (anySame refNames) (Left $ errWithPos pos "An FROM clause can not contain the same stream names")
    when (anyJoin refs && anySimpleRef)
      (Left $ errWithPos pos "Stream name of column in JOIN ON clause has to be explicitly specified when joining exists")
    unless (all (`L.elem` refNames) condRefNames)
      (Left $ errWithPos pos "One or more stream name in joining condition is not specified in FROM clause")
    return from
    where refNames = extractRefNames refs -- Stream names and aliases
          ext :: TableRef a -> (Bool, [Text])
          ext (TableRefJoin _ _ _ _ _ (DJoinCond _ cond)) = extractCondRefNames cond
          ext _ = (False, [])
          -- Stream names in joining conditions
          (anySimpleRef, condRefNames) = let tups = ext <$> refs
                                          in (any (== True) (fst <$> tups), L.concat (snd <$> tups))

instance Validatable TableRef where
  validate r@(TableRefSimple _ _) = Right r
  validate r@(TableRefAs _ ref _) = validate ref >> return r
  validate r@(TableRefJoin pos (TableRefJoin _ _ _ _ _ _) _ _ _ _) = Left $ errWithPos pos "Joining more than 2 streams is not supported"
  validate r@(TableRefJoin pos _ _ (TableRefJoin _ _ _ _ _ _) _ _) = Left $ errWithPos pos "Joining more than 2 streams is not supported"
  validate r@(TableRefJoin _ ref1 joinType ref2 win joinCond) =
    validate ref1 >> validate ref2 >> validate win >> validate joinCond >> return r

instance Validatable JoinWindow where
  validate win@(DJoinWindow _ interval) = validate interval >> return win

instance Validatable JoinCond where
  validate joinCond@(DJoinCond pos (CondOp _ _ op _))     = do
    case op of
      CompOpEQ _ -> return joinCond
      _          -> Left $ errWithPos pos "JOIN ON clause does not support operator other than ="
  validate joinCond@(DJoinCond pos (CondBetween _ _ _ _)) = Left $ errWithPos pos "JOIN ON clause does not support BETWEEN condition"
  validate joinCond@(DJoinCond pos (CondOr _ _ _))        = Left $ errWithPos pos "JOIN ON clause does not support OR condition"
  validate joinCond@(DJoinCond _   (CondAnd _ c1 c2))     = validate c1 >> validate c2 >> return joinCond
  validate joinCond@(DJoinCond pos (CondNot _ _))           = Left $ errWithPos pos "JOIN ON clause does not support NOT condition"

instance Validatable SearchCond where
  validate cond@(CondOr _ c1 c2)    = validate c1 >> validate c2 >> return cond
  validate cond@(CondAnd _ c1 c2)   = validate c1 >> validate c2 >> return cond
  validate cond@(CondNot _ c)       = validate c  >> return cond
  validate cond@(CondOp _ e1 op e2) =
    case op of
      CompOpEQ _ -> validate e1  >> validate e2  >> return cond
      CompOpNE _ -> validate e1  >> validate e2  >> return cond
      _          -> isOrdExpr e1 >> isOrdExpr e2 >> return cond
  validate cond@(CondBetween _ e1 e e2) = isOrdExpr e1 >> isOrdExpr e2 >> isOrdExpr e >> return cond

-- Where
instance Validatable Where where
  validate whr@(DWhereEmpty _)   = Right whr
  validate whr@(DWhere _ cond)   = validate cond >> return whr

-- GroupBy
instance Validatable GroupBy where
  validate grp@(DGroupByEmpty _)     = Right grp
  validate grp@(DGroupBy pos items) = do
    mapM_ validate cols
    when (anySame cols) (Left $ errWithPos pos "An GROUP BY clause can not contain same column names")
    validateWins wins
    return grp
    where
      (cols, wins) = extractColsAndWins items
      extractColsAndWins [] = ([], [])
      extractColsAndWins ((GrpItemCol _ col) : xs) = let (cols, wins) = extractColsAndWins xs in (col:cols, wins)
      extractColsAndWins ((GrpItemWin _ win) : xs) = let (cols, wins) = extractColsAndWins xs in (cols, win:wins)
      validateWins []    = Right []
      validateWins [win] = validate win >> return [win]
      validateWins _     = Left $ errWithPos pos "There can be no more than 1 window in GROUP BY clause"

instance Validatable Window where
  validate win@(TumblingWindow _ interval) = validate interval >> return win
  validate win@(HoppingWindow pos i1 i2) = do
    validate i1
    validate i2
    unless (i1 >= i2) (Left $ errWithPos pos "Hopping interval can not be larger than the size of the window")
    return win
  validate win@(SessionWindow pos i1 i2) = do
    validate i1
    validate i2
    unless (i1 >= i2) (Left $ errWithPos pos "Timeout interval can not be larger than the size of the window")
    return win

-- Having
instance Validatable Having where
  validate hav@(DHavingEmpty _) = Right hav
  validate hav@(DHaving _ cond) = validate cond >> return hav

---- Select
instance Validatable Select where
  validate select@(DSelect _ sel@(DSel _ selList) frm@(DFrom _ refs) whr grp hav) = do
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
          SelListAsterisk _        -> Right ()
          SelListSublist pos' cols -> do
            let (anySimpleRef, selRefNames) = extractSelRefNames cols
                refNames                    = extractRefNames refs
            when (anySimpleRef && anyJoin refs)
              (Left $ errWithPos pos' "Stream name of column in SELECT clause has to be explicitly specified when joining exists")
            unless (all (`L.elem` refNames) selRefNames)
              (Left $ errWithPos pos' "All stream names in SELECT clause have to be explicitly specified in FROM clause")
            return ()
      matchWhrWithFrom =
        case whr of
          DWhereEmpty _    -> Right ()
          DWhere pos' cond -> do
            let (anySimpleRef, whrRefNames) = extractCondRefNames cond
                refNames                    = extractRefNames refs
            when (anySimpleRef && anyJoin refs)
              (Left $ errWithPos pos' "Stream name of column in WHERE clause has to be explicitly specified when joining exists")
            unless (all (`L.elem` refNames) whrRefNames)
              (Left $ errWithPos pos' "All stream names in WHERE clause have to be explicitly specified in FROM clause")
            return ()
      -- TODO: matchHavWithSel

------------------------------------- CREATE -----------------------------------
instance Validatable Create where
  validate create@(CreateAs _ _ select) = validate select >> return create
  validate create@(DCreate _ _ schemaElems options) =
    validate (SEs schemaElems) >> validate (SOs options) >> return create

instance Validatable SchemaElem where
  validate = Right

data SEs a = SEs [SchemaElem a] deriving (Functor)

instance Validatable SEs where
  validate (SEs schemaElems) = do
    when (anySame $ extractFieldName <$> schemaElems)
      (Left "There are fields with the same name in schema")
    return $ SEs schemaElems
    where extractFieldName (DSchemaElem _ (Ident name) _) = name

instance Validatable StreamOption where
  validate op@(OptionSource _ s) = Right op
  validate op@(OptionSink _ s)   = Right op
  validate op@(OptionFormat pos s) = do
    unless (s `L.elem` ["JSON", "json"]) (Left $ errWithPos pos "Stream format can only support JSON yet")
    return op

data SOs a = SOs [(StreamOption a)] deriving (Functor)

instance Validatable SOs where
  validate (SOs options) = do
    mapM_ validate options
    unless (fmt == 1) (Left "There should be one and only one FORMAT option")
    unless ((src == 1 && snk == 0) || src == 0 && snk == 1)
      (Left "There should be one and only one SOURCE or SINK option")
    return $ SOs options
    where
      (src, snk, fmt) = foldr (\option (src', snk', fmt') ->
                                 case option of
                                   OptionSource _ _ -> (src'+1, snk', fmt')
                                   OptionSink   _ _ -> (src', snk'+1, fmt')
                                   OptionFormat _ _ -> (src', snk', fmt'+1)) (0, 0, 0) options

------------------------------------- INSERT -----------------------------------
instance Validatable Insert where
  validate insert@(DInsert _ _ exprs) = mapM_ validate exprs >> return insert
  validate insert@(IndertWithSchema pos _ fields exprs) = do
    unless (L.length fields == L.length exprs) (Left $ errWithPos pos "Schema does not match values")
    mapM_ validate exprs
    return insert

------------------------------------- SQL --------------------------------------
instance Validatable SQL where
  validate sql@(QSelect _ select) = validate select >> return sql
  validate sql@(QCreate _ create) = validate create >> return sql
  validate sql@(QInsert _ insert) = validate insert >> return sql
  -- validate _ = Left "SQL validation only supports SELECT, CREATE and INSERT query yet"
