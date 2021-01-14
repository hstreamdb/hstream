{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE KindSignatures    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds         #-}
{-# LANGUAGE RankNTypes        #-}

module Language.SQL.Validate
  ( Validate (..)
  ) where

import           Control.Monad      (unless, when)
import qualified Data.List          as L
import           Data.List.Extra    (anySame)
import           Data.Text          (Text)
import           Data.Time.Calendar (isLeapYear)
import           Language.SQL.AST   (Position, errGenWithPos, errWithPos)
import           Language.SQL.Abs   (ColName (..), CompOp (CompOpEQ, CompOpNE),
                                     Create (..), Date (..),
                                     DerivedCol (DerivedColAs, DerivedColSimpl),
                                     From (..), GroupBy (..),
                                     GrpItem (GrpItemCol, GrpItemWin),
                                     Having (..), Ident (Ident), Insert (..),
                                     Interval (..), JoinCond (..),
                                     JoinWindow (..),
                                     LabelledValueExpr (DLabelledValueExpr),
                                     SQL (..), SearchCond (..), Sel (..),
                                     SelList (..), Select (..), SetFunc (..),
                                     StreamOption (..), TableRef (..),
                                     Time (..), ValueExpr (..), Where (..),
                                     Window (..))
import           Language.SQL.Extra (anyJoin, extractCondRefNames,
                                     extractRefNames, extractSelRefNames)

------------------------------ TypeClass Definition ----------------------------
class (Functor f) => Validate f where
  validate :: f Position -> Either String (f Position)
  {-# MINIMAL validate #-}

--------------------------------- Basic Types ----------------------------------
-- 1. 0 <= year <= 9999
-- 2. 1 <= month <= 12
-- 3. 1 <= day <= real days(30, 31 or other ones)
instance Validate Date where
  validate date@(DDate pos y m d) = do
    unless (y >= 0 && y <= 9999)     (Left $ errWithPos pos "Year must be between 0 and 9999")
    unless (m >= 1 && m <= 12)       (Left $ errWithPos pos "Month must be between 1 and 12")
    unless (d >= 1 && d <= realDays) (Left $ errWithPos pos $ "Day must be between 1 and " <> show realDays)
    return date
    where daysOfMonth = [31,28 + if isLeapYear y then 1 else 0,31,30,31,30,31,31,30,31,30,31]
          realDays = daysOfMonth !! (fromInteger m - 1)

-- 1. 0 <= hour   <= 23
-- 2. 0 <= minute <= 59
-- 3. 0 <= second <= 59
instance Validate Time where
  validate time@(DTime pos h m s) = do
    unless (h >= 0 && h <= 23) (Left $ errWithPos pos "Hour must be between 0 and 23")
    unless (m >= 0 && m <= 59) (Left $ errWithPos pos "Minute must be between 0 and 59")
    unless (s >= 0 && s <= 59) (Left $ errWithPos pos "Second must be between 0 and 59")
    return time

-- 1. number > 0
instance Validate Interval where
  validate i@(DInterval pos n unit) = do
    unless (n > 0) (Left $ errWithPos pos "Interval must be positive")
    return i

-- 1. only supports "col" and "stream.col"
-- TODO: "col[n]" and "col.x" are not supported yet
instance Validate ColName where
  validate c@(ColNameSimple _ (Ident col)) = Right c
  validate c@(ColNameStream _ (Ident s) (Ident col)) = Right c
  validate c@(ColNameInner pos _ _) = Left $ errWithPos pos "Nested column name is not supported yet"
  validate c@(ColNameIndex pos _ _) = Left $ errWithPos pos "Nested column name is not supported yet"

-- 1. Aggregate functions can not be nested
instance Validate SetFunc where
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

-- 1. Add, Sub and Mul: exprs should be Num
-- 2. Constants should be legal
-- 3. Map and Arr are legal if all elements of them are legal (However Codegen does not support them yet)
--    And Map requires that all keys are unique
-- 4. Cols and Aggs should be legal
instance Validate ValueExpr where
  validate expr@(ExprAdd _ _ _)   = isNumExpr expr
  validate expr@(ExprSub _ _ _)   = isNumExpr expr
  validate expr@(ExprMul _ _ _)   = isNumExpr expr
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

isNumExpr :: ValueExpr Position -> Either String (ValueExpr Position)
isNumExpr expr@(ExprAdd _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprSub _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprMul _ e1 e2)  = isNumExpr e1 >> isNumExpr e2 >> return expr
isNumExpr expr@(ExprInt _ _)      = Right expr
isNumExpr expr@(ExprNum _ _)      = Right expr
isNumExpr expr@(ExprString pos _)   = Left $ errWithPos pos "Expected a numeric expression but got a String"
isNumExpr expr@(ExprDate pos _)     = Left $ errWithPos pos "Expected a numeric expression but got a Date"
isNumExpr expr@(ExprTime pos _)     = Left $ errWithPos pos "Expected a numeric expression but got a Time"
isNumExpr expr@(ExprInterval pos _) = Left $ errWithPos pos "Expected a numeric expression but got an Interval"
isNumExpr expr@(ExprArr pos _)      = Left $ errWithPos pos "Expected a numeric expression but got an Array"
isNumExpr expr@(ExprMap pos _)      = Left $ errWithPos pos "Expected a numeric expression but got a Map"
isNumExpr expr@(ExprColName _ _)  = Right expr -- TODO: Use schema to decide this
isNumExpr expr@(ExprSetFunc _ (SetFuncCountAll _))  = Right expr
isNumExpr expr@(ExprSetFunc _ (SetFuncCount _ _)) = Right expr
isNumExpr expr@(ExprSetFunc _ (SetFuncAvg _ e))   = isNumExpr e >> return expr
isNumExpr expr@(ExprSetFunc _ (SetFuncSum _ e))   = isNumExpr e >> return expr
isNumExpr expr@(ExprSetFunc _ (SetFuncMax _ e))   = isOrdExpr e >> return expr
isNumExpr expr@(ExprSetFunc _ (SetFuncMin _ e))   = isOrdExpr e >> return expr

isOrdExpr :: ValueExpr Position -> Either String (ValueExpr Position)
isOrdExpr expr@(ExprAdd _ _ _)   = isNumExpr expr
isOrdExpr expr@(ExprSub _ _ _)   = isNumExpr expr
isOrdExpr expr@(ExprMul _ _ _)   = isNumExpr expr
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

-- For validating SearchCond
isAggregateExpr :: ValueExpr Position -> Either String (ValueExpr Position)
isAggregateExpr (ExprSetFunc pos _) = Left $ errWithPos pos "Aggregate functions are not allowed in WHERE clause, HAVING clause and JOIN condition"
isAggregateExpr expr@(ExprAdd _ e1 e2) = isAggregateExpr e1 >> isAggregateExpr e2 >> return expr
isAggregateExpr expr@(ExprSub _ e1 e2) = isAggregateExpr e1 >> isAggregateExpr e2 >> return expr
isAggregateExpr expr@(ExprMul _ e1 e2) = isAggregateExpr e1 >> isAggregateExpr e2 >> return expr
isAggregateExpr expr@(ExprArr _ es)    = mapM_ isAggregateExpr es >> return expr
isAggregateExpr expr@(ExprMap _ es)    = mapM_ (isAggregateExpr . extractExpr) es >> return expr
  where extractExpr (DLabelledValueExpr _ _ e) = e
isAggregateExpr expr = return expr

-- For validating Insert
isConstExpr :: ValueExpr Position -> Either String (ValueExpr Position)
isConstExpr expr@(ExprInt _ _)      = Right expr
isConstExpr expr@(ExprNum _ _)      = Right expr
isConstExpr expr@(ExprString _ _)   = Right expr
isConstExpr expr@(ExprDate _ _)     = Right expr
isConstExpr expr@(ExprTime _ _)     = Right expr
isConstExpr expr@(ExprInterval _ _) = Right expr
isConstExpr _ = Left $ errGenWithPos Nothing "INSERT only supports constant values"

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
    mapM_ validateCol dcols
    unless (aggCondition dcols)
      (Left $ errWithPos pos "If a SELECT clause contains a aggregate expression, it can not contain any other fields")
    when (anySame $ extractAlias dcols)
      (Left $ errWithPos pos "An SELECT clause can not contain the same column aliases")
    return l
    where
      validateCol (DerivedColSimpl _ e) = validate e
      validateCol (DerivedColAs _ e _)  = validate e
      isAggCol (DerivedColSimpl _ (ExprSetFunc _ _)) = True
      isAggCol (DerivedColAs _ (ExprSetFunc _ _) _)  = True
      isAggCol _                                     = False
      extractAlias []                                   = []
      extractAlias ((DerivedColSimpl _ _) : xs)         = extractAlias xs
      extractAlias ((DerivedColAs _ _ (Ident as)) : xs) = as : extractAlias xs
      aggCondition []   = True
      aggCondition [_]  = True
      aggCondition cols = not (any isAggCol cols)

-- From
-- 1. FROM only supports:
--    - a single stream
--    - joining of two single streams
-- 2. If joining extsts, every column name in join condition has to be like "s.x" instead of a single "x"
-- 3. Stream names in join condition have to match the ones specified before
-- 4. Stream alias is legal, but it is not supported by Codegen
instance Validate From where
  validate (DFrom pos []) = Left $ errWithPos pos "FROM clause should specify at least one stream"
  validate from@(DFrom pos refs@[ref]) = do
    validate ref
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
          (anySimpleRef, condRefNames) = ext ref
  validate (DFrom pos _) = Left $ errWithPos pos "FROM clause does not support many streams seperated by ',' yet"

-- 1. Joining of more than 2 streams is illegal
-- 2. JoinWindow and JoinCond should be legal
-- 3. Stream names in FROM and JOIN ON should match
-- 4. Stream alias is legal, but it is not supported by Codegen
instance Validate TableRef where
  validate r@(TableRefSimple _ _) = Right r
  validate r@(TableRefAs _ ref _) = validate ref >> return r
  validate r@(TableRefJoin pos (TableRefJoin _ _ _ _ _ _) _ _ _ _) = Left $ errWithPos pos "Joining more than 2 streams is not supported"
  validate r@(TableRefJoin pos _ _ (TableRefJoin _ _ _ _ _ _) _ _) = Left $ errWithPos pos "Joining more than 2 streams is not supported"
  validate r@(TableRefJoin pos ref1 joinType ref2 win joinCond) = do
    let stream1 = streamName ref1
        stream2 = streamName ref2
    when (stream1 == stream2)
      (Left $ errWithPos pos "Streams to be joined can not have the same name")
    validate ref1 >> validate ref2 >> validate win >> validate joinCond
    case joinCondStreamNames joinCond of
      Left err     -> Left err
      Right sNames -> do
        unless (sNames == (stream1, stream2) || sNames == (stream2, stream1))
          (Left $ errWithPos pos "Stream names in FROM and JOIN ON clauses do not match")
        return r
    where
      -- Note: Due to the max-2-join condition, `streamName TableRefJoin` is marked as impossible
      streamName (TableRefSimple _ (Ident t)) = t
      streamName (TableRefAs   _ _ (Ident t)) = t
      streamName _ = error $ errWithPos Nothing "Impossible happened"
      -- Note: Due to `Validate JoinCond`, only forms like "s1.x == s2.y" are legal
      joinCondStreamNames (DJoinCond _
                           cond@(CondOp _
                                 (ExprColName _ (ColNameStream _ (Ident s1) (Ident f1)))
                                 op
                                 (ExprColName _ (ColNameStream _ (Ident s2) (Ident f2)))
                                )
                          ) = return (s1, s2)
      joinCondStreamNames (DJoinCond pos _) = Left $ errWithPos pos "Impossible happened"

-- 1. Interval in the window should be legal
instance Validate JoinWindow where
  validate win@(DJoinWindow _ interval) = validate interval >> return win

-- 1. SearchCond in it should be legal
-- 2. Only "=" Conds is allowed
-- 3. Exprs between "=" should be column name with stream name, like "s1.x == s2.y". And s1 should not be the same as s2
instance Validate JoinCond where
  validate joinCond@(DJoinCond pos
                     cond@(CondOp _
                           (ExprColName _ (ColNameStream _ (Ident s1) (Ident f1)))
                           op
                           (ExprColName _ (ColNameStream _ (Ident s2) (Ident f2)))
                          )
                    ) = do
    when (s1 == s2)
      (Left $ errWithPos pos "Stream name conflicted in JOIN ON clause")
    case op of
      CompOpEQ _ -> validate cond >> return joinCond
      _          -> Left $ errWithPos pos "JOIN ON clause does not support operator other than ="
  validate joinCond@(DJoinCond pos (CondOp _ _ _ _))      = Left $ errWithPos pos "JOIN ON clause only supports forms such as 's1.x = s2.y'"
  validate joinCond@(DJoinCond pos (CondBetween _ _ _ _)) = Left $ errWithPos pos "JOIN ON clause does not support BETWEEN condition"
  validate joinCond@(DJoinCond pos (CondOr _ _ _))        = Left $ errWithPos pos "JOIN ON clause does not support OR condition"
  validate joinCond@(DJoinCond pos (CondAnd _ _ _))       = Left $ errWithPos pos "JOIN ON clause does not support OR condition"
  validate joinCond@(DJoinCond pos (CondNot _ _))         = Left $ errWithPos pos "JOIN ON clause does not support NOT condition"

-- 1. Exprs should be legal
-- 2. No aggregate Expr
-- 3. For LT, GT, LEQ, GEQ and Between SearchConds, every Expr should be comparable
instance Validate SearchCond where
  validate cond@(CondOr _ c1 c2)    = validate c1 >> validate c2 >> return cond
  validate cond@(CondAnd _ c1 c2)   = validate c1 >> validate c2 >> return cond
  validate cond@(CondNot _ c)       = validate c  >> return cond
  validate cond@(CondOp _ e1 op e2) = do
    isAggregateExpr e1 >> isAggregateExpr e2
    case op of
      CompOpEQ _ -> validate e1  >> validate e2  >> return cond
      CompOpNE _ -> validate e1  >> validate e2  >> return cond
      _          -> isOrdExpr e1 >> isOrdExpr e2 >> return cond
  validate cond@(CondBetween _ e1 e e2) = do
    isAggregateExpr e1 >> isAggregateExpr e2 >> isAggregateExpr e
    isOrdExpr e1 >> isOrdExpr e2 >> isOrdExpr e
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
  validate grp@(DGroupByEmpty _) = Right grp
  validate grp@(DGroupBy pos []) = Left $ errWithPos pos "Impossible happened"
  validate grp@(DGroupBy pos [GrpItemCol _ col]) = validate col >> return grp
  validate grp@(DGroupBy pos [GrpItemCol _ col, GrpItemWin _ win]) = validate col >> validate win >> return grp
  validate grp@(DGroupBy pos _) = Left $ errGenWithPos pos "An GROUP BY clause can only contain one column name with/without an window"

-- 1. Intervals should be legal
-- 2. For HoppingWindow, length >= hop
instance Validate Window where
  validate win@(TumblingWindow _ interval)  = validate interval >> return win
  validate win@(HoppingWindow pos i1 i2)    = do
    validate i1
    validate i2
    unless (i1 >= i2) (Left $ errWithPos pos "Hopping interval can not be larger than the size of the window")
    return win
  validate win@(SessionWindow pos interval) = validate interval >> return win

-- Having
-- 1. SearchCond in it should be legal
instance Validate Having where
  validate hav@(DHavingEmpty _) = Right hav
  validate hav@(DHaving _ cond) = validate cond >> return hav

---- Select
instance Validate Select where
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
      -- TODO: groupby has to match aggregate function
      -- TODO: matchHavWithSel


------------------------------------- CREATE -----------------------------------
instance Validate Create where
  validate create@(CreateAs _ _ select options) =
    validate select >> validate (SOs options) >> return create
  validate create@(DCreate _ _ options) =
    validate (SOs options) >> return create

instance Validate StreamOption where
  validate op@(OptionTopic _ s)   = Right op
  validate op@(OptionFormat pos s) = do
    unless (s `L.elem` ["JSON", "json"]) (Left $ errWithPos pos "Stream format can only support JSON yet")
    return op

newtype SOs a = SOs [StreamOption a] deriving (Functor)

instance Validate SOs where
  validate (SOs options) = do
    mapM_ validate options
    unless (fmt   == 1) (Left "There should be one and only one FORMAT option")
    unless (topic == 1) (Left "There should be one and only one TOPIC option")
    return $ SOs options
    where
      (topic, fmt) = foldr (\option (topic', fmt') ->
                                 case option of
                                   OptionTopic  _ _ -> (topic'+1, fmt')
                                   OptionFormat _ _ -> (topic', fmt'+1)) (0,0) options

------------------------------------- INSERT -----------------------------------
instance Validate Insert where
  validate insert@(DInsert _ _ exprs) = do
    mapM_ validate exprs
    mapM_ isConstExpr exprs
    return insert

------------------------------------- SQL --------------------------------------
instance Validate SQL where
  validate sql@(QSelect _ select) = validate select >> return sql
  validate sql@(QCreate _ create) = validate create >> return sql
  validate sql@(QInsert _ insert) = validate insert >> return sql
