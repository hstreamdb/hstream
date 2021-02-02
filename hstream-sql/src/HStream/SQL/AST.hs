{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.AST where

import           Data.Either           (isRight, lefts)
import           Data.Kind             (Type)
import qualified Data.List             as L
import           Data.Text             as Text (Text, unpack)
import qualified Data.Time             as Time
import           GHC.Stack             (HasCallStack)
import           HStream.SQL.Abs
import           HStream.SQL.Exception (Position, SomeSQLException (..),
                                        throwSQLException)
import           HStream.SQL.Print     (printTree)

--------------------------------------------------------------------------------
type family RefinedType a :: Type

class Refine a where
  refine :: HasCallStack => a -> RefinedType a

--------------------------------------------------------------------------------
type StreamName = Text
type FieldName  = Text

type RDate = Time.Day
type instance RefinedType (Date a) = RDate
instance Refine (Date a) where
  refine (DDate _ year month day) = Time.fromGregorian year (fromInteger month) (fromInteger day)

type RTime = Time.DiffTime
type instance RefinedType (Time a) = RTime
instance Refine (Time a) where
  refine (DTime _ hour minute second) = Time.secondsToDiffTime $
    hour * 3600 + minute * 60 + second

type RInterval = Time.DiffTime
type instance RefinedType (Interval a) = RInterval
instance Refine (Interval a) where
  refine (DInterval _ n (TimeUnitSec _  )) = Time.secondsToDiffTime   n
  refine (DInterval _ n (TimeUnitMin _  )) = Time.secondsToDiffTime $ n * 60
  refine (DInterval _ n (TimeUnitDay _  )) = Time.secondsToDiffTime $ n * 60 * 24
  refine (DInterval _ n (TimeUnitWeek _ )) = Time.secondsToDiffTime $ n * 60 * 24 * 7
  refine (DInterval _ n (TimeUnitMonth _)) = Time.secondsToDiffTime $ n * 60 * 24 * 30
  refine (DInterval _ n (TimeUnitYear _ )) = Time.secondsToDiffTime $ n * 60 * 24 * 365

data Constant = ConstantInt       Int
              | ConstantNum       Double
              | ConstantString    String
              | ConstantDate      RDate
              | ConstantTime      RTime
              | ConstantInterval  RInterval
              -- TODO: Support Map and Arr
              deriving (Eq, Show)

data BinaryOp = OpAdd | OpSub | OpMul deriving (Eq, Show)

data Aggregate = Nullary NullaryAggregate
               | Unary   UnaryAggregate RValueExpr
               -- TODO: Add BinaryAggregate if needed
               deriving (Eq, Show)

data NullaryAggregate = AggCountAll deriving (Eq, Show)
data UnaryAggregate   = AggCount
                      | AggAvg
                      | AggSum
                      | AggMax
                      | AggMin
                      deriving (Eq, Show)

type ExprName = String
data RValueExpr = RExprCol       ExprName (Maybe StreamName) FieldName
                | RExprConst     ExprName Constant
                | RExprAggregate ExprName Aggregate
                | RExprBinOp     ExprName BinaryOp RValueExpr RValueExpr
                -- TODO: Add UnaryOp if needed
                deriving (Eq, Show)

type instance RefinedType (ValueExpr a) = RValueExpr
instance Refine (ValueExpr Position) where -- FIXME: Inconsistent form (Position instead of a)
  refine expr@(ExprAdd _ e1 e2)         = RExprBinOp  (printTree expr) OpAdd (refine e1) (refine e2)
  refine expr@(ExprSub _ e1 e2)         = RExprBinOp  (printTree expr) OpSub (refine e1) (refine e2)
  refine expr@(ExprMul _ e1 e2)         = RExprBinOp  (printTree expr) OpMul (refine e1) (refine e2)
  refine expr@(ExprInt _ n)             = RExprConst (printTree expr) (ConstantInt $ fromInteger n) -- WARNING: May lose presision
  refine expr@(ExprNum _ n)             = RExprConst (printTree expr) (ConstantNum n)
  refine expr@(ExprString _ s)          = RExprConst (printTree expr) (ConstantString s)
  refine expr@(ExprDate _ date)         = RExprConst (printTree expr) (ConstantDate $ refine date)
  refine expr@(ExprTime _ time)         = RExprConst (printTree expr) (ConstantTime $ refine time)
  refine expr@(ExprInterval _ interval) = RExprConst (printTree expr) (ConstantInterval $ refine interval)
  refine expr@(ExprColName _ (ColNameSimple _ (Ident t))) = RExprCol (printTree expr) Nothing t
  refine expr@(ExprColName _ (ColNameStream _ (Ident s) (Ident f))) = RExprCol (printTree expr) (Just s) f
  refine      (ExprColName pos ColNameIndex{}) = throwSQLException RefineException pos "Nested column name is not supported yet"
  refine      (ExprColName pos ColNameInner{}) = throwSQLException RefineException pos "Nested column name is not supported yet"
  refine expr@(ExprSetFunc _ (SetFuncCountAll _)) = RExprAggregate (printTree expr) (Nullary AggCountAll)
  refine expr@(ExprSetFunc _ (SetFuncCount _ e )) = RExprAggregate (printTree expr) (Unary AggCount $ refine e)
  refine expr@(ExprSetFunc _ (SetFuncAvg _ e )) = RExprAggregate (printTree expr) (Unary AggAvg $ refine e)
  refine expr@(ExprSetFunc _ (SetFuncSum _ e )) = RExprAggregate (printTree expr) (Unary AggSum $ refine e)
  refine expr@(ExprSetFunc _ (SetFuncMax _ e )) = RExprAggregate (printTree expr) (Unary AggMax $ refine e)
  refine expr@(ExprSetFunc _ (SetFuncMin _ e )) = RExprAggregate (printTree expr) (Unary AggMin $ refine e)
  refine      (ExprArr pos _) = throwSQLException RefineException pos "Array constant is not supported yet"
  refine      (ExprMap pos _) = throwSQLException RefineException pos "Map constant is not supported yet"

---- Sel
type FieldAlias = String
data RSel = RSelAsterisk
          | RSelList [(RValueExpr, FieldAlias)]
          | RSelAggregate Aggregate FieldAlias -- FIXME: Not that natural?
          deriving (Eq, Show)

type instance RefinedType (DerivedCol a) = Either (RValueExpr, FieldAlias) (Aggregate, FieldAlias)
instance Refine (DerivedCol Position) where
  refine (DerivedColSimpl _ expr) = case refine expr of
    RExprAggregate    exprName agg    -> Right (agg, exprName)
    rexpr@(RExprCol   exprName _ _  ) -> Left  (rexpr, exprName)
    rexpr@(RExprConst exprName _    ) -> Left  (rexpr, exprName)
    rexpr@(RExprBinOp exprName _ _ _) -> Left  (rexpr, exprName)
  refine (DerivedColAs pos expr (Ident t)) = case refine (DerivedColSimpl pos expr) of
    Left  (rexpr, _) -> Left  (rexpr, Text.unpack t)
    Right (agg, _)   -> Right (agg, Text.unpack t)

type instance RefinedType (Sel a) = RSel
instance Refine (Sel Position) where
  refine (DSel _ (SelListAsterisk _)) = RSelAsterisk
  refine (DSel _ (SelListSublist pos cols))
    | anyAgg = case L.head rcols of -- NOTE: Ensured by Validate: if there is agg, there is only one
                 Right (agg, alias) -> RSelAggregate agg alias
                 Left _             -> throwSQLException RefineException pos "Impossible happened"
    | otherwise  = RSelList (lefts rcols)
    where rcols  = refine <$> cols
          anyAgg = L.any isRight rcols

---- Frm
data RJoinType = RJoinInner | RJoinLeft | RJoinOuter deriving (Eq, Show)
type instance RefinedType (JoinType a) = RJoinType
instance Refine (JoinType Position) where
  refine (JoinInner  _)   = RJoinInner
  refine (JoinLeft  pos)  = throwSQLException RefineException pos "LEFT JOIN is not supported yet" -- TODO: RJoinLeft
  refine (JoinOuter pos)  = throwSQLException RefineException pos "LEFT JOIN is not supported yet" -- TODO: RJoinOuter

-- TODO: Defined a RJoinWindow type to describe different windows (symmetry, left, right, ...) ?
type RJoinWindow = RInterval
type instance RefinedType (JoinWindow a) = RInterval
instance Refine (JoinWindow a) where
  refine (DJoinWindow _ interval) = refine interval

type instance RefinedType (JoinCond a) = RSearchCond
instance Refine (JoinCond Position) where
  refine (DJoinCond _ cond) = refine cond

-- TODO: Stream alias is not supported yet
data RFrom = RFromSingle StreamName
           | RFromJoin   (StreamName,FieldName) (StreamName,FieldName) RJoinType RJoinWindow
           deriving (Eq, Show)
type instance RefinedType (From a) = RFrom

-- Note: Ensured by Validate: only the following situations are allowed:
--       1. stream1
--       2. stream1 `JOIN` stream2
--       Ensured by Validate: stream names in JOIN ON and FROM match
instance Refine (From Position) where
  refine (DFrom _ [TableRefSimple _ (Ident t)]) = RFromSingle t
  refine (DFrom pos [
                   TableRefJoin _
                   (TableRefSimple _ (Ident t1))
                   joinType
                   (TableRefSimple _ (Ident t2))
                   win
                   cond
                  ]) =
    case refine cond of
      (RCondOp RCompOpEQ (RExprCol _ (Just s1) f1) (RExprCol _ (Just s2) f2)) ->
        case t1 == s1 of
          True  -> RFromJoin (t1,f1) (t2,f2) (refine joinType) (refine win)
          False -> RFromJoin (t1,f2) (t2,f1) (refine joinType) (refine win)
      _ -> throwSQLException RefineException pos "Impossible happened"
  refine (DFrom _ [TableRefAs pos _ _]) = throwSQLException RefineException pos "Stream alias is not supported yet"
  refine (DFrom pos _) = throwSQLException RefineException pos "Impossible happened"

---- Whr
data RCompOp = RCompOpEQ | RCompOpNE | RCompOpLT | RCompOpGT | RCompOpLEQ | RCompOpGEQ deriving (Eq, Show)
type instance RefinedType (CompOp a) = RCompOp
instance Refine (CompOp a) where
  refine (CompOpEQ  _) = RCompOpEQ
  refine (CompOpNE  _) = RCompOpNE
  refine (CompOpLT  _) = RCompOpLT
  refine (CompOpGT  _) = RCompOpGT
  refine (CompOpLEQ _) = RCompOpLEQ
  refine (CompOpGEQ _) = RCompOpGEQ

-- NOTE: Ensured by Validate: no aggregate expression
data RSearchCond = RCondOr      RSearchCond RSearchCond
                 | RCondAnd     RSearchCond RSearchCond
                 | RCondNot     RSearchCond
                 | RCondOp      RCompOp RValueExpr RValueExpr
                 | RCondBetween RValueExpr RValueExpr RValueExpr
                 deriving (Eq, Show)
type instance RefinedType (SearchCond a) = RSearchCond
instance Refine (SearchCond Position) where
  refine (CondOr      _ c1 c2)    = RCondOr  (refine c1) (refine c2)
  refine (CondAnd     _ c1 c2)    = RCondAnd (refine c1) (refine c2)
  refine (CondNot     _ c)        = RCondNot (refine c)
  refine (CondOp      _ e1 op e2) = RCondOp (refine op) (refine e1) (refine e2)
  refine (CondBetween _ e1 e e2)  = RCondBetween (refine e1) (refine e) (refine e2)

data RWhere = RWhereEmpty
            | RWhere RSearchCond
            deriving (Eq, Show)
type instance RefinedType (Where a) = RWhere
instance Refine (Where Position) where
  refine (DWhereEmpty _) = RWhereEmpty
  refine (DWhere _ cond) = RWhere (refine cond)

---- Grp
data RWindow = RTumblingWindow RInterval
             | RHoppingWIndow  RInterval RInterval
             | RSessionWindow  RInterval
             deriving (Eq, Show)
type instance RefinedType (Window a) = RWindow
instance Refine (Window a) where
  refine (TumblingWindow _ interval) = RTumblingWindow (refine interval)
  refine (HoppingWindow  _ len hop ) = RHoppingWIndow (refine len) (refine hop)
  refine (SessionWindow  _ interval) = RSessionWindow (refine interval)

data RGroupBy = RGroupByEmpty
              | RGroupBy (Maybe StreamName) FieldName (Maybe RWindow)
              deriving (Eq, Show)
type instance RefinedType (GroupBy a) = RGroupBy
instance Refine (GroupBy Position) where
  refine (DGroupByEmpty _) = RGroupByEmpty
  refine (DGroupBy _ [GrpItemCol _ col]) =
    case col of
      ColNameSimple _ (Ident f)           -> RGroupBy Nothing f Nothing
      ColNameStream _ (Ident s) (Ident f) -> RGroupBy (Just s) f Nothing
      _                                   -> throwSQLException RefineException Nothing "Impossible happened" -- Index and Inner is not supportede
  refine (DGroupBy _ [GrpItemCol _ col, GrpItemWin _ win]) =
    case col of
      ColNameSimple _ (Ident f)           -> RGroupBy Nothing f (Just $ refine win)
      ColNameStream _ (Ident s) (Ident f) -> RGroupBy (Just s) f (Just $ refine win)
      _                                   -> throwSQLException RefineException Nothing "Impossible happened" -- Index and Inner is not supportede

---- Hav
data RHaving = RHavingEmpty
             | RHaving RSearchCond
             deriving (Eq, Show)
type instance RefinedType (Having a) = RHaving
instance Refine (Having Position) where
  refine (DHavingEmpty _) = RHavingEmpty
  refine (DHaving _ cond) = RHaving (refine cond)

---- SELECT
data RSelect = RSelect RSel RFrom RWhere RGroupBy RHaving deriving (Eq, Show)
type instance RefinedType (Select a) = RSelect
instance Refine (Select Position) where
  refine (DSelect _ sel frm whr grp hav) =
    RSelect (refine sel) (refine frm) (refine whr) (refine grp) (refine hav)

---- CREATE
data StreamFormat = FormatJSON deriving (Eq, Show)
data RStreamOptions = RStreamOptions
  { rStreamFormat :: StreamFormat
  } deriving (Eq, Show)
data RCreate = RCreate   Text RStreamOptions
             | RCreateAs Text RSelect RStreamOptions
             deriving (Eq, Show)

type instance RefinedType [StreamOption a] = RStreamOptions
instance Refine [StreamOption a] where
  refine [OptionFormat _ format] = RStreamOptions (refineFormat format)
    where refineFormat "json" = FormatJSON
          refineFormat "JSON" = FormatJSON
          refineFormat _      = throwSQLException RefineException Nothing "Impossible happened"
  refine _ = throwSQLException RefineException Nothing "Impossible happened"

type instance RefinedType (Create a) = RCreate
instance Refine (Create Position) where
  refine (DCreate  _ (Ident s) options)        = RCreate   s (refine options)
  refine (CreateAs _ (Ident s) select options) = RCreateAs s (refine select) (refine options)

---- INSERT
data RInsert = RInsert Text [(FieldName,Constant)] deriving (Eq, Show)
type instance RefinedType (Insert a) = RInsert
instance Refine (Insert Position) where
  refine (DInsert _ (Ident s) fields exprs) = RInsert s $
    zip ((\(Ident f) -> f) <$> fields) (refineConst <$> exprs)
    where
      refineConst expr =
        let (RExprConst _ constant) = refine expr -- Ensured by Validate
         in constant

---- SQL
data RSQL = RQSelect RSelect
          | RQCreate RCreate
          | RQInsert RInsert
          deriving (Eq, Show)
type instance RefinedType (SQL a) = RSQL
instance Refine (SQL Position) where
  refine (QSelect _ select) = RQSelect (refine select)
  refine (QCreate _ create) = RQCreate (refine create)
  refine (QInsert _ insert) = RQInsert (refine insert)
