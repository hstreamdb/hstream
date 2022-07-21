{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.AST where

import qualified Data.Aeson            as Aeson
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as BSC
import           Data.Functor
import qualified Data.HashMap.Strict   as HM
import           Data.Kind             (Type)
import           Data.Text             (Text)
import qualified Data.Text             as Text
import           Data.Text.Encoding    (encodeUtf8)
import qualified Data.Time             as Time
import           GHC.Stack             (HasCallStack)
import           HStream.SQL.Abs
import           HStream.SQL.Exception (SomeSQLException (..),
                                        throwSQLException)
import           HStream.SQL.Extra     (extractPNDouble, extractPNInteger,
                                        trimSpacesPrint)
import           HStream.SQL.Print     (printTree)

--------------------------------------------------------------------------------
type family RefinedType a :: Type

class Refine a where
  refine :: HasCallStack => a -> RefinedType a

--------------------------------------------------------------------------------
type StreamName = Text
type FieldName  = Text
type ConnectorType = Text

type instance RefinedType PNInteger = Integer
instance Refine PNInteger where
  refine = extractPNInteger

type instance RefinedType PNDouble = Double
instance Refine PNDouble where
  refine = extractPNDouble

type instance RefinedType SString = BS.ByteString
instance Refine SString where
  refine (SString t) = encodeUtf8 . Text.init . Text.tail $ t

type instance RefinedType RawColumn = Text
instance Refine RawColumn where
  refine (RawColumn t) = Text.init . Text.tail $ t

type RBool = Bool
type instance RefinedType Boolean = RBool
instance Refine Boolean where
  refine (BoolTrue _ ) = True
  refine (BoolFalse _) = False

type RDate = Time.Day
type instance RefinedType Date = RDate
instance Refine Date where
  refine (DDate _ year month day) = Time.fromGregorian (refine year) (fromInteger $ refine month) (fromInteger $ refine day)

type RTime = Time.DiffTime
type instance RefinedType Time = RTime
instance Refine Time where
  refine (DTime _ hour minute second) = Time.secondsToDiffTime $
    (refine hour) * 3600 + (refine minute) * 60 + (refine second)

type RInterval = Time.DiffTime
type instance RefinedType Interval = RInterval
instance Refine Interval where
  refine (DInterval _ n (TimeUnitSec _  )) = Time.secondsToDiffTime $ refine n
  refine (DInterval _ n (TimeUnitMin _  )) = Time.secondsToDiffTime $ (refine n) * 60
  refine (DInterval _ n (TimeUnitDay _  )) = Time.secondsToDiffTime $ (refine n) * 60 * 24
  refine (DInterval _ n (TimeUnitWeek _ )) = Time.secondsToDiffTime $ (refine n) * 60 * 24 * 7
  refine (DInterval _ n (TimeUnitMonth _)) = Time.secondsToDiffTime $ (refine n) * 60 * 24 * 30
  refine (DInterval _ n (TimeUnitYear _ )) = Time.secondsToDiffTime $ (refine n) * 60 * 24 * 365

data Constant = ConstantNull
              | ConstantInt       Int
              | ConstantNum       Double
              | ConstantString    String
              | ConstantBool      Bool
              | ConstantDate      RDate
              | ConstantTime      RTime
              | ConstantInterval  RInterval
              -- TODO: Support Map and Arr
              deriving (Eq, Show)

instance Aeson.ToJSON Constant where
  toJSON ConstantNull       = Aeson.Null
  toJSON (ConstantInt v)    = Aeson.toJSON v
  toJSON (ConstantNum v)    = Aeson.toJSON v
  toJSON (ConstantString v) = Aeson.toJSON v
  toJSON (ConstantBool v)   = Aeson.toJSON v

data BinaryOp = OpAdd | OpSub | OpMul
              | OpAnd | OpOr
              | OpContain | OpExcept  | OpIntersect | OpRemove | OpUnion | OpArrJoin'
              | OpIfNull  | OpNullIf  | OpDateStr   | OpStrDate
              | OpSplit   | OpChunksOf
              | OpTake    | OpTakeEnd | OpDrop      | OpDropEnd
              deriving (Eq, Show)

data UnaryOp  = OpSin      | OpSinh    | OpAsin   | OpAsinh  | OpCos   | OpCosh
              | OpAcos     | OpAcosh   | OpTan    | OpTanh   | OpAtan  | OpAtanh
              | OpAbs      | OpCeil    | OpFloor  | OpRound  | OpSign
              | OpSqrt     | OpLog     | OpLog2   | OpLog10  | OpExp
              | OpIsInt    | OpIsFloat | OpIsNum  | OpIsBool | OpIsStr | OpIsMap
              | OpIsArr    | OpIsDate  | OpIsTime
              | OpToStr
              | OpToLower  | OpToUpper | OpTrim   | OpLTrim  | OpRTrim
              | OpReverse  | OpStrLen
              | OpDistinct | OpArrJoin | OpLength | OpArrMax | OpArrMin | OpSort
              deriving (Eq, Show)

data Aggregate = Nullary NullaryAggregate
               | Unary   UnaryAggregate  RValueExpr
               | Binary  BinaryAggregate RValueExpr RValueExpr
               deriving (Eq, Show)

data NullaryAggregate = AggCountAll deriving (Eq, Show)
data UnaryAggregate   = AggCount
                      | AggAvg
                      | AggSum
                      | AggMax
                      | AggMin
                      deriving (Eq, Show)
data BinaryAggregate = AggTopK | AggTopKDistinct
                     deriving (Eq, Show)

type ExprName = String
data RValueExpr = RExprCol       ExprName (Maybe StreamName) FieldName
                | RExprConst     ExprName Constant
                | RExprAggregate ExprName Aggregate
                | RExprBinOp     ExprName BinaryOp RValueExpr RValueExpr
                | RExprUnaryOp   ExprName UnaryOp  RValueExpr
                deriving (Eq, Show)

type instance RefinedType ValueExpr = RValueExpr
instance Refine ValueExpr where -- FIXME: Inconsistent form (Position instead of a)
  refine expr = case expr of
    (ExprAdd _ e1 e2)         -> RExprBinOp (trimSpacesPrint expr) OpAdd (refine e1) (refine e2)
    (ExprSub _ e1 e2)         -> RExprBinOp (trimSpacesPrint expr) OpSub (refine e1) (refine e2)
    (ExprMul _ e1 e2)         -> RExprBinOp (trimSpacesPrint expr) OpMul (refine e1) (refine e2)
    (ExprAnd _ e1 e2)         -> RExprBinOp (trimSpacesPrint expr) OpAnd (refine e1) (refine e2)
    (ExprOr  _ e1 e2)         -> RExprBinOp (trimSpacesPrint expr) OpOr  (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncIfNull   _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpIfNull    (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncNullIf   _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpNullIf    (refine e1) (refine e2)
    (ExprScalarFunc _ (ArrayFuncContain   _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpContain   (refine e1) (refine e2)
    (ExprScalarFunc _ (ArrayFuncExcept    _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpExcept    (refine e1) (refine e2)
    (ExprScalarFunc _ (ArrayFuncIntersect _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpIntersect (refine e1) (refine e2)
    (ExprScalarFunc _ (ArrayFuncRemove    _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpRemove    (refine e1) (refine e2)
    (ExprScalarFunc _ (ArrayFuncUnion     _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpUnion     (refine e1) (refine e2)
    (ExprScalarFunc _ (ArrayFuncJoinWith  _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpArrJoin'  (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncDateStr  _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpDateStr   (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncStrDate  _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpStrDate   (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncSplit    _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpSplit     (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncChunksOf _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpChunksOf  (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncTake     _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpTake      (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncTakeEnd  _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpTakeEnd   (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncDrop     _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpDrop      (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncDropEnd  _ e1 e2)) -> RExprBinOp (trimSpacesPrint expr) OpDropEnd   (refine e1) (refine e2)
    (ExprInt _ n)             -> RExprConst (trimSpacesPrint expr) (ConstantInt . fromInteger . refine $ n) -- WARNING: May lose presision
    (ExprNum _ n)             -> RExprConst (trimSpacesPrint expr) (ConstantNum $ refine n)
    (ExprString _ s)          -> RExprConst (trimSpacesPrint expr) (ConstantString s)
    (ExprRaw _ s)             -> RExprCol (Text.unpack $ refine s) Nothing (refine s) -- WARNING: Spaces are not trimmed
    (ExprNull _)              -> RExprConst (trimSpacesPrint expr) (ConstantNull)
    (ExprBool _ b)            -> RExprConst (trimSpacesPrint expr) (ConstantBool $ refine b)
    (ExprDate _ date)         -> RExprConst (trimSpacesPrint expr) (ConstantDate $ refine date)
    (ExprTime _ time)         -> RExprConst (trimSpacesPrint expr) (ConstantTime $ refine time)
    (ExprInterval _ interval) -> RExprConst (trimSpacesPrint expr) (ConstantInterval $ refine interval)
    (ExprColName _ (ColNameSimple _ (Ident t))) -> RExprCol (trimSpacesPrint expr) Nothing t
    (ExprColName _ (ColNameStream _ (Ident s) (Ident f))) -> RExprCol (trimSpacesPrint expr) (Just s) f
    (ExprColName pos ColNameIndex{}) -> throwSQLException RefineException pos "Nested column name is not supported yet"
    (ExprColName pos ColNameInner{}) -> throwSQLException RefineException pos "Nested column name is not supported yet"
    (ExprSetFunc _ (SetFuncCountAll _)) -> RExprAggregate (trimSpacesPrint expr) (Nullary AggCountAll)
    (ExprSetFunc _ (SetFuncCount _ e )) -> RExprAggregate (trimSpacesPrint expr) (Unary AggCount $ refine e)
    (ExprSetFunc _ (SetFuncAvg _ e )) -> RExprAggregate (trimSpacesPrint expr) (Unary AggAvg $ refine e)
    (ExprSetFunc _ (SetFuncSum _ e )) -> RExprAggregate (trimSpacesPrint expr) (Unary AggSum $ refine e)
    (ExprSetFunc _ (SetFuncMax _ e )) -> RExprAggregate (trimSpacesPrint expr) (Unary AggMax $ refine e)
    (ExprSetFunc _ (SetFuncMin _ e )) -> RExprAggregate (trimSpacesPrint expr) (Unary AggMin $ refine e)
    (ExprSetFunc _ (SetFuncTopK         _ e1 e2)) -> RExprAggregate (trimSpacesPrint expr) (Binary AggTopK         (refine e1) (refine e2))
    (ExprSetFunc _ (SetFuncTopKDistinct _ e1 e2)) -> RExprAggregate (trimSpacesPrint expr) (Binary AggTopKDistinct (refine e1) (refine e2))
    (ExprArr pos _) -> throwSQLException RefineException pos "Array constant is not supported yet"
    (ExprMap pos _) -> throwSQLException RefineException pos "Map constant is not supported yet"
    (ExprScalarFunc _ (ScalarFuncSin     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpSin     (refine e)
    (ExprScalarFunc _ (ScalarFuncSinh    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpSinh    (refine e)
    (ExprScalarFunc _ (ScalarFuncAsin    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAsin    (refine e)
    (ExprScalarFunc _ (ScalarFuncAsinh   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAsinh   (refine e)
    (ExprScalarFunc _ (ScalarFuncCos     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpCos     (refine e)
    (ExprScalarFunc _ (ScalarFuncCosh    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpCosh    (refine e)
    (ExprScalarFunc _ (ScalarFuncAcos    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAcos    (refine e)
    (ExprScalarFunc _ (ScalarFuncAcosh   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAcosh   (refine e)
    (ExprScalarFunc _ (ScalarFuncTan     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpTan     (refine e)
    (ExprScalarFunc _ (ScalarFuncTanh    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpTanh    (refine e)
    (ExprScalarFunc _ (ScalarFuncAtan    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAtan    (refine e)
    (ExprScalarFunc _ (ScalarFuncAtanh   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAtanh   (refine e)
    (ExprScalarFunc _ (ScalarFuncAbs     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpAbs     (refine e)
    (ExprScalarFunc _ (ScalarFuncCeil    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpCeil    (refine e)
    (ExprScalarFunc _ (ScalarFuncFloor   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpFloor   (refine e)
    (ExprScalarFunc _ (ScalarFuncRound   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpRound   (refine e)
    (ExprScalarFunc _ (ScalarFuncSign    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpSign    (refine e)
    (ExprScalarFunc _ (ScalarFuncSqrt    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpSqrt    (refine e)
    (ExprScalarFunc _ (ScalarFuncLog     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpLog     (refine e)
    (ExprScalarFunc _ (ScalarFuncLog2    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpLog2    (refine e)
    (ExprScalarFunc _ (ScalarFuncLog10   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpLog10   (refine e)
    (ExprScalarFunc _ (ScalarFuncExp     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpExp     (refine e)
    (ExprScalarFunc _ (ScalarFuncIsInt   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsInt   (refine e)
    (ExprScalarFunc _ (ScalarFuncIsFloat _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsFloat (refine e)
    (ExprScalarFunc _ (ScalarFuncIsNum   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsNum   (refine e)
    (ExprScalarFunc _ (ScalarFuncIsBool  _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsBool  (refine e)
    (ExprScalarFunc _ (ScalarFuncIsStr   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsStr   (refine e)
    (ExprScalarFunc _ (ScalarFuncIsMap   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsMap   (refine e)
    (ExprScalarFunc _ (ScalarFuncIsArr   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsArr   (refine e)
    (ExprScalarFunc _ (ScalarFuncIsDate  _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsDate  (refine e)
    (ExprScalarFunc _ (ScalarFuncIsTime  _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpIsTime  (refine e)
    (ExprScalarFunc _ (ScalarFuncToStr   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpToStr   (refine e)
    (ExprScalarFunc _ (ScalarFuncToLower _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpToLower (refine e)
    (ExprScalarFunc _ (ScalarFuncToUpper _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpToUpper (refine e)
    (ExprScalarFunc _ (ScalarFuncTrim    _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpTrim    (refine e)
    (ExprScalarFunc _ (ScalarFuncLTrim   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpLTrim   (refine e)
    (ExprScalarFunc _ (ScalarFuncRTrim   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpRTrim   (refine e)
    (ExprScalarFunc _ (ScalarFuncRev     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpReverse (refine e)
    (ExprScalarFunc _ (ScalarFuncStrlen  _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpStrLen  (refine e)
    (ExprScalarFunc _ (ArrayFuncDistinct _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpDistinct(refine e)
    (ExprScalarFunc _ (ArrayFuncLength   _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpLength  (refine e)
    (ExprScalarFunc _ (ArrayFuncJoin     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpArrJoin (refine e)
    (ExprScalarFunc _ (ArrayFuncMax      _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpArrMax  (refine e)
    (ExprScalarFunc _ (ArrayFuncMin      _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpArrMin  (refine e)
    (ExprScalarFunc _ (ArrayFuncSort     _ e)) -> RExprUnaryOp (trimSpacesPrint expr) OpSort    (refine e)

---- Sel
type FieldAlias = String
data RSel = RSelAsterisk
          | RSelList [(Either RValueExpr Aggregate, FieldAlias)]
          deriving (Eq, Show)

type instance RefinedType DerivedCol = (Either RValueExpr Aggregate, FieldAlias)
instance Refine DerivedCol where
  refine (DerivedColSimpl _ expr) = case refine expr of
    RExprAggregate    exprName agg    -> (Right agg,   exprName)
    rexpr@(RExprCol   exprName _ _  ) -> (Left  rexpr, exprName)
    rexpr@(RExprConst exprName _    ) -> (Left  rexpr, exprName)
    rexpr@(RExprBinOp exprName _ _ _) -> (Left  rexpr, exprName)
    rexpr@(RExprUnaryOp exprName _ _) -> (Left  rexpr, exprName)
  refine (DerivedColAs pos expr (Ident t)) = case refine (DerivedColSimpl pos expr) of
    (Left  rexpr, _) -> (Left  rexpr, Text.unpack t)
    (Right agg,   _) -> (Right agg,   Text.unpack t)

type instance RefinedType Sel = RSel
instance Refine Sel where
  refine (DSel _ (SelListAsterisk _))     = RSelAsterisk
  refine (DSel _ (SelListSublist _ cols)) = RSelList $ refine <$> cols

---- Frm
data RTableRef = RTableRefSimple StreamName (Maybe StreamName)
               | RTableRefSubquery RSelect (Maybe StreamName)
               | RTableRefUnion RTableRef RTableRef (Maybe StreamName)
               deriving (Eq, Show)
type instance RefinedType TableRef = RTableRef
instance Refine TableRef where
  refine (TableRefSimple _ (Ident t)) = RTableRefSimple t Nothing
  refine (TableRefSubquery _ select) = RTableRefSubquery (refine select) Nothing
  refine (TableRefUnion _ ref1 ref2) = RTableRefUnion (refine ref1) (refine ref2) Nothing
  refine (TableRefAs _ (TableRefSimple _ (Ident t)) (Ident alias)) = RTableRefSimple t (Just alias)
  refine (TableRefAs _ (TableRefSubquery _ select) (Ident alias)) = RTableRefSubquery (refine select) (Just alias)
  refine (TableRefAs _ (TableRefUnion _ ref1 ref2) (Ident alias)) = RTableRefUnion (refine ref1) (refine ref2) (Just alias)

data RFrom = RFrom [RTableRef] deriving (Eq, Show)
type instance RefinedType From = RFrom
instance Refine From where
  refine (DFrom pos refs) = RFrom (refine <$> refs)

---- Whr
data RCompOp = RCompOpEQ | RCompOpNE | RCompOpLT | RCompOpGT | RCompOpLEQ | RCompOpGEQ deriving (Eq, Show)
type instance RefinedType CompOp = RCompOp
instance Refine CompOp where
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
type instance RefinedType SearchCond = RSearchCond
instance Refine SearchCond where
  refine (CondOr      _ c1 c2)    = RCondOr  (refine c1) (refine c2)
  refine (CondAnd     _ c1 c2)    = RCondAnd (refine c1) (refine c2)
  refine (CondNot     _ c)        = RCondNot (refine c)
  refine (CondOp      _ e1 op e2) = RCondOp (refine op) (refine e1) (refine e2)
  refine (CondBetween _ e1 e e2)  = RCondBetween (refine e1) (refine e) (refine e2)

data RWhere = RWhereEmpty
            | RWhere RSearchCond
            deriving (Eq, Show)
type instance RefinedType Where = RWhere
instance Refine Where where
  refine (DWhereEmpty _) = RWhereEmpty
  refine (DWhere _ cond) = RWhere (refine cond)

---- Grp
data RWindow = RTumblingWindow RInterval
             | RHoppingWIndow  RInterval RInterval
             | RSessionWindow  RInterval
             deriving (Eq, Show)
type instance RefinedType Window = RWindow
instance Refine Window where
  refine (TumblingWindow _ interval) = RTumblingWindow (refine interval)
  refine (HoppingWindow  _ len hop ) = RHoppingWIndow (refine len) (refine hop)
  refine (SessionWindow  _ interval) = RSessionWindow (refine interval)

data RGroupBy = RGroupByEmpty
              | RGroupBy (Maybe StreamName) FieldName (Maybe RWindow)
              deriving (Eq, Show)
type instance RefinedType GroupBy = RGroupBy
instance Refine GroupBy where
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
  refine _ = throwSQLException RefineException Nothing "Impossible happened"

---- Hav
data RHaving = RHavingEmpty
             | RHaving RSearchCond
             deriving (Eq, Show)
type instance RefinedType Having = RHaving
instance Refine Having where
  refine (DHavingEmpty _) = RHavingEmpty
  refine (DHaving _ cond) = RHaving (refine cond)

---- SELECT
data RSelect = RSelect RSel RFrom RWhere RGroupBy RHaving deriving (Eq, Show)
type instance RefinedType Select = RSelect
instance Refine Select where
  refine (DSelect _ sel frm whr grp hav) =
    RSelect (refine sel) (refine frm) (refine whr) (refine grp) (refine hav)

---- SELECTVIEW

data SelectViewSelect = SVSelectAll | SVSelectFields [(FieldName, FieldAlias)] deriving (Eq, Show)
type SelectViewCond = (FieldName, RValueExpr)
data RSelectView = RSelectView
  { rSelectViewSelect :: SelectViewSelect
  , rSelectViewFrom   :: StreamName
  , rSelectViewWhere  :: SelectViewCond
  } deriving (Eq, Show)

type instance RefinedType SelectView = RSelectView
instance Refine SelectView where
  refine (DSelectView _ sel frm whr) =
    RSelectView svSel svFrm svWhr
    where
      -- TODO: use `refine` instance of `Sel`
      svSel :: SelectViewSelect
      svSel = case sel of
        (DSel _ (SelListAsterisk _)) -> SVSelectAll
        (DSel _ (SelListSublist _ dcols)) ->
          let f :: DerivedCol -> (FieldName, FieldAlias)
              f docl = case docl of
                (DerivedColSimpl _ expr@(ExprColName _ (ColNameSimple _ (Ident col))))       ->
                  (col, trimSpacesPrint expr)
                (DerivedColSimpl _ expr@(ExprRaw _ (RawColumn col)))                         ->
                  (col, trimSpacesPrint expr)
                (DerivedColAs _ (ExprColName _ (ColNameSimple _ (Ident col))) (Ident alias)) ->
                  (col, Text.unpack alias)
                (DerivedColAs _ (ExprRaw _ (RawColumn col)) (Ident alias))                   ->
                  (col, Text.unpack alias)
           in SVSelectFields (f <$> dcols)
      svFrm = let (RFrom [RTableRefSimple stream Nothing]) = refine frm in stream
      svWhr = let (RWhere (RCondOp RCompOpEQ (RExprCol _ Nothing field) rexpr)) = refine whr
               in (field, rexpr)

---- EXPLAIN
type RExplain = Text
type instance RefinedType Explain = RExplain
instance Refine Explain where
  refine (ExplainSelect _ select)                = Text.pack (printTree select) <> ";"
  refine (ExplainCreate _ create@(CreateAs{}))   = Text.pack (printTree create) <> ";"
  refine (ExplainCreate _ create@(CreateAsOp{})) = Text.pack (printTree create) <> ";"
  refine (ExplainCreate _ create@(CreateView{})) = Text.pack (printTree create) <> ";"
  refine (ExplainCreate pos _)                   =
    throwSQLException RefineException pos "Impossible happened"

---- CREATE
data RStreamOptions = RStreamOptions
  { rRepFactor    :: Int
  } deriving (Eq, Show)

newtype RConnectorOptions = RConnectorOptions (HM.HashMap Text Aeson.Value)
  deriving (Eq, Show)

data RCreate = RCreate   Text RStreamOptions
             | RCreateAs Text RSelect RStreamOptions
             -- RCreateConnector <SOURCE|SINK> <Name> <Target> <EXISTS> <OPTIONS>
             | RCreateConnector Text Text Text Bool RConnectorOptions
             | RCreateView Text RSelect
             deriving (Eq, Show)

type instance RefinedType [StreamOption] = RStreamOptions
instance Refine [StreamOption] where
  refine [OptionRepFactor _ rep] = RStreamOptions (fromInteger $ refine rep)
  refine [] = RStreamOptions 3
  refine _ = throwSQLException RefineException Nothing "Impossible happened"

type instance RefinedType [ConnectorOption] = RConnectorOptions
instance Refine [ConnectorOption] where
  refine ps = RConnectorOptions $ foldr (insert . toPair) HM.empty ps
    where insert (k, v) = HM.insert k v
          toPair :: ConnectorOption -> (Text, Aeson.Value)
          toPair (ConnectorProperty _ key expr) = (Text.pack key, toValue (refine expr))
          toValue (RExprConst _ c) = Aeson.toJSON c

type instance RefinedType Create = RCreate
instance Refine Create where
  refine (DCreate  _ (Ident s)) = RCreate s $ refine ([] :: [StreamOption])
  refine (CreateOp _ (Ident s) options)  = RCreate s (refine options)
  refine (CreateAs   _ (Ident s) select) = RCreateAs s (refine select) (refine ([] :: [StreamOption]))
  refine (CreateAsOp _ (Ident s) select options) = RCreateAs s (refine select) (refine options)
  refine (CreateSourceConnector _ (Ident s) (Ident t) options) = RCreateConnector "SOURCE" s t False (refine options)
  refine (CreateSourceConnectorIf _ (Ident s) (Ident t) options) = RCreateConnector "SOURCE" s t True (refine options)
  refine (CreateSinkConnector _ (Ident s) (Ident t) options) = RCreateConnector "SINK" s t False (refine options)
  refine (CreateSinkConnectorIf _ (Ident s) (Ident t) options) = RCreateConnector "SINK" s t True (refine options)
  refine (CreateView _ (Ident s) select) = RCreateView s (refine select)

---- INSERT
data RInsert = RInsert Text [(FieldName,Constant)]
             | RInsertBinary Text BS.ByteString
             | RInsertJSON   Text BS.ByteString
             deriving (Eq, Show)
type instance RefinedType Insert = RInsert
instance Refine Insert where
  refine (DInsert _ (Ident s) fields exprs) = RInsert s $
    zip ((\(Ident f) -> f) <$> fields) (refineConst <$> exprs)
    where
      refineConst expr =
        let (RExprConst _ constant) = refine expr -- Ensured by Validate
         in constant
  refine (InsertBinary _ (Ident s) bin) = RInsertBinary s (BSC.pack bin)
  refine (InsertJson _ (Ident s) ss) =
    RInsertJSON s (refine $ ss)

---- SHOW
data RShow
  = RShow RShowOption
  deriving (Eq, Show)
instance Refine ShowQ where
  refine (DShow _ showOp) = RShow (refine showOp)
type instance RefinedType ShowQ = RShow

data RShowOption
  = RShowStreams
  | RShowQueries
  | RShowConnectors
  | RShowViews
  deriving (Eq, Show)
instance Refine ShowOption where
  refine (ShowStreams _)    = RShowStreams
  refine (ShowQueries _)    = RShowQueries
  refine (ShowViews   _)    = RShowViews
  refine (ShowConnectors _) = RShowConnectors
type instance RefinedType ShowOption = RShowOption

---- DROP
data RDrop
  = RDrop   RDropOption Text
  | RDropIf RDropOption Text
  deriving (Eq, Show)
instance Refine Drop where
  refine (DDrop  _ dropOp (Ident x)) = RDrop   (refine dropOp) x
  refine (DropIf _ dropOp (Ident x)) = RDropIf (refine dropOp) x
type instance RefinedType Drop = RDrop

data RDropOption
  = RDropConnector
  | RDropStream
  | RDropView
  deriving (Eq, Show)

instance Refine DropOption where
  refine (DropConnector _) = RDropConnector
  refine (DropStream _)    = RDropStream
  refine (DropView   _)    = RDropView
type instance RefinedType DropOption = RDropOption

---- Terminate
data RTerminate
  = RTerminateQuery String
  | RTerminateAll
  deriving (Eq, Show)
instance Refine Terminate where
  refine (TerminateQuery _ x) = RTerminateQuery (show x)
  refine (TerminateAll   _  ) = RTerminateAll
type instance RefinedType Terminate = RTerminate

---- Pause
newtype RPause = RPauseConnector Text
  deriving (Eq, Show)

type instance RefinedType Pause = RPause

instance Refine Pause where
  refine (PauseConnector _ (Ident name)) = RPauseConnector name

---- Resume
newtype RResume = RResumeConnector Text
  deriving (Eq, Show)

type instance RefinedType Resume = RResume

instance Refine Resume where
  refine (ResumeConnector _ (Ident name)) = RResumeConnector name

---- SQL
data RSQL = RQSelect      RSelect
          | RQCreate      RCreate
          | RQInsert      RInsert
          | RQShow        RShow
          | RQDrop        RDrop
          | RQTerminate   RTerminate
          | RQSelectView  RSelectView
          | RQExplain     RExplain
          | RQPause       RPause
          | RQResume      RResume
          deriving (Eq, Show)
type instance RefinedType SQL = RSQL
instance Refine SQL where
  refine (QSelect     _ select)  =  RQSelect      (refine   select)
  refine (QCreate     _ create)  =  RQCreate      (refine   create)
  refine (QInsert     _ insert)  =  RQInsert      (refine   insert)
  refine (QShow       _ show_)   =  RQShow        (refine    show_)
  refine (QDrop       _ drop_)   =  RQDrop        (refine    drop_)
  refine (QTerminate  _ term)    =  RQTerminate   (refine     term)
  refine (QSelectView _ selView) =  RQSelectView  (refine  selView)
  refine (QExplain    _ explain) =  RQExplain     (refine  explain)
  refine (QPause      _ pause)   =  RQPause       (refine  pause)
  refine (QResume     _ resume)  =  RQResume      (refine  resume)

--------------------------------------------------------------------------------

throwImpossible :: a
throwImpossible = throwSQLException RefineException Nothing "Impossible happened"
