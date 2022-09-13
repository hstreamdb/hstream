{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}

module HStream.SQL.AST where

import qualified Data.Aeson            as Aeson
import qualified Data.ByteString       as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.HashMap.Strict   as HM
import           Data.Kind             (Type)
import qualified Data.List as L
import qualified Data.Map.Strict       as Map
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
data RDataType
  = RTypeInteger | RTypeFloat | RTypeNumeric | RTypeBoolean
  | RTypeBytea | RTypeText | RTypeDate | RTypeTime | RTypeTimestamp
  | RTypeInterval | RTypeJsonb
  | RTypeArray RDataType | RTypeMap RDataType RDataType
  deriving (Show, Eq)

type instance RefinedType DataType = RDataType
instance Refine DataType where
  refine TypeInteger{} = RTypeInteger
  refine TypeFloat{} = RTypeFloat
  refine TypeNumeric{} = RTypeNumeric
  refine TypeBoolean{} = RTypeBoolean
  refine TypeByte{} = RTypeBytea
  refine TypeText{} = RTypeText
  refine TypeDate{} = RTypeDate
  refine TypeTime{} = RTypeTime
  refine TypeTimestamp{} = RTypeTimestamp
  refine TypeInterval{} = RTypeInterval
  refine TypeJson{} = RTypeJsonb
  refine (TypeArray _ t) = RTypeArray (refine t)
  refine (TypeMap _ kt vt) = RTypeMap (refine kt) (refine vt)

--------------------------------------------------------------------------------
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

------- date & time -------
type RTimeStr = Time.TimeOfDay
type instance RefinedType TimeStr = RTimeStr
instance Refine TimeStr where
  refine (TimeStrWithoutMicroSec pos h m s) =
    case makeTimeOfDayValid h m (fromInteger s) of
      Nothing -> throwSQLException RefineException pos "invalid time"
      Just t  -> t

  refine (TimeStrWithMicroSec pos h m s ms) =
    case makeTimeOfDayValid h m (fromInteger s + (fromInteger ms) * 0.001) of
      Nothing -> throwSQLException RefineException pos "invalid time"
      Just t  -> t

type RDateStr = Time.Day
type instance RefinedType DateStr = RDateStr
instance Refine DateStr where
  refine (DDateStr pos y m d) =
    case Time.fromGregorianValid y (fromInteger m) (fromInteger d) of
      Nothing -> throwSQLException RefineException pos "invalid date"
      Just d  -> d

type RDateTimeStr = Time.LocalTime
type instance RefinedType DateTimeStr = RDateTimeStr
instance Refine DateTimeStr where
  refine (DDateTimeStr _ dateStr timeStr) =
    let timeOfDay = refine timeStr
        day       = refine dateStr
     in Time.LocalTime day timeOfDay

type RTimezone = Time.TimeZone
type instance RefinedType Timezone = RTimezone
instance Refine Timezone where
  refine (TimezoneZ _) = Time.minutesToTimeZone 0
  refine (TimezonePositive _ h m) = Time.minutesToTimeZone (h * 60 + m)
  refine (TimezoneNegative _ h m) = Time.minutesToTimeZone (- (h * 60 + m))

type RTimestampStr = Time.ZonedTime
type instance RefinedType TimestampStr = RTimestampStr
instance Refine TimestampStr where
  refine (DTimestampStr pos dateStr timeStr zone) =
    let localTime = refine (DDateTimeStr pos dateStr timeStr)
        timeZone  = refine zone
     in Time.ZonedTime localTime timeZone

type RDate = Time.Day
type instance RefinedType Date = RDate
instance Refine Date where
  refine (DDate _ dateStr) = refine dateStr

type RTime = Time.TimeOfDay
type instance RefinedType Time = RTime
instance Refine Time where
  refine (DTime _ timeStr) = refine timeStr

type RTimestamp = Time.ZonedTime
type instance RefinedType Timestamp = RTimestamp
instance Refine Timestamp where
  refine (TimestampWithoutZone _ dateTimeStr) =
    let localTime = refine dateTimeStr
        timeZone  = Time.utc
     in Time.ZonedTime localTime timeZone
  refine (TimestampWithZone _ tsStr) = refine tsStr

type RInterval = Time.CalendarDiffTime
type instance RefinedType Interval = RInterval
instance Refine Interval where
  refine (IntervalWithoutDate _ timeStr) =
    let nomialDiffTime = Time.daysAndTimeOfDayToTime 0 (refine timeStr)
     in Time.CalendarDiffTime 0 nomialDiffTime
  refine (IntervalWithDate _ (DDateTimeStr _ (DDateStr _ y m d) timeStr)) =
    let nomialDiffTime = Time.daysAndTimeOfDayToTime d (refine timeStr)
     in Time.CalendarDiffTime (12 * y +  m) nomialDiffTime

--------------------------------------------------------------------------------
data Constant = ConstantNull
              | ConstantInt       Int
              | ConstantFloat     Double
              | ConstantNumeric   Double
              | ConstantText      Text
              | ConstantBoolean   Bool
              | ConstantDate      RDate
              | ConstantTime      RTime
              | ConstantTimestamp RTimestamp
              | ConstantInterval  RInterval
              | ConstantBytea     BS.ByteString
              | ConstantJsonb     Aeson.Object
              | ConstantArray     [Constant]
              | ConstantMap       Map.Map Constant Constant
              deriving (Eq, Show)

{-
instance Aeson.ToJSON Constant where
  toJSON ConstantNull       = Aeson.Null
  toJSON (ConstantInt v)    = Aeson.toJSON v
  toJSON (ConstantNum v)    = Aeson.toJSON v
  toJSON (ConstantString v) = Aeson.toJSON v
  toJSON (ConstantBool v)   = Aeson.toJSON v
-}

data BinaryOp = OpAnd | OpOr
              | OpEQ | OpNEQ | OpLT | OpGT | OpLEQ | OpGEQ
              | OpAdd | OpSub | OpMul
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

data JsonOp
  = JOpArrow
  | JOpLongArrow
  | JOpHashArrow
  | JOpHashLongArrow
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

data RArrayAccessRhs
  = RArrayAccessRhsIndex Int
  | RArrayAccessRhsRange (Maybe Int) (Maybe Int)
  deriving (Show, Eq)
type instance RefinedType ArrayAccessRhs = RArrayAccessRhs
instance Refine ArrayAccessRhs where
  refine rhs = case rhs of
    ArrayAccessRhsIndex _ n -> RArrayAccessRhsIndex (fromInteger n)
    ArrayAccessRhsFrom _ n  -> RArrayAccessRhsRange (Just $ fromInteger n) Nothing
    ArrayAccessRhsTo _ n    -> RArrayAccessRhsRange Nothing (Just $ fromInteger n)
    ArrayAccessRhsFromTo _ n1 n2 -> RArrayAccessRhsRange (Just $ fromInteger n1) (Just $ fromInteger n2)

type instance RefinedType [LabelledValueExpr] = Map.Map RValueExpr RValueExpr
instance Refine [LabelledValueExpr] where
  refine ts = Map.fromList $
    L.map (\(DLabelledValueExpr _ ek ev) -> (refine ek, refine rv)) ts

type ExprName = String
type StreamName = Text
type FieldName  = Text
data RValueExpr = RExprCast        ExprName RValueExpr RDataType
                | RExprAccessMap   ExprName RValueExpr RValueExpr
                | RExprAccessArray ExprName RValueExpr RArrayAccessRhs
                | RExprCol         ExprName (Maybe StreamName) FieldName
                | RExprConst       ExprName Constant
                | RExprAggregate   ExprName Aggregate
                | RExprAccessJson  ExprName JsonOp RValueExpr RValueExpr
                | RExprBinOp       ExprName BinaryOp RValueExpr RValueExpr
                | RExprUnaryOp     ExprName UnaryOp  RValueExpr
                | RExprSubquery    ExprName RSelect
                deriving (Eq, Show)

type instance RefinedType ValueExpr = RValueExpr
instance Refine ValueExpr where
  refine expr = case expr of
    -- 1. Operations
    (ExprCast1 _ e typ) -> RExprCast (trimSpacesPrint expr) (refine e) (refine typ)
    (ExprCast2 _ e typ) -> RExprCast (trimSpacesPrint expr) (refine e) (refine typ)
    (ExprAnd _ e1 e2)   -> RExprBinOp (trimSpacesPrint expr) OpAnd (refine e1) (refine e2)
    (ExprOr  _ e1 e2)   -> RExprBinOp (trimSpacesPrint expr) OpOr  (refine e1) (refine e2)
    (ExprEQ _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpEQ (refine e1) (refine e2)
    (ExprNEQ _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpNEQ (refine e1) (refine e2)
    (ExprLT _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpLT (refine e1) (refine e2)
    (ExprEQ _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpGT (refine e1) (refine e2)
    (ExprLEQ _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpLEQ (refine e1) (refine e2)
    (ExprGEQ _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpGEQ (refine e1) (refine e2)
    (ExprAccessMap _ e1 e2) -> RExprAccessMap (trimSpacesPrint expr) (refine e1) (refine e2)
    (ExprAccessArray _ e rhs) -> RExprAccessArray (trimSpacesPrint expr) (refine e) (refine rhs)
    (ExprAdd _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpAdd (refine e1) (refine e2)
    (ExprSub _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpSub (refine e1) (refine e2)
    (ExprMul _ e1 e2) -> RExprBinOp (trimSpacesPrint expr) OpMul (refine e1) (refine e2)
    -- 2. Constants
    (ExprNull _)              -> RExprConst (trimSpacesPrint expr) ConstantNull
    (ExprInt _ n)             -> RExprConst (trimSpacesPrint expr) (ConstantInt . fromInteger . refine $ n)
    (ExprNum _ n)             -> RExprConst (trimSpacesPrint expr) (ConstantNum $ refine n)
    (ExprString _ s)          -> RExprConst (trimSpacesPrint expr) (ConstantText (Text.pack s))
    (ExprBool _ b)            -> RExprConst (trimSpacesPrint expr) (ConstantBoolean $ refine b)
    (ExprDate _ date)         -> RExprConst (trimSpacesPrint expr) (ConstantDate $ refine date)
    (ExprTime _ time)         -> RExprConst (trimSpacesPrint expr) (ConstantTime $ refine time)
    (ExprTimestamp _ ts) -> RExprConst (trimSpacesPrint expr) (ConstantTimestamp $ refine ts)
    (ExprInterval _ interval) -> RExprConst (trimSpacesPrint expr) (ConstantInterval $ refine interval)
    (ExprArr _ es) -> RExprConst (trimSpacesPrint expr) (ConstantArray $ refine <$> es)
    (ExprMap _ ts) -> RExprConst (trimSpacesPrint expr) (ConstantMap $ refine ts)

    -- 3. Json access
    (ExprScalarFunc _ (ScalarFuncFieldToJson _ e1 e2)) -> RExprAccessJson (trimSpacesPrint expr) JOpArrow (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncFieldToText _ e1 e2)) -> RExprAccessJson (trimSpacesPrint expr) JOpLongArrow (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncFieldsToJson _ e1 e2)) -> RExprAccessJson (trimSpacesPrint expr) JOpHashArrow (refine e1) (refine e2)
    (ExprScalarFunc _ (ScalarFuncFieldsToTexts _ e1 e2)) -> RExprAccessJson (trimSpacesPrint expr) JOpHashLongArrow (refine e1) (refine e2)
    -- 4. Scalar functions
    (ExprScalarFunc _ func) -> refine func
    -- 5. Set functions
    (ExprSetFunc _ func) -> refine func
    -- 6. Column access
    (ExprColName _ col) -> refine col
    -- 7. Subquery
    (ExprSubquery _ select) -> RExprSubquery (trimSpacesPrint expr) (refine select)

type instance RefinedType ScalarFunc = RValueExpr
instance Refine ScalarFunc where
  refine func = case func of
    ScalarFuncIfNull   _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpIfNull    (refine e1) (refine e2)
    ScalarFuncNullIf   _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpNullIf    (refine e1) (refine e2)
    ArrayFuncContain   _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpContain   (refine e1) (refine e2)
    ArrayFuncExcept    _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpExcept    (refine e1) (refine e2)
    ArrayFuncIntersect _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpIntersect (refine e1) (refine e2)
    ArrayFuncRemove    _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpRemove    (refine e1) (refine e2)
    ArrayFuncUnion     _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpUnion     (refine e1) (refine e2)
    ArrayFuncJoinWith  _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpArrJoin'  (refine e1) (refine e2)
    ScalarFuncDateStr  _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpDateStr   (refine e1) (refine e2)
    ScalarFuncStrDate  _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpStrDate   (refine e1) (refine e2)
    ScalarFuncSplit    _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpSplit     (refine e1) (refine e2)
    ScalarFuncChunksOf _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpChunksOf  (refine e1) (refine e2)
    ScalarFuncTake     _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpTake      (refine e1) (refine e2)
    ScalarFuncTakeEnd  _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpTakeEnd   (refine e1) (refine e2)
    ScalarFuncDrop     _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpDrop      (refine e1) (refine e2)
    ScalarFuncDropEnd  _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpDropEnd   (refine e1) (refine e2)
    ScalarFuncSin     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpSin     (refine e)
    ScalarFuncSinh    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpSinh    (refine e)
    ScalarFuncAsin    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAsin    (refine e)
    ScalarFuncAsinh   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAsinh   (refine e)
    ScalarFuncCos     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpCos     (refine e)
    ScalarFuncCosh    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpCosh    (refine e)
    ScalarFuncAcos    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAcos    (refine e)
    ScalarFuncAcosh   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAcosh   (refine e)
    ScalarFuncTan     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpTan     (refine e)
    ScalarFuncTanh    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpTanh    (refine e)
    ScalarFuncAtan    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAtan    (refine e)
    ScalarFuncAtanh   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAtanh   (refine e)
    ScalarFuncAbs     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpAbs     (refine e)
    ScalarFuncCeil    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpCeil    (refine e)
    ScalarFuncFloor   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpFloor   (refine e)
    ScalarFuncRound   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpRound   (refine e)
    ScalarFuncSign    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpSign    (refine e)
    ScalarFuncSqrt    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpSqrt    (refine e)
    ScalarFuncLog     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpLog     (refine e)
    ScalarFuncLog2    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpLog2    (refine e)
    ScalarFuncLog10   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpLog10   (refine e)
    ScalarFuncExp     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpExp     (refine e)
    ScalarFuncIsInt   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsInt   (refine e)
    ScalarFuncIsFloat _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsFloat (refine e)
    ScalarFuncIsNum   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsNum   (refine e)
    ScalarFuncIsBool  _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsBool  (refine e)
    ScalarFuncIsStr   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsStr   (refine e)
    ScalarFuncIsMap   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsMap   (refine e)
    ScalarFuncIsArr   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsArr   (refine e)
    ScalarFuncIsDate  _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsDate  (refine e)
    ScalarFuncIsTime  _ e -> RExprUnaryOp (trimSpacesPrint expr) OpIsTime  (refine e)
    ScalarFuncToStr   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpToStr   (refine e)
    ScalarFuncToLower _ e -> RExprUnaryOp (trimSpacesPrint expr) OpToLower (refine e)
    ScalarFuncToUpper _ e -> RExprUnaryOp (trimSpacesPrint expr) OpToUpper (refine e)
    ScalarFuncTrim    _ e -> RExprUnaryOp (trimSpacesPrint expr) OpTrim    (refine e)
    ScalarFuncLTrim   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpLTrim   (refine e)
    ScalarFuncRTrim   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpRTrim   (refine e)
    ScalarFuncRev     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpReverse (refine e)
    ScalarFuncStrlen  _ e -> RExprUnaryOp (trimSpacesPrint expr) OpStrLen  (refine e)
    ArrayFuncDistinct _ e -> RExprUnaryOp (trimSpacesPrint expr) OpDistinct(refine e)
    ArrayFuncLength   _ e -> RExprUnaryOp (trimSpacesPrint expr) OpLength  (refine e)
    ArrayFuncJoin     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpArrJoin (refine e)
    ArrayFuncMax      _ e -> RExprUnaryOp (trimSpacesPrint expr) OpArrMax  (refine e)
    ArrayFuncMin      _ e -> RExprUnaryOp (trimSpacesPrint expr) OpArrMin  (refine e)
    ArrayFuncSort     _ e -> RExprUnaryOp (trimSpacesPrint expr) OpSort    (refine e)

type instance RefinedType SetFunc = RValueExpr
instance Refine SetFunc where
  refine func = case func of
    SetFuncCountAll _ -> RExprAggregate (trimSpacesPrint expr) (Nullary AggCountAll)
    SetFuncCount _ e  -> RExprAggregate (trimSpacesPrint expr) (Unary AggCount $ refine e)
    SetFuncAvg _ e    -> RExprAggregate (trimSpacesPrint expr) (Unary AggAvg $ refine e)
    SetFuncSum _ e    -> RExprAggregate (trimSpacesPrint expr) (Unary AggSum $ refine e)
    SetFuncMax _ e    -> RExprAggregate (trimSpacesPrint expr) (Unary AggMax $ refine e)
    SetFuncMin _ e    -> RExprAggregate (trimSpacesPrint expr) (Unary AggMin $ refine e)
    SetFuncTopK _ e1 e2         -> RExprAggregate (trimSpacesPrint expr) (Binary AggTopK         (refine e1) (refine e2))
    SetFuncTopKDistinct _ e1 e2 -> RExprAggregate (trimSpacesPrint expr) (Binary AggTopKDistinct (refine e1) (refine e2))

type instance RefinedType ColName = RValueExpr
instance Refine ColName where
  refine col = case col of
    ColNameSimple _ (Ident t) -> RExprCol (trimSpacesPrint col) Nothing t
    ColNameRaw _ raw -> RExprCol (trimSpacesPrint col) Nothing (refine raw)
    ColNameStream _ (Ident s) (Ident c) -> RExprCol (trimSpacesPrint col) (Just s) c

--------------------------------------------------------------------------------
---- Sel
type SelectItemAlias = Text
data RSelectItem
  = RSelectItemProject   RValueExpr (Maybe SelectItemAlias)
  | RSelectItemAggregate Aggregate (Maybe SelectItemAlias)
  | RSelectProjectQualifiedAll StreamName
  | RSelectProjectAll
  deriving (Show, Eq)

type instance RefinedType SelectItem = RSelectItem
instance Refine SelectItem where
  refine item = case item of
    SelectItemQualifiedWildcard _ (Ident t) -> RSelectProjectQualifiedAll t
    SelectItemWildcard _ -> RSelectProjectAll
    SelectItemUnnamedExpr _ expr ->
      let rexpr = refine expr
       in case rexpr of
            RExprAggregate _ agg -> RSelectItemAggregate agg Nothing
            _                    -> RSelectItemProject rexpr Nothing
    SelectItemExprWithAlias _ expr (Ident t) ->
      let rexpr = refine expr
       in case rexpr of
            RExprAggregate _ agg -> RSelectItemAggregate agg (Just t)
            _                    -> RSelectItemProject rexpr (Just t)

newtype RSel = RSel [RSelectItem] deriving (Eq, Show)
type instance RefinedType Sel = RSel
instance Refine Sel where
  refine (Sel _ items) = RSel (refine <$> items)

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
             | RHoppingWindow  RInterval RInterval
             | RSlidingWindow  RInterval
             deriving (Eq, Show)
type instance RefinedType Window = RWindow
instance Refine Window where
  refine (TumblingWindow _ interval) = RTumblingWindow (refine interval)
  refine (HoppingWindow  _ len hop ) = RHoppingWindow (refine len) (refine hop)
  refine (SlidingWindow  _ interval) = RSlidingWindow (refine interval)

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

data RSelectView = RSelectView
  { rSelectViewSelect :: SelectViewSelect
  , rSelectViewFrom   :: StreamName
  , rSelectViewWhere  :: RWhere
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
      svFrm :: StreamName
      svFrm = let (RFrom [RTableRefSimple stream Nothing]) = refine frm in stream
      svWhr :: RWhere
      svWhr = refine whr

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
  refine [] = RStreamOptions 1
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
