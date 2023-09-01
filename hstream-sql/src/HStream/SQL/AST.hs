{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

-- This module is only compiled when 'hstream_enable_schema' is disabled.
module HStream.SQL.AST where

#ifndef HStreamEnableSchema
import qualified Data.Aeson               as Aeson
import qualified Data.ByteString          as BS
import           Data.Default
import           Data.Hashable
import qualified Data.HashMap.Strict      as HM
import           Data.Int                 (Int64)
import           Data.Kind                (Type)
import qualified Data.List                as L
import           Data.List.Extra          (anySame)
import           Data.Maybe               (fromJust)
import qualified Data.Scientific          as Scientific
import           Data.Text                (Text)
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8, encodeUtf8)
import qualified Data.Time                as Time
import           Data.Time.Compat         ()
import           Data.Time.Format.ISO8601 (iso8601ParseM, iso8601Show)
import           Data.Typeable            (Typeable)
import qualified Data.Vector              as V
import           Data.Word                (Word32)
import           GHC.Generics
import           GHC.Stack                (HasCallStack)
import           Text.Read                (readMaybe)
import qualified Z.Data.CBytes            as CB
import           Z.Data.CBytes            (CBytes)
import qualified Z.Data.Text              as ZT
import qualified Z.Data.Vector.Base64     as Base64

import           HStream.SQL.Abs
import           HStream.SQL.Exception    (SomeSQLException (..),
                                           throwSQLException)
import           HStream.SQL.Extra
import           HStream.SQL.Rts
import           HStream.Utils            (textToCBytes)
import qualified HStream.Utils.Aeson      as HsAeson


----------------------------- Refinement Main class ----------------------------
type family RefinedType a :: Type

class Refine a where
  refine :: HasCallStack => a -> RefinedType a

--------------------------------------------------------------------------------
-- In new version, defined in HStream.SQL.Binder.Common
class HasName a where
  getName :: a -> String

instance HasName RValueExpr where
  getName expr = case expr of
    RExprCast        name _ _     -> name
    RExprArray       name _       -> name
    RExprAccessArray name _ _     -> name
    RExprCol         name _ _     -> name
    RExprConst       name _       -> name
    RExprAggregate   name _       -> name
    RExprAccessJson  name _ _ _   -> name
    RExprBinOp       name _ _ _   -> name
    RExprUnaryOp     name _ _     -> name
    RExprTerOp       name _ _ _ _ -> name
    -- RExprSubquery    name _     -> name

----------------------------- Refinement details -------------------------------

data RDataType
  = RTypeInteger | RTypeFloat | RTypeBoolean
  | RTypeBytea | RTypeText | RTypeDate | RTypeTime | RTypeTimestamp
  | RTypeInterval | RTypeJsonb
  | RTypeArray RDataType
  deriving (Show, Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance RefinedType DataType = RDataType
instance Refine DataType where
  refine TypeInteger{}   = RTypeInteger
  refine TypeFloat{}     = RTypeFloat
  refine TypeBoolean{}   = RTypeBoolean
  refine TypeByte{}      = RTypeBytea
  refine TypeText{}      = RTypeText
  refine TypeDate{}      = RTypeDate
  refine TypeTime{}      = RTypeTime
  refine TypeTimestamp{} = RTypeTimestamp
  refine TypeInterval{}  = RTypeInterval
  refine TypeJson{}      = RTypeJsonb
  refine (TypeArray _ t) = RTypeArray (refine t)

--------------------------------------------------------------------------------
type ConnectorType = Text

type instance RefinedType PNInteger = Integer
instance Refine PNInteger where
  refine = extractPNInteger

type instance RefinedType PNDouble = Double
instance Refine PNDouble where
  refine = extractPNDouble

type instance RefinedType SingleQuoted = Text
instance Refine SingleQuoted where
  refine = extractSingleQuoted

type instance RefinedType ColumnIdent = Text
instance Refine ColumnIdent where
  refine = extractColumnIdent

type instance RefinedType HIdent = Text
instance Refine HIdent where
  refine = extractHIdent

type RBool = Bool
type instance RefinedType Boolean = RBool
instance Refine Boolean where
  refine (BoolTrue _ ) = True
  refine (BoolFalse _) = False

------- date & time -------
type RTimezone = Time.TimeZone

type RTimestampStr = Time.ZonedTime

type RDate = Time.Day
type instance RefinedType Date = RDate
instance Refine Date where
  refine (DDate _ (SingleQuoted date)) =
    fromJust . iso8601ParseM . tail . init $ Text.unpack date

type RTime = Time.TimeOfDay
type instance RefinedType Time = RTime
instance Refine Time where
  refine (DTime _ (SingleQuoted time)) =
    fromJust . iso8601ParseM . tail . init $ Text.unpack time

type RTimestamp = Time.ZonedTime
type instance RefinedType Timestamp = RTimestamp
instance Refine Timestamp where
  refine (DTimestamp _ (SingleQuoted timestamp)) =
    fromJust . iso8601ParseM . tail . init $ Text.unpack timestamp

type RIntervalUnit = (Integer, Integer) -- from/toMonth, Second
type instance RefinedType IntervalUnit = RIntervalUnit
instance Refine IntervalUnit where
  refine (IntervalSecond _) = (30 * 24 * 60 * 60, 1)
  refine (IntervalMinute _) = (30 * 24 * 60 , 60)
  refine (IntervalHour   _) = (30 * 24, 3600)
  refine (IntervalDay    _) = (30, 3600 * 24)
  refine (IntervalMonth  _) = (1, 0)
  refine (IntervalYear   _) = (12, 0)

fromUnitToDiffTime :: (Integer, Integer) -> Integer -> Time.CalendarDiffTime
fromUnitToDiffTime (m, s) x
  | s == 0    = Time.CalendarDiffTime (x * m) 0
  | otherwise = let (m', rest) = divMod x m in Time.CalendarDiffTime m' (fromIntegral (rest * s))

type RInterval = Time.CalendarDiffTime
type instance RefinedType Interval = RInterval
instance Refine Interval where
  refine (DInterval _ n iUnit) = fromUnitToDiffTime (refine iUnit) n

--------------------------------------------------------------------------------
-- In new version, defined in HStream.SQL.Binder.Basic
data Constant = ConstantNull
              | ConstantInt       Int
              | ConstantFloat     Double
              | ConstantText      Text
              | ConstantBoolean   Bool
              | ConstantDate      RDate
              | ConstantTime      RTime
              | ConstantTimestamp RTimestamp
              | ConstantInterval  RInterval
              | ConstantBytea     CBytes
              | ConstantJsonb     Aeson.Object
              | ConstantArray     [Constant]
              deriving (Show, Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

-- FIXME: `CREATE CONNECTOR` requires plain json value instead of bson value.
constantToPlainJsonValue :: Constant -> Aeson.Value
constantToPlainJsonValue c = case c of
  ConstantNull         -> Aeson.Null
  ConstantInt n        -> Aeson.Number (fromIntegral n)
  ConstantFloat n      -> Aeson.Number (realToFrac n)
  ConstantText t       -> Aeson.String t
  ConstantBoolean b    -> Aeson.Bool b
  ConstantDate d       -> Aeson.String (Text.pack $ iso8601Show d)
  ConstantTime t       -> Aeson.String (Text.pack $ iso8601Show t)
  ConstantTimestamp ts -> Aeson.String (Text.pack $ iso8601Show ts)
  ConstantInterval i   -> Aeson.String (Text.pack $ show i)
  ConstantBytea bs     -> Aeson.String (Text.pack $ CB.unpack bs)
  ConstantJsonb o      -> Aeson.Object o
  ConstantArray arr    -> Aeson.Array (V.fromList (constantToPlainJsonValue <$> arr))

constantToFlowValue :: Constant -> FlowValue
constantToFlowValue constant = case constant of
  ConstantNull         -> FlowNull
  ConstantInt n        -> FlowInt n
  ConstantFloat n      -> FlowFloat n
  ConstantText t       -> FlowText t
  ConstantBoolean b    -> FlowBoolean b
  ConstantDate d       -> FlowDate d
  ConstantTime t       -> FlowTime t
  ConstantTimestamp ts -> FlowTimestamp ts
  ConstantInterval i   -> FlowInterval i
  ConstantBytea bs     -> FlowByte bs
  ConstantJsonb o      -> FlowSubObject (jsonObjectToFlowObject' o)
  ConstantArray arr    -> FlowArray (constantToFlowValue <$> arr)

flowValueToConstant :: FlowValue -> Constant
flowValueToConstant flowValue = case flowValue of
  FlowNull         -> ConstantNull
  FlowInt n        -> ConstantInt n
  FlowFloat n      -> ConstantFloat n
  FlowText t       -> ConstantText t
  FlowBoolean b    -> ConstantBoolean b
  FlowDate d       -> ConstantDate d
  FlowTime t       -> ConstantTime t
  FlowTimestamp ts -> ConstantTimestamp ts
  FlowInterval i   -> ConstantInterval i
  FlowByte bs      -> ConstantBytea bs
  FlowSubObject o  -> ConstantJsonb (flowObjectToJsonObject o)
  FlowArray arr    -> ConstantArray (flowValueToConstant <$> arr)

data BinaryOp = OpAnd | OpOr
              | OpEQ | OpNEQ | OpLT | OpGT | OpLEQ | OpGEQ
              | OpAdd | OpSub | OpMul
              | OpContain | OpExcept  | OpIntersect | OpRemove | OpUnion | OpArrJoin'
              | OpIfNull  | OpNullIf  | OpDateStr   | OpStrDate
              | OpSplit   | OpChunksOf
              | OpTake    | OpTakeEnd | OpDrop      | OpDropEnd
              deriving (Eq, Show, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data UnaryOp  = OpSin      | OpSinh    | OpAsin   | OpAsinh  | OpCos   | OpCosh
              | OpAcos     | OpAcosh   | OpTan    | OpTanh   | OpAtan  | OpAtanh
              | OpAbs      | OpCeil    | OpFloor  | OpRound  | OpSign
              | OpSqrt     | OpLog     | OpLog2   | OpLog10  | OpExp
              | OpIsInt    | OpIsFloat | OpIsBool | OpIsStr
              | OpIsArr    | OpIsDate  | OpIsTime | OpIsNum
              | OpToStr
              | OpToLower  | OpToUpper | OpTrim   | OpLTrim  | OpRTrim
              | OpReverse  | OpStrLen
              | OpDistinct | OpArrJoin | OpLength | OpArrMax | OpArrMin | OpSort
              | OpNot
              deriving (Eq, Show, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data TerOp = OpBetweenAnd | OpNotBetweenAnd | OpBetweenSymAnd | OpNotBetweenSymAnd
  deriving (Show, Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data JsonOp
  = JOpArrow -- json -> text = value
  | JOpLongArrow -- json ->> text = text
  | JOpHashArrow -- json #> array[text/int] = value
  | JOpHashLongArrow -- json #>> array[text/int] = text
  deriving (Eq, Show, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)

data Aggregate expr = Nullary NullaryAggregate
                    | Unary   UnaryAggregate  expr
                    | Binary  BinaryAggregate expr expr
                    deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance (HasName expr) => Show (Aggregate expr) where
  show agg = case agg of
    Nullary nullary     -> show nullary
    Unary unary expr    -> show unary  <> "(" <> getName expr <> ")"
    Binary binary e1 e2 -> show binary <> "(" <> getName e1 <> ", " <> getName e2 <> ")"

data NullaryAggregate = AggCountAll deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show NullaryAggregate where
  show AggCountAll = "COUNT(*)"

data UnaryAggregate   = AggCount
                      | AggAvg
                      | AggSum
                      | AggMax
                      | AggMin
                      deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show UnaryAggregate where
  show agg = case agg of
    AggCount -> "COUNT"
    AggAvg   -> "AVG"
    AggSum   -> "SUM"
    AggMax   -> "MAX"
    AggMin   -> "MIN"

data BinaryAggregate = AggTopK | AggTopKDistinct deriving (Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show BinaryAggregate where
  show agg = case agg of
    AggTopK         -> "TOPK"
    AggTopKDistinct -> "TOPK_DISTINCT"

data RArrayAccessRhs
  = RArrayAccessRhsIndex Int
  | RArrayAccessRhsRange (Maybe Int) (Maybe Int)
  deriving (Eq, Ord, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Show RArrayAccessRhs where
  show (RArrayAccessRhsIndex n) = "[" <> show n <> "]"
  show (RArrayAccessRhsRange l_m r_m) =
    let l = case l_m of
              Nothing -> ""
              Just l  -> show l
        r = case r_m of
              Nothing -> ""
              Just r  -> show r
     in "[" <> l <> ":" <> r <> "]"

type instance RefinedType ArrayAccessRhs = RArrayAccessRhs
instance Refine ArrayAccessRhs where
  refine rhs = case rhs of
    ArrayAccessRhsIndex _ n -> RArrayAccessRhsIndex (fromInteger n)
    ArrayAccessRhsFrom _ n  -> RArrayAccessRhsRange (Just $ fromInteger n) Nothing
    ArrayAccessRhsTo _ n    -> RArrayAccessRhsRange Nothing (Just $ fromInteger n)
    ArrayAccessRhsFromTo _ n1 n2 -> RArrayAccessRhsRange (Just $ fromInteger n1) (Just $ fromInteger n2)

type ExprName = String
type StreamName = Text
type FieldName  = Text
data RValueExpr = RExprCast        ExprName RValueExpr RDataType
                | RExprArray       ExprName [RValueExpr]
                | RExprAccessArray ExprName RValueExpr RArrayAccessRhs
                | RExprCol         ExprName (Maybe StreamName) FieldName
                | RExprConst       ExprName Constant
                | RExprAggregate   ExprName (Aggregate RValueExpr)
                | RExprAccessJson  ExprName JsonOp RValueExpr RValueExpr
                | RExprBinOp       ExprName BinaryOp RValueExpr RValueExpr
                | RExprUnaryOp     ExprName UnaryOp RValueExpr
                | RExprTerOp       ExprName TerOp RValueExpr RValueExpr RValueExpr
                -- | RExprSubquery    ExprName RSelect
                deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
-- FIXME:
instance Ord RValueExpr where
  e1 `compare` e2 = show e1 `compare` show e2

type instance RefinedType ValueExpr = RValueExpr
instance Refine ValueExpr where
  refine expr = case expr of
    -- 1. Operations
    DExprCast _ exprCast -> case exprCast of
      ExprCast1 _ e typ -> RExprCast (trimSpacesPrint expr) (refine e) (refine typ)
      ExprCast2 _ e typ -> RExprCast (trimSpacesPrint expr) (refine e) (refine typ)
    ExprNot _ e         -> RExprUnaryOp (trimSpacesPrint expr) OpNot (refine e)
    ExprAnd _ e1 e2   -> RExprBinOp (trimSpacesPrint expr) OpAnd (refine e1) (refine e2)
    ExprOr  _ e1 e2   -> RExprBinOp (trimSpacesPrint expr) OpOr  (refine e1) (refine e2)
    ExprEQ _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpEQ (refine e1) (refine e2)
    ExprNEQ _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpNEQ (refine e1) (refine e2)
    ExprLT _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpLT (refine e1) (refine e2)
    ExprGT _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpGT (refine e1) (refine e2)
    ExprLEQ _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpLEQ (refine e1) (refine e2)
    ExprGEQ _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpGEQ (refine e1) (refine e2)
    ExprAccessArray _ e rhs -> RExprAccessArray (trimSpacesPrint expr) (refine e) (refine rhs)
    ExprAdd _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpAdd (refine e1) (refine e2)
    ExprSub _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpSub (refine e1) (refine e2)
    ExprMul _ e1 e2 -> RExprBinOp (trimSpacesPrint expr) OpMul (refine e1) (refine e2)
    -- 2. Constants
    ExprNull _                    -> RExprConst (trimSpacesPrint expr) ConstantNull
    ExprInt _ n                   -> RExprConst (trimSpacesPrint expr) (ConstantInt . fromInteger . refine $ n)
    ExprNum _ n                   -> RExprConst (trimSpacesPrint expr) (ConstantFloat $ refine n)
    ExprString _ s@(SingleQuoted _) -> RExprConst (trimSpacesPrint expr) (ConstantText $ refine s)
    ExprBool _ b                  -> RExprConst (trimSpacesPrint expr) (ConstantBoolean $ refine b)
    ExprInterval _ interval       -> RExprConst (trimSpacesPrint expr) (ConstantInterval $ refine interval)
    ExprDate _ date                 -> RExprConst (trimSpacesPrint expr) $ ConstantDate $ refine date
    ExprTime _ time                 -> RExprConst (trimSpacesPrint expr) $ ConstantTime $ refine time
    ExprTimestamp _ timestamp       -> RExprConst (trimSpacesPrint expr) $ ConstantTimestamp $ refine timestamp

    -- 3. Arrays
    ExprArr _ es -> RExprArray (trimSpacesPrint expr) (refine <$> es)

    -- 4. Json access
    -- 5. Scalar functions
    ExprScalarFunc _ func -> refine func
    -- 6. Set functions
    ExprSetFunc _ func -> refine func
    -- 7. Column access
    ExprColName _ col -> refine col
    -- 8. Subquery
    -- (ExprSubquery _ select) -> RExprSubquery (trimSpacesPrint expr) (refine select)

    ExprBetween _ between -> case between of
      BetweenAnd       _ x y z -> h x y z OpBetweenAnd
      NotBetweenAnd    _ x y z -> h x y z OpNotBetweenAnd
      BetweenSymAnd    _ x y z -> h x y z OpBetweenSymAnd
      NotBetweenSymAnd _ x y z -> h x y z OpNotBetweenSymAnd
      where
        h x y z op = RExprTerOp (trimSpacesPrint expr) op
          (refine x) (refine y) (refine z)

type instance RefinedType ScalarFunc = RValueExpr
instance Refine ScalarFunc where
  refine func = case func of
    -- 4. Json access
    (ScalarFuncFieldToJson   _ e1 e2) -> RExprAccessJson (trimSpacesPrint func) JOpArrow         (refine e1) (refine e2)
    (ScalarFuncFieldToText   _ e1 e2) -> RExprAccessJson (trimSpacesPrint func) JOpLongArrow     (refine e1) (refine e2)
    (ScalarFuncFieldsToJson  _ e1 e2) -> RExprAccessJson (trimSpacesPrint func) JOpHashArrow     (refine e1) (refine e2)
    (ScalarFuncFieldsToTexts _ e1 e2) -> RExprAccessJson (trimSpacesPrint func) JOpHashLongArrow (refine e1) (refine e2)
    -- 5. Scalar functions
    ScalarFuncIfNull   _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpIfNull    (refine e1) (refine e2)
    ScalarFuncNullIf   _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpNullIf    (refine e1) (refine e2)
    ArrayFuncContain   _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpContain   (refine e1) (refine e2)
    ArrayFuncExcept    _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpExcept    (refine e1) (refine e2)
    ArrayFuncIntersect _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpIntersect (refine e1) (refine e2)
    ArrayFuncRemove    _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpRemove    (refine e1) (refine e2)
    ArrayFuncUnion     _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpUnion     (refine e1) (refine e2)
    ArrayFuncJoinWith  _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpArrJoin'  (refine e1) (refine e2)
    ScalarFuncDateStr  _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpDateStr   (refine e1) (refine e2)
    ScalarFuncStrDate  _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpStrDate   (refine e1) (refine e2)
    ScalarFuncSplit    _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpSplit     (refine e1) (refine e2)
    ScalarFuncChunksOf _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpChunksOf  (refine e1) (refine e2)
    ScalarFuncTake     _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpTake      (refine e1) (refine e2)
    ScalarFuncTakeEnd  _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpTakeEnd   (refine e1) (refine e2)
    ScalarFuncDrop     _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpDrop      (refine e1) (refine e2)
    ScalarFuncDropEnd  _ e1 e2 -> RExprBinOp (trimSpacesPrint func) OpDropEnd   (refine e1) (refine e2)
    ScalarFuncSin     _ e -> RExprUnaryOp (trimSpacesPrint func) OpSin     (refine e)
    ScalarFuncSinh    _ e -> RExprUnaryOp (trimSpacesPrint func) OpSinh    (refine e)
    ScalarFuncAsin    _ e -> RExprUnaryOp (trimSpacesPrint func) OpAsin    (refine e)
    ScalarFuncAsinh   _ e -> RExprUnaryOp (trimSpacesPrint func) OpAsinh   (refine e)
    ScalarFuncCos     _ e -> RExprUnaryOp (trimSpacesPrint func) OpCos     (refine e)
    ScalarFuncCosh    _ e -> RExprUnaryOp (trimSpacesPrint func) OpCosh    (refine e)
    ScalarFuncAcos    _ e -> RExprUnaryOp (trimSpacesPrint func) OpAcos    (refine e)
    ScalarFuncAcosh   _ e -> RExprUnaryOp (trimSpacesPrint func) OpAcosh   (refine e)
    ScalarFuncTan     _ e -> RExprUnaryOp (trimSpacesPrint func) OpTan     (refine e)
    ScalarFuncTanh    _ e -> RExprUnaryOp (trimSpacesPrint func) OpTanh    (refine e)
    ScalarFuncAtan    _ e -> RExprUnaryOp (trimSpacesPrint func) OpAtan    (refine e)
    ScalarFuncAtanh   _ e -> RExprUnaryOp (trimSpacesPrint func) OpAtanh   (refine e)
    ScalarFuncAbs     _ e -> RExprUnaryOp (trimSpacesPrint func) OpAbs     (refine e)
    ScalarFuncCeil    _ e -> RExprUnaryOp (trimSpacesPrint func) OpCeil    (refine e)
    ScalarFuncFloor   _ e -> RExprUnaryOp (trimSpacesPrint func) OpFloor   (refine e)
    ScalarFuncRound   _ e -> RExprUnaryOp (trimSpacesPrint func) OpRound   (refine e)
    ScalarFuncSign    _ e -> RExprUnaryOp (trimSpacesPrint func) OpSign    (refine e)
    ScalarFuncSqrt    _ e -> RExprUnaryOp (trimSpacesPrint func) OpSqrt    (refine e)
    ScalarFuncLog     _ e -> RExprUnaryOp (trimSpacesPrint func) OpLog     (refine e)
    ScalarFuncLog2    _ e -> RExprUnaryOp (trimSpacesPrint func) OpLog2    (refine e)
    ScalarFuncLog10   _ e -> RExprUnaryOp (trimSpacesPrint func) OpLog10   (refine e)
    ScalarFuncExp     _ e -> RExprUnaryOp (trimSpacesPrint func) OpExp     (refine e)
    ScalarFuncIsInt   _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsInt   (refine e)
    ScalarFuncIsFloat _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsFloat (refine e)
    ScalarFuncIsBool  _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsBool  (refine e)
    ScalarFuncIsStr   _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsStr   (refine e)
    ScalarFuncIsArr   _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsArr   (refine e)
    ScalarFuncIsDate  _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsDate  (refine e)
    ScalarFuncIsTime  _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsTime  (refine e)
    ScalarFuncIsNum   _ e -> RExprUnaryOp (trimSpacesPrint func) OpIsNum   (refine e)
    ScalarFuncToStr   _ e -> RExprUnaryOp (trimSpacesPrint func) OpToStr   (refine e)
    ScalarFuncToLower _ e -> RExprUnaryOp (trimSpacesPrint func) OpToLower (refine e)
    ScalarFuncToUpper _ e -> RExprUnaryOp (trimSpacesPrint func) OpToUpper (refine e)
    ScalarFuncTrim    _ e -> RExprUnaryOp (trimSpacesPrint func) OpTrim    (refine e)
    ScalarFuncLTrim   _ e -> RExprUnaryOp (trimSpacesPrint func) OpLTrim   (refine e)
    ScalarFuncRTrim   _ e -> RExprUnaryOp (trimSpacesPrint func) OpRTrim   (refine e)
    ScalarFuncRev     _ e -> RExprUnaryOp (trimSpacesPrint func) OpReverse (refine e)
    ScalarFuncStrlen  _ e -> RExprUnaryOp (trimSpacesPrint func) OpStrLen  (refine e)
    ArrayFuncDistinct _ e -> RExprUnaryOp (trimSpacesPrint func) OpDistinct(refine e)
    ArrayFuncLength   _ e -> RExprUnaryOp (trimSpacesPrint func) OpLength  (refine e)
    ArrayFuncJoin     _ e -> RExprUnaryOp (trimSpacesPrint func) OpArrJoin (refine e)
    ArrayFuncMax      _ e -> RExprUnaryOp (trimSpacesPrint func) OpArrMax  (refine e)
    ArrayFuncMin      _ e -> RExprUnaryOp (trimSpacesPrint func) OpArrMin  (refine e)
    ArrayFuncSort     _ e -> RExprUnaryOp (trimSpacesPrint func) OpSort    (refine e)


type instance RefinedType SetFunc = RValueExpr
instance Refine SetFunc where
  refine func = case func of
    SetFuncCountAll _ -> RExprAggregate (trimSpacesPrint func) (Nullary AggCountAll)
    SetFuncCount _ e  -> RExprAggregate (trimSpacesPrint func) (Unary AggCount $ refine e)
    SetFuncAvg _ e    -> RExprAggregate (trimSpacesPrint func) (Unary AggAvg $ refine e)
    SetFuncSum _ e    -> RExprAggregate (trimSpacesPrint func) (Unary AggSum $ refine e)
    SetFuncMax _ e    -> RExprAggregate (trimSpacesPrint func) (Unary AggMax $ refine e)
    SetFuncMin _ e    -> RExprAggregate (trimSpacesPrint func) (Unary AggMin $ refine e)
    SetFuncTopK _ e1 e2         -> RExprAggregate (trimSpacesPrint func) (Binary AggTopK         (refine e1) (refine e2))
    SetFuncTopKDistinct _ e1 e2 -> RExprAggregate (trimSpacesPrint func) (Binary AggTopKDistinct (refine e1) (refine e2))

type instance RefinedType ColName = RValueExpr
instance Refine ColName where
  refine col = case col of
    ColNameSimple _ colIdent ->
      RExprCol (trimSpacesPrint col) Nothing (refine colIdent)
    ColNameStream _ hIdent colIdent ->
      RExprCol (trimSpacesPrint col) (Just $ refine hIdent) (refine colIdent)

--------------------------------------------------------------------------------
---- Sel
type SelectItemAlias = Text
data RSelectItem
  = RSelectItemProject RValueExpr (Maybe SelectItemAlias)
  | RSelectProjectQualifiedAll StreamName
  | RSelectProjectAll
  deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance RefinedType SelectItem = RSelectItem
instance Refine SelectItem where
  refine item = case item of
    SelectItemQualifiedWildcard _ hIdent -> RSelectProjectQualifiedAll (refine hIdent)
    SelectItemWildcard _ -> RSelectProjectAll
    SelectItemUnnamedExpr _ expr -> RSelectItemProject (refine expr) Nothing
    SelectItemExprWithAlias _ expr colIdent ->
      RSelectItemProject (refine expr) (Just $ refine colIdent)

newtype RSel = RSel [RSelectItem] deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType Sel = RSel
instance Refine Sel where
  refine (DSel _ items) = RSel (refine <$> items)

---- Frm
data WindowType
  = Tumbling RInterval
  | Hopping  RInterval RInterval
#ifdef HStreamUseV2Engine
  | Sliding RInterval
#else
  | Session  RInterval
#endif
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

#ifdef HStreamUseV2Engine
data RTableRef = RTableRefSimple StreamName (Maybe StreamName)
               | RTableRefSubquery RSelect  (Maybe StreamName)
               | RTableRefCrossJoin RTableRef RTableRef (Maybe StreamName)
               | RTableRefNaturalJoin RTableRef RJoinType RTableRef (Maybe StreamName)
               | RTableRefJoinOn RTableRef RJoinType RTableRef RValueExpr (Maybe StreamName)
               | RTableRefJoinUsing RTableRef RJoinType RTableRef [Text] (Maybe StreamName)
               | RTableRefWindowed RTableRef WindowType (Maybe StreamName)
#else
data RTableRef = RTableRefSimple StreamName (Maybe StreamName)
              --  | RTableRefSubquery RSelect  (Maybe StreamName)
               | RTableRefWindowed StreamName WindowType
               | RTableRefCrossJoin RTableRef RTableRef RInterval
               | RTableRefNaturalJoin RTableRef RJoinType RTableRef RInterval
               | RTableRefJoinOn RTableRef RJoinType RTableRef RValueExpr RInterval
               | RTableRefJoinUsing RTableRef RJoinType RTableRef [Text] RInterval
#endif
               deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
#ifdef HStreamUseV2Engine
setRTableRefAlias :: RTableRef -> StreamName -> RTableRef
setRTableRefAlias ref alias = case ref of
  RTableRefSimple s _ -> RTableRefSimple s (Just alias)
  RTableRefSubquery sel _ -> RTableRefSubquery sel (Just alias)
  RTableRefCrossJoin r1 r2 _ -> RTableRefCrossJoin r1 r2 (Just alias)
  RTableRefNaturalJoin r1 typ r2 _ -> RTableRefNaturalJoin r1 typ r2 (Just alias)
  RTableRefJoinOn r1 typ r2 e _ -> RTableRefJoinOn r1 typ r2 e (Just alias)
  RTableRefJoinUsing r1 typ r2 cols _ -> RTableRefJoinUsing r1 typ r2 cols (Just alias)
  RTableRefWindowed r win _ -> RTableRefWindowed r win (Just alias)
-- #else
--   RTableRefCrossJoin r1 r2 t _ -> RTableRefCrossJoin r1 r2 t (Just alias)
--   RTableRefNaturalJoin r1 typ r2 t _ -> RTableRefNaturalJoin r1 typ r2 t (Just alias)
--   RTableRefJoinOn r1 typ r2 e t _ -> RTableRefJoinOn r1 typ r2 e t (Just alias)
--   RTableRefJoinUsing r1 typ r2 cols t _ -> RTableRefJoinUsing r1 typ r2 cols t (Just alias)
--   RTableRefWindowed r win _ -> RTableRefWindowed r win (Just alias)
#endif

data RJoinType = InnerJoin | LeftJoin | RightJoin | FullJoin
               deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType JoinTypeWithCond = RJoinType
instance Refine JoinTypeWithCond where
  refine joinType = case joinType of
    JoinInner1{} -> InnerJoin
    JoinInner2{} -> InnerJoin
    JoinLeft1{}  -> LeftJoin
    JoinLeft2{}  -> LeftJoin
    JoinRight1{} -> RightJoin
    JoinRight2{} -> RightJoin
    JoinFull1{}  -> FullJoin
    JoinFull2{}  -> FullJoin

type instance RefinedType TableRef = RTableRef
instance Refine TableRef where
#ifdef HStreamUseV2Engine
  refine (TableRefIdent _ hIdent) = RTableRefSimple (refine hIdent) Nothing
  refine (TableRefSubquery _ select) = RTableRefSubquery (refine select) Nothing
  refine (TableRefAs _ ref alias) =
    let rRef = refine ref
     in setRTableRefAlias rRef (refine alias)
  refine (TableRefCrossJoin _ r1 _ r2) = RTableRefCrossJoin (refine r1) (refine r2) Nothing
  refine (TableRefNaturalJoin _ r1 typ r2) = RTableRefNaturalJoin (refine r1) (refine typ) (refine r2) Nothing
  refine (TableRefJoinOn _ r1 typ r2 e) = RTableRefJoinOn (refine r1) (refine typ) (refine r2) (refine e) Nothing
  refine (TableRefJoinUsing _ r1 typ r2 cols) = RTableRefJoinUsing (refine r1) (refine typ) (refine r2) (extractStreamNameFromColName <$> cols) Nothing
    where extractStreamNameFromColName col = case col of
            ColNameSimple _ colIdent -> refine colIdent
            ColNameStream pos _ _    -> throwImpossible
  refine (TableRefTumbling _ ref interval) = RTableRefWindowed (refine ref) (Tumbling (refine interval)) Nothing
  refine (TableRefHopping _ ref len hop) = RTableRefWindowed (refine ref) (Hopping (refine len) (refine hop)) Nothing
  refine (TableRefSliding _ ref interval) = RTableRefWindowed (refine ref) (Sliding (refine interval)) Nothing
#else
  refine (TableRefIdent _ hIdent) = RTableRefSimple (refine hIdent) Nothing
  refine (TableRefSubquery pos _) = throwSQLException RefineException pos "Subquery is not supported yet"
  refine (TableRefAs pos r alias) = case r of
    TableRefIdent _ hIdent -> RTableRefSimple (refine hIdent) (Just $ refine alias)
    _ -> throwSQLException RefineException pos "Aliases is only supported for simple table reference yet"
  refine (TableRefCrossJoin _ r1 _ r2 interval) = RTableRefCrossJoin (refine r1) (refine r2) (refine interval)
  refine (TableRefNaturalJoin _ r1 typ r2 interval) = RTableRefNaturalJoin (refine r1) (refine typ) (refine r2) (refine interval)
  refine (TableRefJoinOn _ r1 typ r2 e interval) = RTableRefJoinOn (refine r1) (refine typ) (refine r2) (refine e) (refine interval)
  refine (TableRefJoinUsing _ r1 typ r2 cols interval) = RTableRefJoinUsing (refine r1) (refine typ) (refine r2) (refine <$> cols) (refine interval)
  refine (TableRefTumbling pos ref interval) = case ref of
    TableRefIdent _ hIdent -> RTableRefWindowed (refine hIdent) (Tumbling (refine interval))
    _ -> throwSQLException RefineException pos "Tumbling is only supported for simple table reference yet"
  refine (TableRefHopping pos ref len hop)   = case ref of
    TableRefIdent _ hIdent -> RTableRefWindowed (refine hIdent) (Hopping (refine len) (refine hop))
    _ -> throwSQLException RefineException pos "Hopping is only supported for simple table reference yet"
  refine (TableRefSession pos ref interval)  = case ref of
    TableRefIdent _ hIdent -> RTableRefWindowed (refine hIdent) (Session (refine interval))
    _ -> throwSQLException RefineException pos "Session is only supported for simple table reference yet"
#endif

#ifdef HStreamUseV2Engine
newtype RFrom = RFrom [RTableRef] deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType From = RFrom
instance Refine From where
  refine (DFrom _ refs) = RFrom (refine <$> refs)
#else
newtype RFrom = RFrom RTableRef deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType From = RFrom
instance Refine From where
  refine (DFrom _ ref) = RFrom (refine ref)
#endif

---- Whr
data RWhere = RWhereEmpty
            | RWhere RValueExpr
            deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType Where = RWhere
instance Refine Where where
  refine (DWhereEmpty _) = RWhereEmpty
  refine (DWhere _ expr) = RWhere (refine expr)

---- Grp
#ifdef HStreamUseV2Engine
data RGroupBy = RGroupByEmpty
              | RGroupBy [(Maybe StreamName, FieldName)]
              deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType GroupBy = RGroupBy
instance Refine GroupBy where
  refine (DGroupByEmpty _) = RGroupByEmpty
  refine (DGroupBy _ cols) = RGroupBy $
    L.map (\col -> let (RExprCol _ m_stream field) = refine col
                    in (m_stream, field)) cols
#else

data RGroupBy = RGroupByEmpty
              | RGroupBy [(Maybe StreamName, FieldName)] (Maybe WindowType)
              deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType GroupBy = RGroupBy
instance Refine GroupBy where
  refine (DGroupByEmpty _) = RGroupByEmpty
  refine (DGroupBy pos cols) = RGroupBy
    (L.map (\col -> case refine col of
      RExprCol _ m_stream field -> (m_stream, field)
      _                         -> throwSQLException RefineException pos "Group By Columns should be RExprCol"
      ) cols
    ) Nothing
  -- refine (DGroupByWin pos cols win) =
  --   let (RGroupBy tups Nothing) = refine (DGroupBy pos cols)
  --    in RGroupBy tups (Just $ refine win)
#endif

---- Hav
data RHaving = RHavingEmpty
             | RHaving RValueExpr
             deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType Having = RHaving
instance Refine Having where
  refine (DHavingEmpty _) = RHavingEmpty
  refine (DHaving _ expr) = RHaving (refine expr)

---- SELECT

data RSelect = RSelect RSel RFrom RWhere RGroupBy RHaving
             deriving (Show, Eq, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType Select = RSelect
#ifdef HStreamUseV2Engine
instance Refine Select where
  refine (DSelect _ sel frm whr grp hav) =
    RSelect (refine sel) (refine frm) (refine whr) (refine grp) (refine hav)
#else
instance Refine Select where
  refine (DSelect _ sel frm whr grp hav) =
    case refine frm of
      RFrom (RTableRefWindowed r win)->
        let newFrm = RFrom (RTableRefSimple r Nothing) in
        let newGrp = case refine grp of RGroupBy x _ -> RGroupBy x (Just win); x -> x in
        RSelect (refine sel) newFrm (refine whr) newGrp (refine hav)
      rfrm -> RSelect (refine sel) rfrm (refine whr) (refine grp) (refine hav)
#endif

---- EXPLAIN
type RExplain = RSelect
type instance RefinedType Explain = RExplain
instance Refine Explain where
  refine (ExplainSelect _ select)                    = refine select
  refine (ExplainCreate _ (CreateAs _ _ select))     = refine select
  refine (ExplainCreate _ (CreateAsOp _ _ select _)) = refine select
  refine (ExplainCreate _ (CreateView _ _ select))   = refine select
  refine (ExplainCreate _ _)                         = throwImpossible

---- CREATE
data RStreamOptions = RStreamOptions
  { rRepFactor       :: Int
  , rBacklogDuration :: Word32
  } deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance Default RStreamOptions where
  def = RStreamOptions
      { rRepFactor       = 1
      , rBacklogDuration = 7 * 24 * 3600
      }

newtype RConnectorOptions = RConnectorOptions (HM.HashMap Text Aeson.Value)
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

data RCreate = RCreate   Text RStreamOptions
             | RCreateAs Text RSelect RStreamOptions
             -- RCreateConnector <SOURCE|SINK> <Name> <Target> <EXISTS> <OPTIONS>
             | RCreateConnector Text Text Text Bool RConnectorOptions
             | RCreateView Text RSelect
             deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance RefinedType [StreamOption] = RStreamOptions
instance Refine [StreamOption] where
  refine options =
    let factor_m = L.find (\x -> case x of
                                   OptionRepFactor{} -> True
                                   _                 -> False
                          ) options
        duration_m = L.find (\x -> case x of
                                   OptionDuration{} -> True
                                   _                -> False
                            ) options
        factor = maybe (rRepFactor def)
                       (\case
                        OptionRepFactor _ n' -> fromInteger $ extractPNInteger n'
                        _ -> throwSQLException RefineException Nothing "Invalid StreamOption OptionRepFactor"
                        )
                       factor_m
        duration = maybe (rBacklogDuration def)
                         (\case
                          OptionDuration _ interval ->
                            fromIntegral (calendarDiffTimeToMs (refine (interval :: Interval))) `div` 1000
                          _ -> throwSQLException RefineException Nothing "Invalid StreamOption OptionDuration"
                         )
                         duration_m
     in RStreamOptions { rRepFactor       = factor
                       , rBacklogDuration = duration
                       }

type instance RefinedType [ConnectorOption] = RConnectorOptions
instance Refine [ConnectorOption] where
  refine ps = RConnectorOptions $ foldr (insert . toPair) HM.empty ps
    where insert (k, v) = HM.insert k v
          toPair :: ConnectorOption -> (Text, Aeson.Value)
          toPair (ConnectorProperty _ key expr) = (extractHIdent key, toValue (refine expr))
          toValue (RExprConst _ c) = constantToPlainJsonValue c
          toValue _                = throwSQLException RefineException Nothing "Connector Property should be const literal expression"

type instance RefinedType Create = RCreate
instance Refine Create where
  refine (DCreate  _ hIdent) = RCreate (refine hIdent) $ refine ([] :: [StreamOption])
  refine (CreateOp _ hIdent options)  = RCreate (refine hIdent) (refine options)
  refine (CreateAs   _ hIdent select) = RCreateAs (refine hIdent) (refine select) (refine ([] :: [StreamOption]))
  refine (CreateAsOp _ hIdent select options) = RCreateAs (refine hIdent) (refine select) (refine options)
  refine (CreateSourceConnector _ s t options) = RCreateConnector "SOURCE" (refine s) (refine t) False (refine options)
  refine (CreateSourceConnectorIf _ s t options) = RCreateConnector "SOURCE" (refine s) (refine t) True (refine options)
  refine (CreateSinkConnector _ s t options) = RCreateConnector "SINK" (refine s) (refine t) False (refine options)
  refine (CreateSinkConnectorIf _ s t options) = RCreateConnector "SINK" (refine s) (refine t) True (refine options)
  refine (CreateView _ s select) = RCreateView (refine s) (refine select)
  refine (DCreateWithSchema _ _ _) = throwSQLException RefineException Nothing "CreateWithSchema is not supported without `hstream_enable_schema` flag."

data RInsertRawOrJsonPayloadType = RInsertRawOrJsonPayloadTypeRaw | RInsertRawOrJsonPayloadTypeJson
  deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

---- INSERT
data RInsert = RInsert          Text [(FieldName, Constant)]
             | RInsertRawOrJson Text BS.ByteString RInsertRawOrJsonPayloadType
             | RInsertSel       Text RSelect
             deriving (Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
type instance RefinedType Insert = RInsert
instance Refine Insert where
  refine (DInsert _ s fields exprs) = RInsert (refine s) $
    zip ((\colIdent -> refine colIdent) <$> fields) (refineConst <$> exprs)
    where
      refineConst expr = case refine expr of
        RExprConst _ constant -> constant
        _ -> error "INTERNAL ERROR: constant expr in RInsert is ensured by validate"
  refine (InsertRawOrJson _ streamName valExprCast) =
    let (val, typ, _) = unifyValueExprCast valExprCast
        errMsg        = "INTERNAL ERROR: Insert RawRecord or HRecord syntax only supports string literals to be casted to `BYTEA` or `JSONB`, which is ensured by validate"
        rVal          = case val of
                          ExprString _ singleQuoted@(SingleQuoted _) -> refine singleQuoted
                          _ -> error errMsg
        rTyp          = case typ of
                          TypeByte _ -> RInsertRawOrJsonPayloadTypeRaw
                          TypeJson _ -> RInsertRawOrJsonPayloadTypeJson
                          _          -> error errMsg
    in RInsertRawOrJson (extractHIdent streamName) (encodeUtf8 rVal) rTyp

  refine (DInsertSel _ insSel) = case insSel of
    InsSel _ streamName sel -> h streamName sel
    where h streamName sel = RInsertSel (extractHIdent streamName) (refine sel)

---- SHOW
data RShow
  = RShow RShowOption
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Refine ShowQ where
  refine (DShow _ showOp) = RShow (refine showOp)
type instance RefinedType ShowQ = RShow

data RShowOption
  = RShowStreams
  | RShowQueries
  | RShowConnectors
  | RShowViews
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
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
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Refine Drop where
  refine (DDrop  _ dropOp x) = RDrop   (refine dropOp) (refine x)
  refine (DropIf _ dropOp x) = RDropIf (refine dropOp) (refine x)
type instance RefinedType Drop = RDrop

data RDropOption
  = RDropConnector
  | RDropStream
  | RDropView
  | RDropQuery
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

instance Refine DropOption where
  refine (DropConnector _) = RDropConnector
  refine (DropStream _)    = RDropStream
  refine (DropView   _)    = RDropView
  refine (DropQuery   _)   = RDropQuery
type instance RefinedType DropOption = RDropOption

---- Terminate
data RTerminate
  = RTerminateQuery Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)
instance Refine Terminate where
  refine (TerminateQuery _ x) = RTerminateQuery (refine x)
type instance RefinedType Terminate = RTerminate

---- Pause
data RPause
  = RPauseConnector Text
  | RPauseQuery Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance RefinedType Pause = RPause

instance Refine Pause where
  refine (PauseConnector _ name) = RPauseConnector (refine name)
  refine (PauseQuery _ name)     = RPauseQuery (refine name)

---- Resume
data RResume
  = RResumeConnector Text
  | RResumeQuery Text
  deriving (Eq, Show, Generic, Aeson.ToJSON, Aeson.FromJSON)

type instance RefinedType Resume = RResume

instance Refine Resume where
  refine (ResumeConnector _ name) = RResumeConnector (refine name)
  refine (ResumeQuery _ name)     = RResumeQuery (refine name)

---- SQL
data RSQL = RQSelect      RSelect
          | RQPushSelect  RSelect
          | RQCreate      RCreate
          | RQInsert      RInsert
          | RQShow        RShow
          | RQDrop        RDrop
          | RQTerminate   RTerminate
          | RQExplain     RExplain
          | RQPause       RPause
          | RQResume      RResume
          deriving (Show, Generic, Aeson.FromJSON, Aeson.ToJSON)
type instance RefinedType SQL = RSQL
instance Refine SQL where
  refine (QSelect     _ select)  =  RQSelect      (refine   select)
  refine (QPushSelect _ select)  =  RQPushSelect  (refine   select)
  refine (QCreate     _ create)  =  RQCreate      (refine   create)
  refine (QInsert     _ insert)  =  RQInsert      (refine   insert)
  refine (QShow       _ show_)   =  RQShow        (refine    show_)
  refine (QDrop       _ drop_)   =  RQDrop        (refine    drop_)
  refine (QTerminate  _ term)    =  RQTerminate   (refine     term)
  refine (QExplain    _ explain) =  RQExplain     (refine  explain)
  refine (QPause      _ pause)   =  RQPause       (refine  pause)
  refine (QResume     _ resume)  =  RQResume      (refine  resume)

--------------------------------------------------------------------------------
-- In new version, defined in HStream.SQL.Binder.Common
throwImpossible :: a
throwImpossible = throwSQLException RefineException Nothing "Impossible happened"

#endif
