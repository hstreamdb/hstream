module HStream.SQL.Binder.Operator where

import qualified Data.List                 as L
import qualified Data.Set                  as Set

import           HStream.SQL.Binder.Basic
import           HStream.SQL.Binder.Common

----------------------------------------
--           common utils
----------------------------------------
-- | All bound data types except 'BTypeArray's whose depth is greater than 5.
allBoundDataTypes :: Set.Set BoundDataType
allBoundDataTypes = go basic 5
  where basic = Set.fromList [ BTypeInteger, BTypeFloat, BTypeText, BTypeBoolean, BTypeJsonb, BTypeBytea, BTypeTimestamp, BTypeDate, BTypeTime, BTypeInterval ]
        go acc n = if n <= 0 then acc else
                     Set.map BTypeArray acc `Set.union` basic

----------------------------------------
--   nullary operators a.k.a values
----------------------------------------
class NullaryOperator a where
  nResType :: a -> BoundDataType
  {-# MINIMAL nResType #-}
instance NullaryOperator Constant where
  nResType v = case v of
    ConstantNull           -> BTypeInteger -- FIXME: type for null?
    ConstantInt _          -> BTypeInteger
    ConstantFloat _        -> BTypeFloat
    ConstantText _         -> BTypeText
    ConstantBoolean _      -> BTypeBoolean
    ConstantDate _         -> BTypeDate
    ConstantTime _         -> BTypeTime
    ConstantTimestamp _    -> BTypeTimestamp
    ConstantInterval _     -> BTypeInterval
    ConstantBytea _        -> BTypeBytea
    ConstantJsonb _        -> BTypeJsonb
    ConstantArray vs
      | L.null vs -> BTypeArray BTypeInteger -- FIXME: type for empty array?
      | otherwise -> BTypeArray (nResType (L.head vs))

----------------------------------------
--           unary operators
----------------------------------------
class UnaryOperator a where
  uOpType  :: a -> Set.Set BoundDataType
  uResType :: a -> BoundDataType -> BoundDataType
  {-# MINIMAL uOpType, uResType #-}

instance UnaryOperator UnaryOp where
  uOpType op = case op of
    OpSin      -> Set.fromList [BTypeInteger, BTypeFloat]
    OpSinh     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAsin     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAsinh    -> Set.fromList [BTypeInteger, BTypeFloat]
    OpCos      -> Set.fromList [BTypeInteger, BTypeFloat]
    OpCosh     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAcos     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAcosh    -> Set.fromList [BTypeInteger, BTypeFloat]
    OpTan      -> Set.fromList [BTypeInteger, BTypeFloat]
    OpTanh     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAtan     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAtanh    -> Set.fromList [BTypeInteger, BTypeFloat]
    OpAbs      -> Set.fromList [BTypeInteger, BTypeFloat]
    OpCeil     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpFloor    -> Set.fromList [BTypeInteger, BTypeFloat]
    OpRound    -> Set.fromList [BTypeInteger, BTypeFloat]
    OpSign     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpSqrt     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpLog      -> Set.fromList [BTypeInteger, BTypeFloat]
    OpLog2     -> Set.fromList [BTypeInteger, BTypeFloat]
    OpLog10    -> Set.fromList [BTypeInteger, BTypeFloat]
    OpExp      -> Set.fromList [BTypeInteger, BTypeFloat]
    OpIsInt    -> allBoundDataTypes
    OpIsFloat  -> allBoundDataTypes
    OpIsBool   -> allBoundDataTypes
    OpIsStr    -> allBoundDataTypes
    OpIsArr    -> allBoundDataTypes
    OpIsDate   -> allBoundDataTypes
    OpIsTime   -> allBoundDataTypes
    OpIsNum    -> allBoundDataTypes
    OpToStr    -> allBoundDataTypes
    OpToLower  -> Set.singleton BTypeText
    OpToUpper  -> Set.singleton BTypeText
    OpTrim     -> Set.singleton BTypeText
    OpLTrim    -> Set.singleton BTypeText
    OpRTrim    -> Set.singleton BTypeText
    OpReverse  -> Set.singleton BTypeText
    OpStrLen   -> Set.singleton BTypeText
    OpDistinct -> Set.map BTypeArray allBoundDataTypes
    OpArrJoin  -> Set.map BTypeArray allBoundDataTypes
    OpLength   -> Set.map BTypeArray allBoundDataTypes
    OpArrMax   -> Set.map BTypeArray allBoundDataTypes
    OpArrMin   -> Set.map BTypeArray allBoundDataTypes
    OpSort     -> Set.map BTypeArray allBoundDataTypes
    OpNot      -> Set.singleton BTypeBoolean
  uResType op t = case op of
    OpSin      -> BTypeFloat
    OpSinh     -> BTypeFloat
    OpAsin     -> BTypeFloat
    OpAsinh    -> BTypeFloat
    OpCos      -> BTypeFloat
    OpCosh     -> BTypeFloat
    OpAcos     -> BTypeFloat
    OpAcosh    -> BTypeFloat
    OpTan      -> BTypeFloat
    OpTanh     -> BTypeFloat
    OpAtan     -> BTypeFloat
    OpAtanh    -> BTypeFloat
    OpAbs      -> t
    OpCeil     -> BTypeInteger
    OpFloor    -> BTypeInteger
    OpRound    -> BTypeInteger
    OpSign     -> BTypeInteger
    OpSqrt     -> BTypeFloat
    OpLog      -> BTypeFloat
    OpLog2     -> BTypeFloat
    OpLog10    -> BTypeFloat
    OpExp      -> BTypeFloat
    OpIsInt    -> BTypeBoolean
    OpIsFloat  -> BTypeBoolean
    OpIsBool   -> BTypeBoolean
    OpIsStr    -> BTypeBoolean
    OpIsArr    -> BTypeBoolean
    OpIsDate   -> BTypeBoolean
    OpIsTime   -> BTypeBoolean
    OpIsNum    -> BTypeBoolean
    OpToStr    -> BTypeText
    OpToLower  -> BTypeText
    OpToUpper  -> BTypeText
    OpTrim     -> BTypeText
    OpLTrim    -> BTypeText
    OpRTrim    -> BTypeText
    OpReverse  -> BTypeText
    OpStrLen   -> BTypeInteger
    OpDistinct -> t
    OpArrJoin  -> BTypeText
    OpLength   -> BTypeInteger
    OpArrMax   -> let BTypeArray t' = t in t'
    OpArrMin   -> let BTypeArray t' = t in t'
    OpSort     -> t
    OpNot      -> BTypeBoolean

----------------------------------------
--           binary operators
----------------------------------------
class BinaryOperator a where
  bOp1Type :: a -> Set.Set BoundDataType
  bOp2Type :: a -> Set.Set BoundDataType
  bResType :: a -> BoundDataType -> BoundDataType -> BoundDataType
  {-# MINIMAL bOp1Type, bOp2Type, bResType #-}

instance BinaryOperator JsonOp where
  bOp1Type _ = Set.singleton BTypeJsonb
  bOp2Type op = case op of
    JOpArrow         -> Set.singleton BTypeText
    JOpLongArrow     -> Set.singleton BTypeText
    JOpHashArrow     -> Set.fromList [ BTypeArray BTypeText
                                     , BTypeArray BTypeInteger]
    JOpHashLongArrow -> Set.fromList [ BTypeArray BTypeText
                                     , BTypeArray BTypeInteger]
  bResType op _ _ = case op of
    JOpArrow         -> BTypeInteger -- FIXME: can not determine at compile time!
    JOpLongArrow     -> BTypeText
    JOpHashArrow     -> BTypeInteger -- FIXME: can not determine at compile time!
    JOpHashLongArrow -> BTypeText

instance BinaryOperator BinaryOp where
  bOp1Type op = case op of
    OpAnd       -> Set.singleton BTypeBoolean
    OpOr        -> Set.singleton BTypeBoolean
    OpEQ        -> allBoundDataTypes
    OpNEQ       -> allBoundDataTypes
    OpLT        -> allBoundDataTypes
    OpGT        -> allBoundDataTypes
    OpLEQ       -> allBoundDataTypes
    OpGEQ       -> allBoundDataTypes
    OpAdd       -> Set.fromList [BTypeInteger, BTypeFloat]
    OpSub       -> Set.fromList [BTypeInteger, BTypeFloat]
    OpMul       -> Set.fromList [BTypeInteger, BTypeFloat]
    OpContain   -> Set.map BTypeArray allBoundDataTypes
    OpExcept    -> Set.map BTypeArray allBoundDataTypes
    OpIntersect -> Set.map BTypeArray allBoundDataTypes
    OpRemove    -> Set.map BTypeArray allBoundDataTypes
    OpUnion     -> Set.map BTypeArray allBoundDataTypes
    OpArrJoin'  -> Set.map BTypeArray allBoundDataTypes
    OpIfNull    -> allBoundDataTypes
    OpNullIf    -> allBoundDataTypes
    OpDateStr   -> Set.singleton BTypeInteger
    OpStrDate   -> Set.singleton BTypeText
    OpSplit     -> Set.singleton BTypeText
    OpChunksOf  -> Set.singleton BTypeInteger
    OpTake      -> Set.singleton BTypeInteger
    OpTakeEnd   -> Set.singleton BTypeInteger
    OpDrop      -> Set.singleton BTypeInteger
    OpDropEnd   -> Set.singleton BTypeInteger
  bOp2Type op = case op of
    OpAnd       -> Set.singleton BTypeBoolean
    OpOr        -> Set.singleton BTypeBoolean
    OpEQ        -> allBoundDataTypes
    OpNEQ       -> allBoundDataTypes
    OpLT        -> allBoundDataTypes
    OpGT        -> allBoundDataTypes
    OpLEQ       -> allBoundDataTypes
    OpGEQ       -> allBoundDataTypes
    OpAdd       -> Set.fromList [BTypeInteger, BTypeFloat]
    OpSub       -> Set.fromList [BTypeInteger, BTypeFloat]
    OpMul       -> Set.fromList [BTypeInteger, BTypeFloat]
    OpContain   -> allBoundDataTypes
    OpExcept    -> Set.map BTypeArray allBoundDataTypes
    OpIntersect -> Set.map BTypeArray allBoundDataTypes
    OpRemove    -> allBoundDataTypes
    OpUnion     -> Set.map BTypeArray allBoundDataTypes
    OpArrJoin'  -> Set.singleton BTypeText
    OpIfNull    -> allBoundDataTypes
    OpNullIf    -> allBoundDataTypes
    OpDateStr   -> Set.singleton BTypeText
    OpStrDate   -> Set.singleton BTypeText
    OpSplit     -> Set.singleton BTypeText
    OpChunksOf  -> Set.singleton BTypeText
    OpTake      -> Set.singleton BTypeText
    OpTakeEnd   -> Set.singleton BTypeText
    OpDrop      -> Set.singleton BTypeText
    OpDropEnd   -> Set.singleton BTypeText
  bResType op t1 t2 = case op of
    OpAnd        -> BTypeBoolean
    OpOr         -> BTypeBoolean
    OpEQ         -> BTypeBoolean
    OpNEQ        -> BTypeBoolean
    OpLT         -> BTypeBoolean
    OpGT         -> BTypeBoolean
    OpLEQ        -> BTypeBoolean
    OpGEQ        -> BTypeBoolean
    OpAdd
      | t1 == BTypeInteger && t2 == BTypeInteger -> BTypeInteger
      | otherwise                                -> BTypeFloat
    OpSub
      | t1 == BTypeInteger && t2 == BTypeInteger -> BTypeInteger
      | otherwise                                -> BTypeFloat
    OpMul
      | t1 == BTypeInteger && t2 == BTypeInteger -> BTypeInteger
      | otherwise                                -> BTypeFloat
    OpContain    -> BTypeBoolean
    OpExcept     -> t1
    OpIntersect  -> t1
    OpRemove     -> t1
    OpUnion      -> t1
    OpArrJoin'   -> BTypeText
    OpIfNull     -> t2
    OpNullIf     -> t1
    OpDateStr    -> BTypeText
    OpStrDate    -> BTypeInteger
    OpSplit      -> BTypeArray BTypeText
    OpChunksOf   -> BTypeArray BTypeText
    OpTake       -> BTypeText
    OpTakeEnd    -> BTypeText
    OpDrop       -> BTypeText
    OpDropEnd    -> BTypeText

----------------------------------------
--           trinary operators
----------------------------------------
class TrianyOperator a where
  tOp1Type :: a -> Set.Set BoundDataType
  tOp2Type :: a -> Set.Set BoundDataType
  tOp3Type :: a -> Set.Set BoundDataType
  tResType :: a -> BoundDataType -> BoundDataType -> BoundDataType -> BoundDataType
  {-# MINIMAL tOp1Type, tOp2Type, tOp3Type, tResType #-}

instance TrianyOperator TerOp where
  tOp1Type _ = allBoundDataTypes
  tOp2Type _ = allBoundDataTypes
  tOp3Type _ = allBoundDataTypes
  tResType op _ _ _ = case op of
    OpBetweenAnd       -> BTypeBoolean
    OpNotBetweenAnd    -> BTypeBoolean
    OpBetweenSymAnd    -> BTypeBoolean
    OpNotBetweenSymAnd -> BTypeBoolean
