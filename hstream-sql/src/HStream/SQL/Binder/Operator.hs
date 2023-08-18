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
  where basic = Set.fromList [ BTypeInteger, BTypeFloat, BTypeText, BTypeBoolean, BTypeJsonb, BTypeBytea, BTypeTimestamp, BTypeDate, BTypeTime, BTypeInterval, BTypeUnknown ]
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
    ConstantNull           -> BTypeUnknown
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
      | L.null vs -> BTypeArray BTypeUnknown
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
    OpSin      -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpSinh     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAsin     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAsinh    -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpCos      -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpCosh     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAcos     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAcosh    -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpTan      -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpTanh     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAtan     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAtanh    -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpAbs      -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpCeil     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpFloor    -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpRound    -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpSign     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpSqrt     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpLog      -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpLog2     -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpLog10    -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpExp      -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpIsInt    -> allBoundDataTypes
    OpIsFloat  -> allBoundDataTypes
    OpIsBool   -> allBoundDataTypes
    OpIsStr    -> allBoundDataTypes
    OpIsArr    -> allBoundDataTypes
    OpIsDate   -> allBoundDataTypes
    OpIsTime   -> allBoundDataTypes
    OpIsNum    -> allBoundDataTypes
    OpToStr    -> allBoundDataTypes
    OpToLower  -> Set.fromList [BTypeText, BTypeUnknown]
    OpToUpper  -> Set.fromList [BTypeText, BTypeUnknown]
    OpTrim     -> Set.fromList [BTypeText, BTypeUnknown]
    OpLTrim    -> Set.fromList [BTypeText, BTypeUnknown]
    OpRTrim    -> Set.fromList [BTypeText, BTypeUnknown]
    OpReverse  -> Set.fromList [BTypeText, BTypeUnknown]
    OpStrLen   -> Set.fromList [BTypeText, BTypeUnknown]
    OpDistinct -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpArrJoin  -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpLength   -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpArrMax   -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpArrMin   -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpSort     -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpNot      -> Set.fromList [BTypeText, BTypeUnknown]
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
  bOp1Type _ = Set.fromList [BTypeJsonb, BTypeUnknown]
  bOp2Type op = case op of
    JOpArrow         -> Set.fromList [ BTypeText, BTypeUnknown ]
    JOpLongArrow     -> Set.fromList [ BTypeText, BTypeUnknown ]
    JOpHashArrow     -> Set.fromList [ BTypeArray BTypeText
                                     , BTypeArray BTypeInteger
                                     , BTypeArray BTypeUnknown
                                     , BTypeUnknown ]
    JOpHashLongArrow -> Set.fromList [ BTypeArray BTypeText
                                     , BTypeArray BTypeInteger
                                     , BTypeArray BTypeUnknown
                                     , BTypeUnknown ]
  bResType op _ _ = case op of
    JOpArrow         -> BTypeUnknown
    JOpLongArrow     -> BTypeText
    JOpHashArrow     -> BTypeUnknown
    JOpHashLongArrow -> BTypeText

instance BinaryOperator BinaryOp where
  bOp1Type op = case op of
    OpAnd       -> Set.fromList [BTypeBoolean, BTypeUnknown]
    OpOr        -> Set.fromList [BTypeBoolean, BTypeUnknown]
    OpEQ        -> allBoundDataTypes
    OpNEQ       -> allBoundDataTypes
    OpLT        -> allBoundDataTypes
    OpGT        -> allBoundDataTypes
    OpLEQ       -> allBoundDataTypes
    OpGEQ       -> allBoundDataTypes
    OpAdd       -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpSub       -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpMul       -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpContain   -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpExcept    -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpIntersect -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpRemove    -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpUnion     -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpArrJoin'  -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpIfNull    -> allBoundDataTypes
    OpNullIf    -> allBoundDataTypes
    OpDateStr   -> Set.fromList [BTypeInteger, BTypeUnknown]
    OpStrDate   -> Set.fromList [BTypeText   , BTypeUnknown]
    OpSplit     -> Set.fromList [BTypeText   , BTypeUnknown]
    OpChunksOf  -> Set.fromList [BTypeInteger, BTypeUnknown]
    OpTake      -> Set.fromList [BTypeInteger, BTypeUnknown]
    OpTakeEnd   -> Set.fromList [BTypeInteger, BTypeUnknown]
    OpDrop      -> Set.fromList [BTypeInteger, BTypeUnknown]
    OpDropEnd   -> Set.fromList [BTypeInteger, BTypeUnknown]
  bOp2Type op = case op of
    OpAnd       -> Set.fromList [BTypeBoolean, BTypeUnknown]
    OpOr        -> Set.fromList [BTypeBoolean, BTypeUnknown]
    OpEQ        -> allBoundDataTypes
    OpNEQ       -> allBoundDataTypes
    OpLT        -> allBoundDataTypes
    OpGT        -> allBoundDataTypes
    OpLEQ       -> allBoundDataTypes
    OpGEQ       -> allBoundDataTypes
    OpAdd       -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpSub       -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpMul       -> Set.fromList [BTypeInteger, BTypeFloat, BTypeUnknown]
    OpContain   -> allBoundDataTypes
    OpExcept    -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpIntersect -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpRemove    -> allBoundDataTypes
    OpUnion     -> Set.singleton BTypeUnknown `Set.union` Set.map BTypeArray allBoundDataTypes
    OpArrJoin'  -> Set.fromList [BTypeText, BTypeUnknown]
    OpIfNull    -> allBoundDataTypes
    OpNullIf    -> allBoundDataTypes
    OpDateStr   -> Set.fromList [BTypeText, BTypeUnknown]
    OpStrDate   -> Set.fromList [BTypeText, BTypeUnknown]
    OpSplit     -> Set.fromList [BTypeText, BTypeUnknown]
    OpChunksOf  -> Set.fromList [BTypeText, BTypeUnknown]
    OpTake      -> Set.fromList [BTypeText, BTypeUnknown]
    OpTakeEnd   -> Set.fromList [BTypeText, BTypeUnknown]
    OpDrop      -> Set.fromList [BTypeText, BTypeUnknown]
    OpDropEnd   -> Set.fromList [BTypeText, BTypeUnknown]
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
