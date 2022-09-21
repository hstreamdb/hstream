{-# LANGUAGE BinaryLiterals     #-}
{-# LANGUAGE DeriveFunctor      #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PolyKinds          #-}
{-# LANGUAGE RankNTypes         #-}

module HStream.SQL.Validate.Utils where

import           Data.Bits
import           Data.Word       (Word8)
import           HStream.SQL.Abs

--------------------------------------------------------------------------------
class HaveValueExpr a where
  getValueExpr :: a -> ValueExpr

instance HaveValueExpr ScalarFunc where
  getValueExpr func = case func of
    (ScalarFuncFieldToJson _ e _)   -> e
    (ScalarFuncFieldToText _ e _)   -> e
    (ScalarFuncFieldsToJson _ e _)  -> e
    (ScalarFuncFieldsToTexts _ e _) -> e

    (ScalarFuncSin        _ e)      -> e
    (ScalarFuncSinh       _ e)      -> e
    (ScalarFuncAsin       _ e)      -> e
    (ScalarFuncAsinh      _ e)      -> e
    (ScalarFuncCos        _ e)      -> e
    (ScalarFuncCosh       _ e)      -> e
    (ScalarFuncAcos       _ e)      -> e
    (ScalarFuncAcosh      _ e)      -> e
    (ScalarFuncTan        _ e)      -> e
    (ScalarFuncTanh       _ e)      -> e
    (ScalarFuncAtan       _ e)      -> e
    (ScalarFuncAtanh      _ e)      -> e
    (ScalarFuncAbs        _ e)      -> e
    (ScalarFuncCeil       _ e)      -> e
    (ScalarFuncFloor      _ e)      -> e
    (ScalarFuncRound      _ e)      -> e
    (ScalarFuncSqrt       _ e)      -> e
    (ScalarFuncSign       _ e)      -> e
    (ScalarFuncLog        _ e)      -> e
    (ScalarFuncLog2       _ e)      -> e
    (ScalarFuncLog10      _ e)      -> e
    (ScalarFuncExp        _ e)      -> e
    (ScalarFuncIsInt      _ e)      -> e
    (ScalarFuncIsFloat    _ e)      -> e
    (ScalarFuncIsNum      _ e)      -> e
    (ScalarFuncIsBool     _ e)      -> e
    (ScalarFuncIsStr      _ e)      -> e
    (ScalarFuncIsMap      _ e)      -> e
    (ScalarFuncIsArr      _ e)      -> e
    (ScalarFuncIsDate     _ e)      -> e
    (ScalarFuncIsTime     _ e)      -> e
    (ScalarFuncToStr      _ e)      -> e
    (ScalarFuncToLower    _ e)      -> e
    (ScalarFuncToUpper    _ e)      -> e
    (ScalarFuncTrim       _ e)      -> e
    (ScalarFuncLTrim      _ e)      -> e
    (ScalarFuncRTrim      _ e)      -> e
    (ScalarFuncRev        _ e)      -> e
    (ScalarFuncStrlen     _ e)      -> e
    (ScalarFuncIfNull   _ e _)      -> e
    (ScalarFuncNullIf   _ e _)      -> e
    (ArrayFuncContain   _ e _)      -> e
    (ArrayFuncDistinct    _ e)      -> e
    (ArrayFuncExcept    _ e _)      -> e
    (ArrayFuncIntersect _ e _)      -> e
    (ArrayFuncLength      _ e)      -> e
    (ArrayFuncRemove    _ e _)      -> e
    (ArrayFuncUnion     _ e _)      -> e
    (ArrayFuncJoin        _ e)      -> e
    (ArrayFuncJoinWith  _ e _)      -> e
    (ArrayFuncMax         _ e)      -> e
    (ArrayFuncMin         _ e)      -> e
    (ArrayFuncSort        _ e)      -> e
    (ScalarFuncDateStr  _ e _)      -> e
    (ScalarFuncStrDate  _ e _)      -> e
    (ScalarFuncSplit    _ e _)      -> e
    (ScalarFuncChunksOf _ e _)      -> e
    (ScalarFuncTake     _ e _)      -> e
    (ScalarFuncTakeEnd  _ e _)      -> e
    (ScalarFuncDrop     _ e _)      -> e
    (ScalarFuncDropEnd  _ e _)      -> e

class HavePos a where
  getPos :: a -> BNFC'Position

instance HavePos DataType where
  getPos (TypeInteger pos)   = pos
  getPos (TypeFloat pos)     = pos
  getPos (TypeNumeric pos)   = pos
  getPos (TypeBoolean pos)   = pos
  getPos (TypeByte pos)      = pos
  getPos (TypeText pos)      = pos
  getPos (TypeDate pos)      = pos
  getPos (TypeTime pos)      = pos
  getPos (TypeTimestamp pos) = pos
  getPos (TypeInterval pos)  = pos
  getPos (TypeJson pos)      = pos
  getPos (TypeArray pos _)   = pos
  getPos (TypeMap pos _ _)   = pos

instance HavePos ScalarFunc where
  getPos func = case func of
    (ScalarFuncFieldToJson pos _ _)   -> pos
    (ScalarFuncFieldToText pos _ _)   -> pos
    (ScalarFuncFieldsToJson pos _ _)  -> pos
    (ScalarFuncFieldsToTexts pos _ _) -> pos

    (ScalarFuncSin        pos _)      -> pos
    (ScalarFuncSinh       pos _)      -> pos
    (ScalarFuncAsin       pos _)      -> pos
    (ScalarFuncAsinh      pos _)      -> pos
    (ScalarFuncCos        pos _)      -> pos
    (ScalarFuncCosh       pos _)      -> pos
    (ScalarFuncAcos       pos _)      -> pos
    (ScalarFuncAcosh      pos _)      -> pos
    (ScalarFuncTan        pos _)      -> pos
    (ScalarFuncTanh       pos _)      -> pos
    (ScalarFuncAtan       pos _)      -> pos
    (ScalarFuncAtanh      pos _)      -> pos
    (ScalarFuncAbs        pos _)      -> pos
    (ScalarFuncCeil       pos _)      -> pos
    (ScalarFuncFloor      pos _)      -> pos
    (ScalarFuncRound      pos _)      -> pos
    (ScalarFuncSqrt       pos _)      -> pos
    (ScalarFuncSign       pos _)      -> pos
    (ScalarFuncLog        pos _)      -> pos
    (ScalarFuncLog2       pos _)      -> pos
    (ScalarFuncLog10      pos _)      -> pos
    (ScalarFuncExp        pos _)      -> pos
    (ScalarFuncIsInt      pos _)      -> pos
    (ScalarFuncIsFloat    pos _)      -> pos
    (ScalarFuncIsNum      pos _)      -> pos
    (ScalarFuncIsBool     pos _)      -> pos
    (ScalarFuncIsStr      pos _)      -> pos
    (ScalarFuncIsMap      pos _)      -> pos
    (ScalarFuncIsArr      pos _)      -> pos
    (ScalarFuncIsDate     pos _)      -> pos
    (ScalarFuncIsTime     pos _)      -> pos
    (ScalarFuncToStr      pos _)      -> pos
    (ScalarFuncToLower    pos _)      -> pos
    (ScalarFuncToUpper    pos _)      -> pos
    (ScalarFuncTrim       pos _)      -> pos
    (ScalarFuncLTrim      pos _)      -> pos
    (ScalarFuncRTrim      pos _)      -> pos
    (ScalarFuncRev        pos _)      -> pos
    (ScalarFuncStrlen     pos _)      -> pos
    (ScalarFuncIfNull   pos _ _)      -> pos
    (ScalarFuncNullIf   pos _ _)      -> pos
    (ArrayFuncContain   pos _ _)      -> pos
    (ArrayFuncDistinct    pos _)      -> pos
    (ArrayFuncExcept    pos _ _)      -> pos
    (ArrayFuncIntersect pos _ _)      -> pos
    (ArrayFuncLength      pos _)      -> pos
    (ArrayFuncRemove    pos _ _)      -> pos
    (ArrayFuncUnion     pos _ _)      -> pos
    (ArrayFuncJoin        pos _)      -> pos
    (ArrayFuncJoinWith  pos _ _)      -> pos
    (ArrayFuncMax         pos _)      -> pos
    (ArrayFuncMin         pos _)      -> pos
    (ArrayFuncSort        pos _)      -> pos
    (ScalarFuncDateStr  pos _ _)      -> pos
    (ScalarFuncStrDate  pos _ _)      -> pos
    (ScalarFuncSplit    pos _ _)      -> pos
    (ScalarFuncChunksOf pos _ _)      -> pos
    (ScalarFuncTake     pos _ _)      -> pos
    (ScalarFuncTakeEnd  pos _ _)      -> pos
    (ScalarFuncDrop     pos _ _)      -> pos
    (ScalarFuncDropEnd  pos _ _)      -> pos

instance HavePos TableRef where
  getPos ref = case ref of
    TableRefAs     pos _ _        -> pos
    TableRefJoinOn pos _ _ _ _    -> pos
    TableRefJoinUsing pos _ _ _ _ -> pos
    TableRefIdent pos _           -> pos
    TableRefSubquery pos _        -> pos

--------------------------------------------------------------------------------
-- mask
-- 0b    *      *      *     *           *   *    *    *
--    Unused   Any  String Bool         Ord Num Float Int
intMask, floatMask, numMask, ordMask, boolMask, stringMask, anyMask :: Word8
intMask    = 0b0000_0001
floatMask  = 0b0000_0010
numMask    = 0b0000_0100
ordMask    = 0b0000_1000
boolMask   = 0b0001_0000
stringMask = 0b0010_0000
anyMask    = 0b0100_0000

getScalarFuncType :: ScalarFunc -> Word8
getScalarFuncType f = case f of
  (ScalarFuncFieldToJson _ _ _)   -> 0b0100_0000
  (ScalarFuncFieldToText _ _ _)   -> 0b0100_0000
  (ScalarFuncFieldsToJson _ _ _)  -> 0b0100_0000
  (ScalarFuncFieldsToTexts _ _ _) -> 0b0100_0000

  (ScalarFuncSin        _ _)      -> 0b0000_1110
  (ScalarFuncSinh       _ _)      -> 0b0000_1110
  (ScalarFuncAsin       _ _)      -> 0b0000_1110
  (ScalarFuncAsinh      _ _)      -> 0b0000_1110
  (ScalarFuncCos        _ _)      -> 0b0000_1110
  (ScalarFuncCosh       _ _)      -> 0b0000_1110
  (ScalarFuncAcos       _ _)      -> 0b0000_1110
  (ScalarFuncAcosh      _ _)      -> 0b0000_1110
  (ScalarFuncTan        _ _)      -> 0b0000_1110
  (ScalarFuncTanh       _ _)      -> 0b0000_1110
  (ScalarFuncAtan       _ _)      -> 0b0000_1110
  (ScalarFuncAtanh      _ _)      -> 0b0000_1110
  (ScalarFuncAbs        _ _)      -> 0b0000_1110
  (ScalarFuncCeil       _ _)      -> 0b0000_1101
  (ScalarFuncFloor      _ _)      -> 0b0000_1101
  (ScalarFuncRound      _ _)      -> 0b0000_1101
  (ScalarFuncSqrt       _ _)      -> 0b0000_1110
  (ScalarFuncSign       _ _)      -> 0b0000_0001
  (ScalarFuncLog        _ _)      -> 0b0000_1110
  (ScalarFuncLog2       _ _)      -> 0b0000_1110
  (ScalarFuncLog10      _ _)      -> 0b0000_1110
  (ScalarFuncExp        _ _)      -> 0b0000_1110
  (ScalarFuncIsInt      _ _)      -> 0b0001_0000
  (ScalarFuncIsFloat    _ _)      -> 0b0001_0000
  (ScalarFuncIsNum      _ _)      -> 0b0001_0000
  (ScalarFuncIsBool     _ _)      -> 0b0001_0000
  (ScalarFuncIsStr      _ _)      -> 0b0001_0000
  (ScalarFuncIsMap      _ _)      -> 0b0001_0000
  (ScalarFuncIsArr      _ _)      -> 0b0001_0000
  (ScalarFuncIsDate     _ _)      -> 0b0001_0000
  (ScalarFuncIsTime     _ _)      -> 0b0001_0000
  (ScalarFuncToStr      _ _)      -> 0b0010_1000
  (ScalarFuncToLower    _ _)      -> 0b0010_1000
  (ScalarFuncToUpper    _ _)      -> 0b0010_1000
  (ScalarFuncTrim       _ _)      -> 0b0010_1000
  (ScalarFuncLTrim      _ _)      -> 0b0010_1000
  (ScalarFuncRTrim      _ _)      -> 0b0010_1000
  (ScalarFuncRev        _ _)      -> 0b0010_1000
  (ScalarFuncStrlen     _ _)      -> 0b0000_1101
  (ScalarFuncIfNull   _ _ _)      -> 0b0100_0000
  (ScalarFuncNullIf   _ _ _)      -> 0b0100_0000
  (ArrayFuncContain   _ _ _)      -> 0b0001_0000
  (ArrayFuncDistinct    _ _)      -> 0b0100_0000
  (ArrayFuncExcept    _ _ _)      -> 0b0100_0000
  (ArrayFuncIntersect _ _ _)      -> 0b0100_0000
  (ArrayFuncLength      _ _)      -> 0b0000_0001
  (ArrayFuncRemove    _ _ _)      -> 0b0100_0000
  (ArrayFuncUnion     _ _ _)      -> 0b0100_0000
  (ArrayFuncJoin        _ _)      -> 0b0010_0000
  (ArrayFuncJoinWith  _ _ _)      -> 0b0010_0000
  (ArrayFuncMax         _ _)      -> 0b0100_0000
  (ArrayFuncMin         _ _)      -> 0b0100_0000
  (ArrayFuncSort        _ _)      -> 0b0100_0000
  (ScalarFuncDateStr  _ _ _)      -> 0b0010_0000
  (ScalarFuncStrDate  _ _ _)      -> 0b0000_0100
  (ScalarFuncSplit    _ _ _)      -> 0b0100_0000
  (ScalarFuncChunksOf _ _ _)      -> 0b0100_0000
  (ScalarFuncTake     _ _ _)      -> 0b0010_0000
  (ScalarFuncTakeEnd  _ _ _)      -> 0b0010_0000
  (ScalarFuncDrop     _ _ _)      -> 0b0010_0000
  (ScalarFuncDropEnd  _ _ _)      -> 0b0010_0000

getScalarArgType :: ScalarFunc -> Word8
getScalarArgType f = case f of
  (ScalarFuncFieldToJson _ _ _)   -> anyMask
  (ScalarFuncFieldToText _ _ _)   -> anyMask
  (ScalarFuncFieldsToJson _ _ _)  -> anyMask
  (ScalarFuncFieldsToTexts _ _ _) -> anyMask

  (ScalarFuncSin        _ _)      -> numMask
  (ScalarFuncSinh       _ _)      -> numMask
  (ScalarFuncAsin       _ _)      -> numMask
  (ScalarFuncAsinh      _ _)      -> numMask
  (ScalarFuncCos        _ _)      -> numMask
  (ScalarFuncCosh       _ _)      -> numMask
  (ScalarFuncAcos       _ _)      -> numMask
  (ScalarFuncAcosh      _ _)      -> numMask
  (ScalarFuncTan        _ _)      -> numMask
  (ScalarFuncTanh       _ _)      -> numMask
  (ScalarFuncAtan       _ _)      -> numMask
  (ScalarFuncAtanh      _ _)      -> numMask
  (ScalarFuncAbs        _ _)      -> numMask
  (ScalarFuncCeil       _ _)      -> numMask
  (ScalarFuncFloor      _ _)      -> numMask
  (ScalarFuncRound      _ _)      -> numMask
  (ScalarFuncSqrt       _ _)      -> numMask
  (ScalarFuncSign       _ _)      -> numMask
  (ScalarFuncLog        _ _)      -> numMask
  (ScalarFuncLog2       _ _)      -> numMask
  (ScalarFuncLog10      _ _)      -> numMask
  (ScalarFuncExp        _ _)      -> numMask
  (ScalarFuncIsInt      _ _)      -> anyMask
  (ScalarFuncIsFloat    _ _)      -> anyMask
  (ScalarFuncIsNum      _ _)      -> anyMask
  (ScalarFuncIsBool     _ _)      -> anyMask
  (ScalarFuncIsStr      _ _)      -> anyMask
  (ScalarFuncIsMap      _ _)      -> anyMask
  (ScalarFuncIsArr      _ _)      -> anyMask
  (ScalarFuncIsDate     _ _)      -> anyMask
  (ScalarFuncIsTime     _ _)      -> anyMask
  (ScalarFuncToStr      _ _)      -> anyMask
  (ScalarFuncToLower    _ _)      -> stringMask
  (ScalarFuncToUpper    _ _)      -> stringMask
  (ScalarFuncTrim       _ _)      -> stringMask
  (ScalarFuncLTrim      _ _)      -> stringMask
  (ScalarFuncRTrim      _ _)      -> stringMask
  (ScalarFuncRev        _ _)      -> stringMask
  (ScalarFuncStrlen     _ _)      -> stringMask
  (ScalarFuncIfNull   _ _ _)      -> anyMask
  (ScalarFuncNullIf   _ _ _)      -> anyMask
  (ArrayFuncContain   _ _ _)      -> anyMask
  (ArrayFuncDistinct    _ _)      -> anyMask
  (ArrayFuncExcept    _ _ _)      -> anyMask
  (ArrayFuncIntersect _ _ _)      -> anyMask
  (ArrayFuncLength      _ _)      -> anyMask
  (ArrayFuncRemove    _ _ _)      -> anyMask
  (ArrayFuncUnion     _ _ _)      -> anyMask
  (ArrayFuncJoin        _ _)      -> anyMask
  (ArrayFuncJoinWith  _ _ _)      -> anyMask
  (ArrayFuncMax         _ _)      -> anyMask
  (ArrayFuncMin         _ _)      -> anyMask
  (ArrayFuncSort        _ _)      -> anyMask
  (ScalarFuncDateStr  _ _ _)      -> numMask
  (ScalarFuncStrDate  _ _ _)      -> stringMask
  (ScalarFuncSplit    _ _ _)      -> stringMask
  (ScalarFuncChunksOf _ _ _)      -> stringMask
  (ScalarFuncTake     _ _ _)      -> anyMask
  (ScalarFuncTakeEnd  _ _ _)      -> anyMask
  (ScalarFuncDrop     _ _ _)      -> anyMask
  (ScalarFuncDropEnd  _ _ _)      -> anyMask

isTypeInt, isTypeFloat, isTypeNum, isTypeOrd, isTypeBool, isTypeString :: Word8 -> Bool
isTypeInt    n = n .&. intMask    /= 0 || n .&. anyMask /= 0
isTypeFloat  n = n .&. floatMask  /= 0 || n .&. anyMask /= 0
isTypeNum    n = n .&. numMask    /= 0 || n .&. anyMask /= 0
isTypeOrd    n = n .&. ordMask    /= 0 || n .&. anyMask /= 0
isTypeBool   n = n .&. boolMask   /= 0 || n .&. anyMask /= 0
isTypeString n = n .&. stringMask /= 0 || n .&. anyMask /= 0

--------------------------------------------------------------------------------
anyAggInSelList :: SelList -> Bool
anyAggInSelList (SelListAsterisk _)      = False
anyAggInSelList (SelListSublist _ dcols) = or $ isAggDCol <$> dcols
  where isAggDCol (DerivedColSimpl _ (ExprSetFunc _ _)) = True
        isAggDCol (DerivedColAs _ (ExprSetFunc _ _) _)  = True
        isAggDCol _                                     = False
