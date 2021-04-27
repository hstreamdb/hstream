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
    (ScalarFuncSin     _ e) -> e
    (ScalarFuncSinh    _ e) -> e
    (ScalarFuncAsin    _ e) -> e
    (ScalarFuncAsinh   _ e) -> e
    (ScalarFuncCos     _ e) -> e
    (ScalarFuncCosh    _ e) -> e
    (ScalarFuncAcos    _ e) -> e
    (ScalarFuncAcosh   _ e) -> e
    (ScalarFuncTan     _ e) -> e
    (ScalarFuncTanh    _ e) -> e
    (ScalarFuncAtan    _ e) -> e
    (ScalarFuncAtanh   _ e) -> e
    (ScalarFuncAbs     _ e) -> e
    (ScalarFuncCeil    _ e) -> e
    (ScalarFuncFloor   _ e) -> e
    (ScalarFuncRound   _ e) -> e
    (ScalarFuncSqrt    _ e) -> e
    (ScalarFuncLog     _ e) -> e
    (ScalarFuncLog2    _ e) -> e
    (ScalarFuncLog10   _ e) -> e
    (ScalarFuncExp     _ e) -> e
    (ScalarFuncIsInt   _ e) -> e
    (ScalarFuncIsFloat _ e) -> e
    (ScalarFuncIsNum   _ e) -> e
    (ScalarFuncIsBool  _ e) -> e
    (ScalarFuncIsStr   _ e) -> e
    (ScalarFuncIsMap   _ e) -> e
    (ScalarFuncIsArr   _ e) -> e
    (ScalarFuncIsDate  _ e) -> e
    (ScalarFuncIsTime  _ e) -> e
    (ScalarFuncToStr   _ e) -> e
    (ScalarFuncToLower _ e) -> e
    (ScalarFuncToUpper _ e) -> e
    (ScalarFuncTrim    _ e) -> e
    (ScalarFuncLTrim   _ e) -> e
    (ScalarFuncRTrim   _ e) -> e
    (ScalarFuncRev     _ e) -> e
    (ScalarFuncStrlen  _ e) -> e

class HavePos a where
  getPos :: a -> BNFC'Position

instance HavePos ScalarFunc where
  getPos func = case func of
    (ScalarFuncSin     pos _) -> pos
    (ScalarFuncSinh    pos _) -> pos
    (ScalarFuncAsin    pos _) -> pos
    (ScalarFuncAsinh   pos _) -> pos
    (ScalarFuncCos     pos _) -> pos
    (ScalarFuncCosh    pos _) -> pos
    (ScalarFuncAcos    pos _) -> pos
    (ScalarFuncAcosh   pos _) -> pos
    (ScalarFuncTan     pos _) -> pos
    (ScalarFuncTanh    pos _) -> pos
    (ScalarFuncAtan    pos _) -> pos
    (ScalarFuncAtanh   pos _) -> pos
    (ScalarFuncAbs     pos _) -> pos
    (ScalarFuncCeil    pos _) -> pos
    (ScalarFuncFloor   pos _) -> pos
    (ScalarFuncRound   pos _) -> pos
    (ScalarFuncSqrt    pos _) -> pos
    (ScalarFuncLog     pos _) -> pos
    (ScalarFuncLog2    pos _) -> pos
    (ScalarFuncLog10   pos _) -> pos
    (ScalarFuncExp     pos _) -> pos
    (ScalarFuncIsInt   pos _) -> pos
    (ScalarFuncIsFloat pos _) -> pos
    (ScalarFuncIsNum   pos _) -> pos
    (ScalarFuncIsBool  pos _) -> pos
    (ScalarFuncIsStr   pos _) -> pos
    (ScalarFuncIsMap   pos _) -> pos
    (ScalarFuncIsArr   pos _) -> pos
    (ScalarFuncIsDate  pos _) -> pos
    (ScalarFuncIsTime  pos _) -> pos
    (ScalarFuncToStr   pos _) -> pos
    (ScalarFuncToLower pos _) -> pos
    (ScalarFuncToUpper pos _) -> pos
    (ScalarFuncTrim    pos _) -> pos
    (ScalarFuncLTrim   pos _) -> pos
    (ScalarFuncRTrim   pos _) -> pos
    (ScalarFuncRev     pos _) -> pos
    (ScalarFuncStrlen  pos _) -> pos

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
  (ScalarFuncSin     _ _) -> 0b0000_1110
  (ScalarFuncSinh    _ _) -> 0b0000_1110
  (ScalarFuncAsin    _ _) -> 0b0000_1110
  (ScalarFuncAsinh   _ _) -> 0b0000_1110
  (ScalarFuncCos     _ _) -> 0b0000_1110
  (ScalarFuncCosh    _ _) -> 0b0000_1110
  (ScalarFuncAcos    _ _) -> 0b0000_1110
  (ScalarFuncAcosh   _ _) -> 0b0000_1110
  (ScalarFuncTan     _ _) -> 0b0000_1110
  (ScalarFuncTanh    _ _) -> 0b0000_1110
  (ScalarFuncAtan    _ _) -> 0b0000_1110
  (ScalarFuncAtanh   _ _) -> 0b0000_1110
  (ScalarFuncAbs     _ _) -> 0b0000_1110
  (ScalarFuncCeil    _ _) -> 0b0000_1101
  (ScalarFuncFloor   _ _) -> 0b0000_1101
  (ScalarFuncRound   _ _) -> 0b0000_1101
  (ScalarFuncSqrt    _ _) -> 0b0000_1110
  (ScalarFuncLog     _ _) -> 0b0000_1110
  (ScalarFuncLog2    _ _) -> 0b0000_1110
  (ScalarFuncLog10   _ _) -> 0b0000_1110
  (ScalarFuncExp     _ _) -> 0b0000_1110
  (ScalarFuncIsInt   _ _) -> 0b0001_0000
  (ScalarFuncIsFloat _ _) -> 0b0001_0000
  (ScalarFuncIsNum   _ _) -> 0b0001_0000
  (ScalarFuncIsBool  _ _) -> 0b0001_0000
  (ScalarFuncIsStr   _ _) -> 0b0001_0000
  (ScalarFuncIsMap   _ _) -> 0b0001_0000
  (ScalarFuncIsArr   _ _) -> 0b0001_0000
  (ScalarFuncIsDate  _ _) -> 0b0001_0000
  (ScalarFuncIsTime  _ _) -> 0b0001_0000
  (ScalarFuncToStr   _ _) -> 0b0010_1000
  (ScalarFuncToLower _ _) -> 0b0010_1000
  (ScalarFuncToUpper _ _) -> 0b0010_1000
  (ScalarFuncTrim    _ _) -> 0b0010_1000
  (ScalarFuncLTrim   _ _) -> 0b0010_1000
  (ScalarFuncRTrim   _ _) -> 0b0010_1000
  (ScalarFuncRev     _ _) -> 0b0010_1000
  (ScalarFuncStrlen  _ _) -> 0b0000_1101

getScalarArgType :: ScalarFunc -> Word8
getScalarArgType f = case f of
  (ScalarFuncSin     _ _) -> numMask
  (ScalarFuncSinh    _ _) -> numMask
  (ScalarFuncAsin    _ _) -> numMask
  (ScalarFuncAsinh   _ _) -> numMask
  (ScalarFuncCos     _ _) -> numMask
  (ScalarFuncCosh    _ _) -> numMask
  (ScalarFuncAcos    _ _) -> numMask
  (ScalarFuncAcosh   _ _) -> numMask
  (ScalarFuncTan     _ _) -> numMask
  (ScalarFuncTanh    _ _) -> numMask
  (ScalarFuncAtan    _ _) -> numMask
  (ScalarFuncAtanh   _ _) -> numMask
  (ScalarFuncAbs     _ _) -> numMask
  (ScalarFuncCeil    _ _) -> numMask
  (ScalarFuncFloor   _ _) -> numMask
  (ScalarFuncRound   _ _) -> numMask
  (ScalarFuncSqrt    _ _) -> numMask
  (ScalarFuncLog     _ _) -> numMask
  (ScalarFuncLog2    _ _) -> numMask
  (ScalarFuncLog10   _ _) -> numMask
  (ScalarFuncExp     _ _) -> numMask
  (ScalarFuncIsInt   _ _) -> anyMask
  (ScalarFuncIsFloat _ _) -> anyMask
  (ScalarFuncIsNum   _ _) -> anyMask
  (ScalarFuncIsBool  _ _) -> anyMask
  (ScalarFuncIsStr   _ _) -> anyMask
  (ScalarFuncIsMap   _ _) -> anyMask
  (ScalarFuncIsArr   _ _) -> anyMask
  (ScalarFuncIsDate  _ _) -> anyMask
  (ScalarFuncIsTime  _ _) -> anyMask
  (ScalarFuncToStr   _ _) -> anyMask
  (ScalarFuncToLower _ _) -> stringMask
  (ScalarFuncToUpper _ _) -> stringMask
  (ScalarFuncTrim    _ _) -> stringMask
  (ScalarFuncLTrim   _ _) -> stringMask
  (ScalarFuncRTrim   _ _) -> stringMask
  (ScalarFuncRev     _ _) -> stringMask
  (ScalarFuncStrlen  _ _) -> stringMask

isTypeInt, isTypeFloat, isTypeNum, isTypeOrd, isTypeBool, isTypeString :: Word8 -> Bool
isTypeInt    n = n .&. intMask    /= 0
isTypeFloat  n = n .&. floatMask  /= 0
isTypeNum    n = n .&. numMask    /= 0
isTypeOrd    n = n .&. ordMask    /= 0
isTypeBool   n = n .&. boolMask   /= 0
isTypeString n = n .&. stringMask /= 0
