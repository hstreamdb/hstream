module HStream.SQL.Codegen.UnaryOp where

import qualified Data.List                 as L
import           Data.Scientific
import qualified Data.Text                 as T
import           GHC.Stack
import           HStream.SQL.AST
import           HStream.SQL.Codegen.Utils
import           HStream.SQL.Exception

--------------------------------------------------------------------------------
unaryOpOnValue :: HasCallStack => UnaryOp -> FlowValue -> FlowValue
unaryOpOnValue OpSin   v = op_sin v
unaryOpOnValue OpSinh  v = op_sinh v
unaryOpOnValue OpAsin  v = op_asin v
unaryOpOnValue OpAsinh v = op_asinh v
unaryOpOnValue OpCos   v = op_cos v
unaryOpOnValue OpCosh  v = op_cosh v
unaryOpOnValue OpAcos  v = op_acos v
unaryOpOnValue OpAcosh v = op_acosh v
unaryOpOnValue OpTan   v = op_tan v
unaryOpOnValue OpTanh  v = op_tanh v
unaryOpOnValue OpAtan  v = op_atan v
unaryOpOnValue OpAtanh v = op_atanh v
unaryOpOnValue OpAbs   v = op_abs v
unaryOpOnValue OpCeil  v = op_ceil v
unaryOpOnValue OpFloor v = op_floor v
unaryOpOnValue OpRound v = op_round v
unaryOpOnValue OpSign  v = op_sign v
unaryOpOnValue OpSqrt  v = op_sqrt v
unaryOpOnValue OpLog   v = op_log v
unaryOpOnValue OpLog2  v = op_log2 v
unaryOpOnValue OpLog10 v = op_log10 v
unaryOpOnValue OpExp   v = op_exp v
unaryOpOnValue OpIsInt v = op_isInt v
unaryOpOnValue OpIsFloat v = op_isFloat v
unaryOpOnValue OpIsNum v = op_isNum v
unaryOpOnValue OpIsBool v = op_isBool v
unaryOpOnValue OpIsStr v = op_isStr v
unaryOpOnValue OpIsMap v = op_isMap v
unaryOpOnValue OpIsArr v = op_isArr v
unaryOpOnValue OpIsDate v = op_isDate v
unaryOpOnValue OpIsTime v = op_isTime v
unaryOpOnValue OpToStr v = op_toStr v
unaryOpOnValue OpToLower v = op_toLower v
unaryOpOnValue OpToUpper v = op_toUpper v
unaryOpOnValue OpTrim v = op_trim v
unaryOpOnValue OpLTrim v = op_ltrim v
unaryOpOnValue OpRTrim v = op_rtrim v
unaryOpOnValue OpReverse v = op_reverse v
unaryOpOnValue OpStrLen v = op_strlen v
unaryOpOnValue OpDistinct v = op_distinct v
unaryOpOnValue OpArrJoin v = op_arrJoin v
unaryOpOnValue OpLength v = op_length v
unaryOpOnValue OpArrMax v = op_arrMax v
unaryOpOnValue OpArrMin v = op_arrMin v
unaryOpOnValue OpSort v = op_sort v
unaryOpOnValue _ FlowNull = FlowNull
unaryOpOnValue op v =
  throwRuntimeException $ "Unsupported operator <" <> show op <> "> on value <" <> show v <> ">"

--------------------------------------------------------------------------------
op_sin :: HasCallStack => FlowValue -> FlowValue
op_sin (FlowInt n)     = FlowNumeral (fromFloatDigits $ sin (fromIntegral n))
op_sin (FlowFloat n)   = FlowNumeral (fromFloatDigits $ sin n)
op_sin (FlowNumeral n) = FlowNumeral (funcOnScientific sin n)

op_sinh :: HasCallStack => FlowValue -> FlowValue
op_sinh (FlowInt n)     = FlowNumeral (fromFloatDigits $ sinh (fromIntegral n))
op_sinh (FlowFloat n)   = FlowNumeral (fromFloatDigits $ sinh n)
op_sinh (FlowNumeral n) = FlowNumeral (funcOnScientific sinh n)

op_asin :: HasCallStack => FlowValue -> FlowValue
op_asin (FlowInt n)
  | n >= (-1) && n <= 1 = FlowNumeral (fromFloatDigits $ asin (fromIntegral n))
  | otherwise = throwRuntimeException "Function <asin>: mathematical error"
op_asin (FlowFloat n)
  | n >= (-1) && n <= 1 = FlowNumeral (fromFloatDigits $ asin n)
  | otherwise = throwRuntimeException "Function <asin>: mathematical error"
op_asin (FlowNumeral n)
  | n >= (-1) && n <= 1 = FlowNumeral (funcOnScientific asin n)
  | otherwise = throwRuntimeException "Function <asin>: mathematical error"

op_asinh :: HasCallStack => FlowValue -> FlowValue
op_asinh (FlowInt n) = FlowNumeral (fromFloatDigits $ asinh (fromIntegral n))
op_asinh (FlowFloat n) = FlowNumeral (fromFloatDigits $ asinh n)
op_asinh (FlowNumeral n) = FlowNumeral (funcOnScientific asinh n)

op_cos :: HasCallStack => FlowValue -> FlowValue
op_cos (FlowInt n)     = FlowNumeral (fromFloatDigits $ cos (fromIntegral n))
op_cos (FlowFloat n)   = FlowNumeral (fromFloatDigits $ cos n)
op_cos (FlowNumeral n) = FlowNumeral (funcOnScientific cos n)

op_cosh :: HasCallStack => FlowValue -> FlowValue
op_cosh (FlowInt n)     = FlowNumeral (fromFloatDigits $ cosh (fromIntegral n))
op_cosh (FlowFloat n)   = FlowNumeral (fromFloatDigits $ cosh n)
op_cosh (FlowNumeral n) = FlowNumeral (funcOnScientific cosh n)

op_acos :: HasCallStack => FlowValue -> FlowValue
op_acos (FlowInt n)
  | n >= (-1) && n <= 1 = FlowNumeral (fromFloatDigits $ acos (fromIntegral n))
  | otherwise = throwRuntimeException "Function <acos>: mathematical error"
op_acos (FlowFloat n)
  | n >= (-1) && n <= 1 = FlowNumeral (fromFloatDigits $ acos n)
  | otherwise = throwRuntimeException "Function <acos>: mathematical error"
op_acos (FlowNumeral n)
  | n >= (-1) && n <= 1 = FlowNumeral (funcOnScientific acos n)
  | otherwise = throwRuntimeException "Function <acos>: mathematical error"

op_acosh :: HasCallStack => FlowValue -> FlowValue
op_acosh (FlowInt n)
  | n >= 1 = FlowNumeral (fromFloatDigits $ acosh (fromIntegral n))
  | otherwise = throwRuntimeException "Function <acosh>: mathematical error"
op_acosh (FlowFloat n)
  | n >= 1 = FlowNumeral (fromFloatDigits $ acosh n)
  | otherwise = throwRuntimeException "Function <acosh>: mathematical error"
op_acosh (FlowNumeral n)
  | n >= 1 = FlowNumeral (funcOnScientific acosh n)
  | otherwise = throwRuntimeException "Function <acosh>: mathematical error"

op_tan :: HasCallStack => FlowValue -> FlowValue
op_tan (FlowInt n)     = FlowNumeral (fromFloatDigits $ tan (fromIntegral n))
op_tan (FlowFloat n)   = FlowNumeral (fromFloatDigits $ tan n)
op_tan (FlowNumeral n) = FlowNumeral (funcOnScientific tan n)

op_tanh :: HasCallStack => FlowValue -> FlowValue
op_tanh (FlowInt n)     = FlowNumeral (fromFloatDigits $ tanh (fromIntegral n))
op_tanh (FlowFloat n)   = FlowNumeral (fromFloatDigits $ tanh n)
op_tanh (FlowNumeral n) = FlowNumeral (funcOnScientific tanh n)

op_atan :: HasCallStack => FlowValue -> FlowValue
op_atan (FlowInt n)     = FlowNumeral (fromFloatDigits $ atan (fromIntegral n))
op_atan (FlowFloat n)   = FlowNumeral (fromFloatDigits $ atan n)
op_atan (FlowNumeral n) = FlowNumeral (funcOnScientific atan n)

op_atanh :: HasCallStack => FlowValue -> FlowValue
op_atanh (FlowInt n)
  | n > (-1) && n < 1 = FlowNumeral (fromFloatDigits $ atanh (fromIntegral n))
  | otherwise = throwRuntimeException "Function <atanh>: mathematical error"
op_atanh (FlowFloat n)
  | n > (-1) && n < 1 = FlowNumeral (fromFloatDigits $ atanh n)
  | otherwise = throwRuntimeException "Function <atanh>: mathematical error"
op_atanh (FlowNumeral n)
  | n > (-1) && n < 1 = FlowNumeral (funcOnScientific atanh n)
  | otherwise = throwRuntimeException "Function <atanh>: mathematical error"

op_abs :: HasCallStack => FlowValue -> FlowValue
op_abs (FlowInt n)     = FlowInt (abs n)
op_abs (FlowFloat n)   = FlowFloat (abs n)
op_abs (FlowNumeral n) = FlowNumeral (funcOnScientific abs n)

op_ceil :: HasCallStack => FlowValue -> FlowValue
op_ceil (FlowInt n)     = FlowInt (ceiling $ fromIntegral n)
op_ceil (FlowFloat n)   = FlowInt (ceiling n)
op_ceil (FlowNumeral n) = FlowInt (ceiling $ toRealFloat n)

op_floor :: HasCallStack => FlowValue -> FlowValue
op_floor (FlowInt n)     = FlowInt (floor $ fromIntegral n)
op_floor (FlowFloat n)   = FlowInt (floor n)
op_floor (FlowNumeral n) = FlowInt (floor $ toRealFloat n)

op_round :: HasCallStack => FlowValue -> FlowValue
op_round (FlowInt n)     = FlowInt (round $ fromIntegral n)
op_round (FlowFloat n)   = FlowInt (round n)
op_round (FlowNumeral n) = FlowInt (round $ toRealFloat n)

op_sqrt :: HasCallStack => FlowValue -> FlowValue
op_sqrt (FlowInt n)     = FlowNumeral (fromFloatDigits $ sqrt (fromIntegral n))
op_sqrt (FlowFloat n)   = FlowNumeral (fromFloatDigits $ sqrt n)
op_sqrt (FlowNumeral n) = FlowNumeral (funcOnScientific sqrt n)

op_sign :: HasCallStack => FlowValue -> FlowValue
op_sign (FlowInt n)
  | n > 0  = FlowInt 1
  | n == 0 = FlowInt 0
  | n < 0  = FlowInt (-1)
op_sign (FlowFloat n)
  | n > 0  = FlowInt 1
  | n == 0 = FlowInt 0
  | n < 0  = FlowInt (-1)
op_sign (FlowNumeral n)
  | n > 0  = FlowInt 1
  | n == 0 = FlowInt 0
  | n < 0  = FlowInt (-1)

op_log :: HasCallStack => FlowValue -> FlowValue
op_log (FlowInt n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log (fromIntegral n))
  | otherwise = throwRuntimeException "Function <log>: mathematical error"
op_log (FlowFloat n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log n)
  | otherwise = throwRuntimeException "Function <log>: mathematical error"
op_log (FlowNumeral n)
  | n > 0 = FlowNumeral (funcOnScientific log n)
  | otherwise = throwRuntimeException "Function <log>: mathematical error"

op_log2 :: HasCallStack => FlowValue -> FlowValue
op_log2 (FlowInt n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log (fromIntegral n) / log 2)
  | otherwise = throwRuntimeException "Function <log2>: mathematical error"
op_log2 (FlowFloat n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log n / log 2)
  | otherwise = throwRuntimeException "Function <log2>: mathematical error"
op_log2 (FlowNumeral n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log (toRealFloat n) / log 2)
  | otherwise = throwRuntimeException "Function <log2>: mathematical error"

op_log10 :: HasCallStack => FlowValue -> FlowValue
op_log10 (FlowInt n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log (fromIntegral n) / log 10)
  | otherwise = throwRuntimeException "Function <log10>: mathematical error"
op_log10 (FlowFloat n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log n / log 10)
  | otherwise = throwRuntimeException "Function <log10>: mathematical error"
op_log10 (FlowNumeral n)
  | n > 0 = FlowNumeral (fromFloatDigits $ log (toRealFloat n) / log 10)
  | otherwise = throwRuntimeException "Function <log10>: mathematical error"

op_exp :: HasCallStack => FlowValue -> FlowValue
op_exp (FlowInt n)     = FlowNumeral (fromFloatDigits $ exp (fromIntegral n))
op_exp (FlowFloat n)   = FlowNumeral (fromFloatDigits $ exp n)
op_exp (FlowNumeral n) = FlowNumeral (funcOnScientific exp n)

op_isInt :: HasCallStack => FlowValue -> FlowValue
op_isInt (FlowInt _) = FlowBoolean True
op_isInt _           = FlowBoolean False

op_isFloat :: HasCallStack => FlowValue -> FlowValue
op_isFloat (FlowFloat _) = FlowBoolean True
op_isFloat _             = FlowBoolean False

op_isNum :: HasCallStack => FlowValue -> FlowValue
op_isNum (FlowNumeral _) = FlowBoolean True
op_isNum _               = FlowBoolean False

op_isBool :: HasCallStack => FlowValue -> FlowValue
op_isBool (FlowBoolean _) = FlowBoolean True
op_isBool _               = FlowBoolean False

op_isStr :: HasCallStack => FlowValue -> FlowValue
op_isStr (FlowText _) = FlowBoolean True
op_isStr _            = FlowBoolean False

op_isMap :: HasCallStack => FlowValue -> FlowValue
op_isMap (FlowMap _) = FlowBoolean True
op_isMap _           = FlowBoolean False

op_isArr :: HasCallStack => FlowValue -> FlowValue
op_isArr (FlowArray _) = FlowBoolean True
op_isArr _             = FlowBoolean False

op_isDate :: HasCallStack => FlowValue -> FlowValue
op_isDate (FlowDate _) = FlowBoolean True
op_isDate _            = FlowBoolean False

op_isTime :: HasCallStack => FlowValue -> FlowValue
op_isTime (FlowTime _) = FlowBoolean True
op_isTime _            = FlowBoolean False

op_toStr :: HasCallStack => FlowValue -> FlowValue
op_toStr v = FlowText (T.pack $ show v)

op_toLower :: HasCallStack => FlowValue -> FlowValue
op_toLower (FlowText t) = FlowText (T.toLower t)

op_toUpper :: HasCallStack => FlowValue -> FlowValue
op_toUpper (FlowText t) = FlowText (T.toUpper t)

op_trim :: HasCallStack => FlowValue -> FlowValue
op_trim (FlowText t) = FlowText (T.strip t)

op_ltrim :: HasCallStack => FlowValue -> FlowValue
op_ltrim (FlowText t) = FlowText (T.stripStart t)

op_rtrim :: HasCallStack => FlowValue -> FlowValue
op_rtrim (FlowText t) = FlowText (T.stripEnd t)

op_reverse :: HasCallStack => FlowValue -> FlowValue
op_reverse (FlowText t) = FlowText (T.reverse t)

op_strlen :: HasCallStack => FlowValue -> FlowValue
op_strlen (FlowText t) = FlowInt (T.length t)

op_distinct :: HasCallStack => FlowValue -> FlowValue
op_distinct (FlowArray arr) = FlowArray (L.nub arr)

op_length :: HasCallStack => FlowValue -> FlowValue
op_length (FlowArray arr) = FlowInt (L.length arr)

op_arrJoin :: HasCallStack => FlowValue -> FlowValue
op_arrJoin (FlowArray arr) = FlowText (arrJoinPrim arr Nothing)

op_arrMax :: HasCallStack => FlowValue -> FlowValue
op_arrMax (FlowArray arr) = L.maximum arr

op_arrMin :: HasCallStack => FlowValue -> FlowValue
op_arrMin (FlowArray arr) = L.minimum arr

op_sort :: HasCallStack => FlowValue -> FlowValue
op_sort (FlowArray arr) = FlowArray (L.sort arr)
