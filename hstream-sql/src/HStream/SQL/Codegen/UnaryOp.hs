{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.UnaryOp
  ( unaryOpOnValue
  ) where

import qualified Data.List                 as L
import           Data.Scientific
import qualified Data.Text                 as T
#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST
import           HStream.SQL.Codegen.Utils

#ifdef HStreamUseV2Engine
#define ERROR_TYPE DiffFlowError
#define ERR RunShardError
#else
#define ERROR_TYPE HStreamProcessingError
#define ERR OperationError
#endif

--------------------------------------------------------------------------------
unaryOpOnValue :: UnaryOp -> FlowValue -> Either ERROR_TYPE FlowValue
unaryOpOnValue OpSin   v    = op_sin v
unaryOpOnValue OpSinh  v    = op_sinh v
unaryOpOnValue OpAsin  v    = op_asin v
unaryOpOnValue OpAsinh v    = op_asinh v
unaryOpOnValue OpCos   v    = op_cos v
unaryOpOnValue OpCosh  v    = op_cosh v
unaryOpOnValue OpAcos  v    = op_acos v
unaryOpOnValue OpAcosh v    = op_acosh v
unaryOpOnValue OpTan   v    = op_tan v
unaryOpOnValue OpTanh  v    = op_tanh v
unaryOpOnValue OpAtan  v    = op_atan v
unaryOpOnValue OpAtanh v    = op_atanh v
unaryOpOnValue OpAbs   v    = op_abs v
unaryOpOnValue OpCeil  v    = op_ceil v
unaryOpOnValue OpFloor v    = op_floor v
unaryOpOnValue OpRound v    = op_round v
unaryOpOnValue OpSign  v    = op_sign v
unaryOpOnValue OpSqrt  v    = op_sqrt v
unaryOpOnValue OpLog   v    = op_log v
unaryOpOnValue OpLog2  v    = op_log2 v
unaryOpOnValue OpLog10 v    = op_log10 v
unaryOpOnValue OpExp   v    = op_exp v
unaryOpOnValue OpIsInt v    = op_isInt v
unaryOpOnValue OpIsFloat v  = op_isFloat v
unaryOpOnValue OpIsNum v    = op_isNum v
unaryOpOnValue OpIsBool v   = op_isBool v
unaryOpOnValue OpIsStr v    = op_isStr v
unaryOpOnValue OpIsMap v    = op_isMap v
unaryOpOnValue OpIsArr v    = op_isArr v
unaryOpOnValue OpIsDate v   = op_isDate v
unaryOpOnValue OpIsTime v   = op_isTime v
unaryOpOnValue OpToStr v    = op_toStr v
unaryOpOnValue OpToLower v  = op_toLower v
unaryOpOnValue OpToUpper v  = op_toUpper v
unaryOpOnValue OpTrim v     = op_trim v
unaryOpOnValue OpLTrim v    = op_ltrim v
unaryOpOnValue OpRTrim v    = op_rtrim v
unaryOpOnValue OpReverse v  = op_reverse v
unaryOpOnValue OpStrLen v   = op_strlen v
unaryOpOnValue OpDistinct v = op_distinct v
unaryOpOnValue OpArrJoin v  = op_arrJoin v
unaryOpOnValue OpLength v   = op_length v
unaryOpOnValue OpArrMax v   = op_arrMax v
unaryOpOnValue OpArrMin v   = op_arrMin v
unaryOpOnValue OpSort v     = op_sort v

--------------------------------------------------------------------------------
op_sin :: FlowValue -> Either ERROR_TYPE FlowValue
op_sin (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (sin :: Double -> Double) (fromIntegral n))
op_sin (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ sin n)
op_sin (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (sin :: Double -> Double) n)
op_sin FlowNull        = Right FlowNull
op_sin v               = Left . ERR $ "Unsupported operator <sin> on value <" <> T.pack (show v) <> ">"

op_sinh :: FlowValue -> Either ERROR_TYPE FlowValue
op_sinh (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (sinh :: Double -> Double) (fromIntegral n))
op_sinh (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ sinh n)
op_sinh (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (sinh :: Double -> Double) n)
op_sinh FlowNull        = Right FlowNull
op_sinh v               = Left . ERR $ "Unsupported operator <sinh> on value <" <> T.pack (show v) <> ">"

op_asin :: FlowValue -> Either ERROR_TYPE FlowValue
op_asin (FlowInt n)
  | n >= (-1) && n <= 1 = Right $ FlowNumeral (fromFloatDigits $ (asin :: Double -> Double) (fromIntegral n))
  | otherwise = Left . ERR $ "Function <asin>: mathematical error"
op_asin (FlowFloat n)
  | n >= (-1) && n <= 1 = Right $ FlowNumeral (fromFloatDigits $ asin n)
  | otherwise = Left . ERR $ "Function <asin>: mathematical error"
op_asin (FlowNumeral n)
  | n >= (-1) && n <= 1 = Right $ FlowNumeral (funcOnScientific (asin :: Double -> Double) n)
  | otherwise = Left . ERR $ "Function <asin>: mathematical error"
op_asin FlowNull = Right FlowNull
op_asin v = Left . ERR $ "Unsupported operator <asin> on value <" <> T.pack (show v) <> ">"

op_asinh :: FlowValue -> Either ERROR_TYPE FlowValue
op_asinh (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (asinh :: Double -> Double) (fromIntegral n))
op_asinh (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ asinh n)
op_asinh (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (asinh :: Double -> Double) n)
op_asinh FlowNull        = Right FlowNull
op_asinh v = Left . ERR $ "Unsupported operator <asinh> on value <" <> T.pack (show v) <> ">"

op_cos :: FlowValue -> Either ERROR_TYPE FlowValue
op_cos (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (cos :: Double -> Double) (fromIntegral n))
op_cos (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ cos n)
op_cos (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (cos :: Double -> Double) n)
op_cos FlowNull        = Right FlowNull
op_cos v = Left . ERR $ "Unsupported operator <cos> on value <" <> T.pack (show v) <> ">"

op_cosh :: FlowValue -> Either ERROR_TYPE FlowValue
op_cosh (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (cosh :: Double -> Double) (fromIntegral n))
op_cosh (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ cosh n)
op_cosh (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (cosh :: Double -> Double) n)
op_cosh FlowNull        = Right FlowNull
op_cosh v = Left . ERR $ "Unsupported operator <cosh> on value <" <> T.pack (show v) <> ">"

op_acos :: FlowValue -> Either ERROR_TYPE FlowValue
op_acos (FlowInt n)
  | n >= (-1) && n <= 1 = Right $ FlowNumeral (fromFloatDigits $ (acos :: Double -> Double) (fromIntegral n))
  | otherwise = Left . ERR $ "Function <acos>: mathematical error"
op_acos (FlowFloat n)
  | n >= (-1) && n <= 1 = Right $ FlowNumeral (fromFloatDigits $ acos n)
  | otherwise = Left . ERR $ "Function <acos>: mathematical error"
op_acos (FlowNumeral n)
  | n >= (-1) && n <= 1 = Right $ FlowNumeral (funcOnScientific (acos :: Double -> Double) n)
  | otherwise = Left . ERR $ "Function <acos>: mathematical error"
op_acos FlowNull = Right FlowNull
op_acos v = Left . ERR $ "Unsupported operator <acos> on value <" <> T.pack (show v) <> ">"

op_acosh :: FlowValue -> Either ERROR_TYPE FlowValue
op_acosh (FlowInt n)
  | n >= 1 = Right $ FlowNumeral (fromFloatDigits $ (acosh :: Double -> Double) (fromIntegral n))
  | otherwise = Left . ERR $ "Function <acosh>: mathematical error"
op_acosh (FlowFloat n)
  | n >= 1 = Right $ FlowNumeral (fromFloatDigits $ acosh n)
  | otherwise = Left . ERR $ "Function <acosh>: mathematical error"
op_acosh (FlowNumeral n)
  | n >= 1 = Right $ FlowNumeral (funcOnScientific (acosh :: Double -> Double) n)
  | otherwise = Left . ERR $ "Function <acosh>: mathematical error"
op_acosh FlowNull = Right FlowNull
op_acosh v = Left . ERR $ "Unsupported operator <acosh> on value <" <> T.pack (show v) <> ">"

op_tan :: FlowValue -> Either ERROR_TYPE FlowValue
op_tan (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (tan :: Double -> Double) (fromIntegral n))
op_tan (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ tan n)
op_tan (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (tan :: Double -> Double) n)
op_tan FlowNull        = Right FlowNull
op_tan v = Left . ERR $ "Unsupported operator <tan> on value <" <> T.pack (show v) <> ">"

op_tanh :: FlowValue -> Either ERROR_TYPE FlowValue
op_tanh (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (tanh :: Double -> Double) (fromIntegral n))
op_tanh (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ tanh n)
op_tanh (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (tanh :: Double -> Double) n)
op_tanh FlowNull        = Right FlowNull
op_tanh v = Left . ERR $ "Unsupported operator <tanh> on value <" <> T.pack (show v) <> ">"

op_atan :: FlowValue -> Either ERROR_TYPE FlowValue
op_atan (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ (atan :: Double -> Double) (fromIntegral n))
op_atan (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ atan n)
op_atan (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (atan :: Double -> Double) n)
op_atan FlowNull        = Right FlowNull
op_atan v = Left . ERR $ "Unsupported operator <atan> on value <" <> T.pack (show v) <> ">"

op_atanh :: FlowValue -> Either ERROR_TYPE FlowValue
op_atanh (FlowInt n)
  | n > (-1) && n < 1 = Right $ FlowNumeral (fromFloatDigits $ (atanh :: Double -> Double) (fromIntegral n))
  | otherwise = Left . ERR $ "Function <atanh>: mathematical error"
op_atanh (FlowFloat n)
  | n > (-1) && n < 1 = Right $ FlowNumeral (fromFloatDigits $ atanh n)
  | otherwise = Left . ERR $ "Function <atanh>: mathematical error"
op_atanh (FlowNumeral n)
  | n > (-1) && n < 1 = Right $ FlowNumeral (funcOnScientific (atanh :: Double -> Double) n)
  | otherwise = Left . ERR $ "Function <atanh>: mathematical error"
op_atanh FlowNull = Right FlowNull
op_atanh v = Left . ERR $ "Unsupported operator <atanh> on value <" <> T.pack (show v) <> ">"

op_abs :: FlowValue -> Either ERROR_TYPE FlowValue
op_abs (FlowInt n)     = Right $ FlowInt (abs n)
op_abs (FlowFloat n)   = Right $ FlowFloat (abs n)
op_abs (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (abs :: Double -> Double) n)
op_abs FlowNull        = Right FlowNull
op_abs v = Left . ERR $ "Unsupported operator <abs> on value <" <> T.pack (show v) <> ">"

op_ceil :: FlowValue -> Either ERROR_TYPE FlowValue
op_ceil (FlowInt n)     = Right $ FlowInt n
op_ceil (FlowFloat n)   = Right $ FlowInt (ceiling n)
op_ceil (FlowNumeral n) = Right $ FlowInt (ceiling (toRealFloat n :: Double))
op_ceil FlowNull        = Right FlowNull
op_ceil v = Left . ERR $ "Unsupported operator <ceil> on value <" <> T.pack (show v) <> ">"

op_floor :: FlowValue -> Either ERROR_TYPE FlowValue
op_floor (FlowInt n)     = Right $ FlowInt n
op_floor (FlowFloat n)   = Right $ FlowInt (floor n)
op_floor (FlowNumeral n) = Right $ FlowInt (floor (toRealFloat n :: Double))
op_floor FlowNull        = Right FlowNull
op_floor v = Left . ERR $ "Unsupported operator <floor> on value <" <> T.pack (show v) <> ">"

op_round :: FlowValue -> Either ERROR_TYPE FlowValue
op_round (FlowInt n)     = Right $ FlowInt n
op_round (FlowFloat n)   = Right $ FlowInt (round n)
op_round (FlowNumeral n) = Right $ FlowInt (round (toRealFloat n :: Double))
op_round FlowNull        = Right FlowNull
op_round v = Left . ERR $ "Unsupported operator <round> on value <" <> T.pack (show v) <> ">"

op_sqrt :: FlowValue -> Either ERROR_TYPE FlowValue
op_sqrt (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ sqrt (fromIntegral n :: Double))
op_sqrt (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ sqrt n)
op_sqrt (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (sqrt :: Double -> Double) n)
op_sqrt FlowNull        = Right FlowNull
op_sqrt v = Left . ERR $ "Unsupported operator <sqrt> on value <" <> T.pack (show v) <> ">"

op_sign :: FlowValue -> Either ERROR_TYPE FlowValue
op_sign (FlowInt n)
  | n > 0  = Right $ FlowInt 1
  | n == 0 = Right $ FlowInt 0
  | n < 0  = Right $ FlowInt (-1)
op_sign (FlowFloat n)
  | n > 0  = Right $ FlowInt 1
  | n == 0 = Right $ FlowInt 0
  | n < 0  = Right $ FlowInt (-1)
op_sign (FlowNumeral n)
  | n > 0  = Right $ FlowInt 1
  | n == 0 = Right $ FlowInt 0
  | n < 0  = Right $ FlowInt (-1)
op_sign FlowNull = Right FlowNull
op_sign v = Left . ERR $ "Unsupported operator <sign> on value <" <> T.pack (show v) <> ">"

op_log :: FlowValue -> Either ERROR_TYPE FlowValue
op_log (FlowInt n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log (fromIntegral n :: Double))
  | otherwise = Left . ERR $ "Function <log>: mathematical error"
op_log (FlowFloat n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log n)
  | otherwise = Left . ERR $ "Function <log>: mathematical error"
op_log (FlowNumeral n)
  | n > 0 = Right $ FlowNumeral (funcOnScientific (log :: Double -> Double) n)
  | otherwise = Left . ERR $ "Function <log>: mathematical error"
op_log FlowNull = Right FlowNull
op_log v = Left . ERR $ "Unsupported operator <log> on value <" <> T.pack (show v) <> ">"

op_log2 :: FlowValue -> Either ERROR_TYPE FlowValue
op_log2 (FlowInt n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log (fromIntegral n :: Double) / log 2)
  | otherwise = Left . ERR $ "Function <log2>: mathematical error"
op_log2 (FlowFloat n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log n / log 2)
  | otherwise = Left . ERR $ "Function <log2>: mathematical error"
op_log2 (FlowNumeral n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log (toRealFloat n :: Double) / log 2)
  | otherwise = Left . ERR $ "Function <log2>: mathematical error"
op_log2 FlowNull = Right FlowNull
op_log2 v = Left . ERR $ "Unsupported operator <log2> on value <" <> T.pack (show v) <> ">"

op_log10 :: FlowValue -> Either ERROR_TYPE FlowValue
op_log10 (FlowInt n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log (fromIntegral n :: Double) / log 10)
  | otherwise = Left . ERR $ "Function <log10>: mathematical error"
op_log10 (FlowFloat n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log n / log 10)
  | otherwise = Left . ERR $ "Function <log10>: mathematical error"
op_log10 (FlowNumeral n)
  | n > 0 = Right $ FlowNumeral (fromFloatDigits $ log (toRealFloat n :: Double) / log 10)
  | otherwise = Left . ERR $ "Function <log10>: mathematical error"
op_log10 FlowNull = Right FlowNull
op_log10 v = Left . ERR $ "Unsupported operator <log10> on value <" <> T.pack (show v) <> ">"

op_exp :: FlowValue -> Either ERROR_TYPE FlowValue
op_exp (FlowInt n)     = Right $ FlowNumeral (fromFloatDigits $ exp (fromIntegral n :: Double))
op_exp (FlowFloat n)   = Right $ FlowNumeral (fromFloatDigits $ exp n)
op_exp (FlowNumeral n) = Right $ FlowNumeral (funcOnScientific (exp :: Double -> Double) n)
op_exp FlowNull        = Right FlowNull
op_exp v = Left . ERR $ "Unsupported operator <exp> on value <" <> T.pack (show v) <> ">"

op_isInt :: FlowValue -> Either ERROR_TYPE FlowValue
op_isInt (FlowInt _) = Right $ FlowBoolean True
op_isInt _           = Right $ FlowBoolean False

op_isFloat :: FlowValue -> Either ERROR_TYPE FlowValue
op_isFloat (FlowFloat _) = Right $ FlowBoolean True
op_isFloat _             = Right $ FlowBoolean False

op_isNum :: FlowValue -> Either ERROR_TYPE FlowValue
op_isNum (FlowNumeral _) = Right $ FlowBoolean True
op_isNum _               = Right $ FlowBoolean False

op_isBool :: FlowValue -> Either ERROR_TYPE FlowValue
op_isBool (FlowBoolean _) = Right $ FlowBoolean True
op_isBool _               = Right $ FlowBoolean False

op_isStr :: FlowValue -> Either ERROR_TYPE FlowValue
op_isStr (FlowText _) = Right $ FlowBoolean True
op_isStr _            = Right $ FlowBoolean False

op_isMap :: FlowValue -> Either ERROR_TYPE FlowValue
op_isMap (FlowMap _) = Right $ FlowBoolean True
op_isMap _           = Right $ FlowBoolean False

op_isArr :: FlowValue -> Either ERROR_TYPE FlowValue
op_isArr (FlowArray _) = Right $ FlowBoolean True
op_isArr _             = Right $ FlowBoolean False

op_isDate :: FlowValue -> Either ERROR_TYPE FlowValue
op_isDate (FlowDate _) = Right $ FlowBoolean True
op_isDate _            = Right $ FlowBoolean False

op_isTime :: FlowValue -> Either ERROR_TYPE FlowValue
op_isTime (FlowTime _) = Right $ FlowBoolean True
op_isTime _            = Right $ FlowBoolean False

op_toStr :: FlowValue -> Either ERROR_TYPE FlowValue
op_toStr v = Right $ FlowText (T.pack $ show v)

op_toLower :: FlowValue -> Either ERROR_TYPE FlowValue
op_toLower (FlowText t) = Right $ FlowText (T.toLower t)
op_toLower FlowNull     = Right FlowNull
op_toLower v = Left . ERR $ "Unsupported operator <toLower> on value <" <> T.pack (show v) <> ">"

op_toUpper :: FlowValue -> Either ERROR_TYPE FlowValue
op_toUpper (FlowText t) = Right $ FlowText (T.toUpper t)
op_toUpper FlowNull     = Right FlowNull
op_toUpper v = Left . ERR $ "Unsupported operator <toUpper> on value <" <> T.pack (show v) <> ">"

op_trim :: FlowValue -> Either ERROR_TYPE FlowValue
op_trim (FlowText t) = Right $ FlowText (T.strip t)
op_trim FlowNull     = Right FlowNull
op_trim v = Left . ERR $ "Unsupported operator <trim> on value <" <> T.pack (show v) <> ">"

op_ltrim :: FlowValue -> Either ERROR_TYPE FlowValue
op_ltrim (FlowText t) = Right $ FlowText (T.stripStart t)
op_ltrim FlowNull     = Right FlowNull
op_ltrim v = Left . ERR $ "Unsupported operator <ltrim> on value <" <> T.pack (show v) <> ">"

op_rtrim :: FlowValue -> Either ERROR_TYPE FlowValue
op_rtrim (FlowText t) = Right $ FlowText (T.stripEnd t)
op_rtrim FlowNull     = Right FlowNull
op_rtrim v = Left . ERR $ "Unsupported operator <rtrim> on value <" <> T.pack (show v) <> ">"

op_reverse :: FlowValue -> Either ERROR_TYPE FlowValue
op_reverse (FlowText t) = Right $ FlowText (T.reverse t)
op_reverse FlowNull     = Right FlowNull
op_reverse v = Left . ERR $ "Unsupported operator <reverse> on value <" <> T.pack (show v) <> ">"

op_strlen :: FlowValue -> Either ERROR_TYPE FlowValue
op_strlen (FlowText t) = Right $ FlowInt (T.length t)
op_strlen FlowNull     = Right FlowNull
op_strlen v = Left . ERR $ "Unsupported operator <strlen> on value <" <> T.pack (show v) <> ">"

op_distinct :: FlowValue -> Either ERROR_TYPE FlowValue
op_distinct (FlowArray arr) = Right $ FlowArray (L.nub arr)
op_distinct FlowNull        = Right FlowNull
op_distinct v = Left . ERR $ "Unsupported operator <distinct> on value <" <> T.pack (show v) <> ">"

op_length :: FlowValue -> Either ERROR_TYPE FlowValue
op_length (FlowArray arr) = Right $ FlowInt (L.length arr)
op_length FlowNull        = Right FlowNull
op_length v = Left . ERR $ "Unsupported operator <length> on value <" <> T.pack (show v) <> ">"

op_arrJoin :: FlowValue -> Either ERROR_TYPE FlowValue
op_arrJoin (FlowArray arr) = Right $ FlowText (arrJoinPrim arr Nothing)
op_arrJoin FlowNull        = Right FlowNull
op_arrJoin v = Left . ERR $ "Unsupported operator <arrJoin> on value <" <> T.pack (show v) <> ">"

op_arrMax :: FlowValue -> Either ERROR_TYPE FlowValue
op_arrMax (FlowArray arr)
  | L.null arr = Left . ERR $ "Function <arrMax>: empty array"
  | otherwise  = Right $ L.maximum arr
op_arrMax FlowNull = Right FlowNull
op_arrMax v = Left . ERR $ "Unsupported operator <arrMax> on value <" <> T.pack (show v) <> ">"

op_arrMin :: FlowValue -> Either ERROR_TYPE FlowValue
op_arrMin (FlowArray arr)
  | L.null arr = Left . ERR $ "Function <arrMin>: empty array"
  | otherwise  = Right $ L.minimum arr
op_arrMin FlowNull = Right FlowNull
op_arrMin v = Left . ERR $ "Unsupported operator <arrMin> on value <" <> T.pack (show v) <> ">"

op_sort :: FlowValue -> Either ERROR_TYPE FlowValue
op_sort (FlowArray arr) = Right $ FlowArray (L.sort arr)
op_sort FlowNull        = Right FlowNull
op_sort v = Left . ERR $ "Unsupported operator <sort> on value <" <> T.pack (show v) <> ">"
