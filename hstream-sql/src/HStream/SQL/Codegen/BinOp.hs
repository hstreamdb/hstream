{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.BinOp
  ( binOpOnValue
  ) where

import qualified Data.List                 as L
import           Data.Scientific
import qualified Data.Text                 as T
import           DiffFlow.Error
import           HStream.SQL.AST
import           HStream.SQL.Codegen.Utils

--------------------------------------------------------------------------------
binOpOnValue :: BinaryOp -> FlowValue -> FlowValue -> Either DiffFlowError FlowValue
binOpOnValue OpAdd v1 v2       = op_add v1 v2
binOpOnValue OpSub v1 v2       = op_sub v1 v2
binOpOnValue OpMul v1 v2       = op_mul v1 v2
binOpOnValue OpAnd v1 v2       = op_and v1 v2
binOpOnValue OpOr  v1 v2       = op_or  v1 v2
binOpOnValue OpEQ  v1 v2       = op_eq  v1 v2
binOpOnValue OpNEQ v1 v2       = op_neq v1 v2
binOpOnValue OpLT  v1 v2       = op_lt  v1 v2
binOpOnValue OpGT  v1 v2       = op_gt  v1 v2
binOpOnValue OpLEQ v1 v2       = op_leq v1 v2
binOpOnValue OpGEQ v1 v2       = op_geq v1 v2
binOpOnValue OpContain v1 v2   = op_contain v1 v2
binOpOnValue OpExcept v1 v2    = op_except v1 v2
binOpOnValue OpIntersect v1 v2 = op_intersect v1 v2
binOpOnValue OpRemove v1 v2    = op_remove v1 v2
binOpOnValue OpUnion v1 v2     = op_union v1 v2
binOpOnValue OpArrJoin' v1 v2  = op_arrJoin' v1 v2
binOpOnValue OpIfNull v1 v2    = op_ifNull v1 v2
binOpOnValue OpNullIf v1 v2    = op_nullIf v1 v2
binOpOnValue OpDateStr v1 v2   = op_dateStr v1 v2
binOpOnValue OpStrDate v1 v2   = op_strDate v1 v2
binOpOnValue OpSplit v1 v2     = op_split v1 v2
binOpOnValue OpChunksOf v1 v2  = op_chunksOf v1 v2
binOpOnValue OpTake v1 v2      = op_take v1 v2
binOpOnValue OpTakeEnd v1 v2   = op_takeEnd v1 v2
binOpOnValue OpDrop v1 v2      = op_drop v1 v2
binOpOnValue OpDropEnd v1 v2   = op_dropEnd v1 v2

--------------------------------------------------------------------------------
op_add :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_add (FlowInt n)     (FlowInt m)     = Right $ FlowInt (n+m)
op_add (FlowInt n)     (FlowFloat m)   = Right $ FlowFloat (fromIntegral n + m)
op_add (FlowInt n)     (FlowNumeral m) = Right $ FlowNumeral (fromIntegral n + m)
op_add (FlowFloat n)   (FlowFloat m)   = Right $ FlowFloat (n+m)
op_add (FlowFloat n)   (FlowInt m)     = Right $ FlowFloat (n + fromIntegral m)
op_add (FlowFloat n)   (FlowNumeral m) = Right $ FlowNumeral (fromFloatDigits n + m)
op_add (FlowNumeral n) (FlowNumeral m) = Right $ FlowNumeral (n+m)
op_add (FlowNumeral n) (FlowInt m)     = Right $ FlowNumeral (n + fromIntegral m)
op_add (FlowNumeral n) (FlowFloat m)   = Right $ FlowNumeral (n + fromFloatDigits m)
op_add FlowNull        _               = Right $ FlowNull
op_add _               FlowNull        = Right $ FlowNull
op_add v1 v2 = Left . RunShardError $ "Unsupported operator <add> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_sub :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_sub (FlowInt n)     (FlowInt m)     = Right $ FlowInt (n-m)
op_sub (FlowInt n)     (FlowFloat m)   = Right $ FlowFloat (fromIntegral n - m)
op_sub (FlowInt n)     (FlowNumeral m) = Right $ FlowNumeral (fromIntegral n - m)
op_sub (FlowFloat n)   (FlowFloat m)   = Right $ FlowFloat (n-m)
op_sub (FlowFloat n)   (FlowInt m)     = Right $ FlowFloat (n - fromIntegral m)
op_sub (FlowFloat n)   (FlowNumeral m) = Right $ FlowNumeral (fromFloatDigits n - m)
op_sub (FlowNumeral n) (FlowNumeral m) = Right $ FlowNumeral (n-m)
op_sub (FlowNumeral n) (FlowInt m)     = Right $ FlowNumeral (n - fromIntegral m)
op_sub (FlowNumeral n) (FlowFloat m)   = Right $ FlowNumeral (n - fromFloatDigits m)
op_sub FlowNull        _               = Right $ FlowNull
op_sub _               FlowNull        = Right $ FlowNull
op_sub v1 v2 = Left . RunShardError $ "Unsupported operator <sub> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_mul :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_mul (FlowInt n)     (FlowInt m)     = Right $ FlowInt (n*m)
op_mul (FlowInt n)     (FlowFloat m)   = Right $ FlowFloat (fromIntegral n * m)
op_mul (FlowInt n)     (FlowNumeral m) = Right $ FlowNumeral (fromIntegral n * m)
op_mul (FlowFloat n)   (FlowFloat m)   = Right $ FlowFloat (n*m)
op_mul (FlowFloat n)   (FlowInt m)     = Right $ FlowFloat (n * fromIntegral m)
op_mul (FlowFloat n)   (FlowNumeral m) = Right $ FlowNumeral (fromFloatDigits n * m)
op_mul (FlowNumeral n) (FlowNumeral m) = Right $ FlowNumeral (n*m)
op_mul (FlowNumeral n) (FlowInt m)     = Right $ FlowNumeral (n * fromIntegral m)
op_mul (FlowNumeral n) (FlowFloat m)   = Right $ FlowNumeral (n * fromFloatDigits m)
op_mul FlowNull        _               = Right $ FlowNull
op_mul _               FlowNull        = Right $ FlowNull
op_mul v1 v2 = Left . RunShardError $ "Unsupported operator <mul> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_and :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_and (FlowBoolean b1) (FlowBoolean b2) = Right $ FlowBoolean (b1 && b2)
op_and FlowNull        _                 = Right $ FlowNull
op_and _               FlowNull          = Right $ FlowNull
op_and v1 v2 = Left . RunShardError $ "Unsupported operator <and> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_or :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_or (FlowBoolean b1) (FlowBoolean b2) = Right $ FlowBoolean (b1 || b2)
op_or FlowNull        _                 = Right $ FlowNull
op_or _               FlowNull          = Right $ FlowNull
op_or v1 v2 = Left . RunShardError $ "Unsupported operator <or> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_eq :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_eq (FlowInt n)     (FlowInt m)           = Right $ FlowBoolean (n == m)
op_eq (FlowInt n)     (FlowFloat m)         = Right $ FlowBoolean (fromIntegral n == m)
op_eq (FlowInt n)     (FlowNumeral m)       = Right $ FlowBoolean (fromIntegral n == m)
op_eq (FlowFloat n)   (FlowFloat m)         = Right $ FlowBoolean (n == m)
op_eq (FlowFloat n)   (FlowInt m)           = Right $ FlowBoolean (n == fromIntegral m)
op_eq (FlowFloat n)   (FlowNumeral m)       = Right $ FlowBoolean (fromFloatDigits n == m)
op_eq (FlowNumeral n) (FlowNumeral m)       = Right $ FlowBoolean (n == m)
op_eq (FlowNumeral n) (FlowInt m)           = Right $ FlowBoolean (n == fromIntegral m)
op_eq (FlowNumeral n) (FlowFloat m)         = Right $ FlowBoolean (n == fromFloatDigits m)
op_eq (FlowBoolean b1) (FlowBoolean b2)     = Right $ FlowBoolean (b1 == b2)
op_eq (FlowByte bs1) (FlowByte bs2)         = Right $ FlowBoolean (bs1 == bs2)
op_eq (FlowText s1) (FlowText s2)           = Right $ FlowBoolean (s1 == s2)
op_eq (FlowDate d1) (FlowDate d2)           = Right $ FlowBoolean (d1 == d2)
op_eq (FlowTime t1) (FlowTime t2)           = Right $ FlowBoolean (t1 == t2)
op_eq (FlowTimestamp t1) (FlowTimestamp t2) = Right $ FlowBoolean (t1 == t2)
op_eq (FlowInterval i1) (FlowInterval i2)   = Right $ FlowBoolean (i1 == i2)
op_eq (FlowJson o1) (FlowJson o2)           = Right $ FlowBoolean (o1 == o2)
op_eq (FlowArray xs1) (FlowArray xs2)       = Right $ FlowBoolean (xs1 == xs2)
op_eq (FlowMap m1) (FlowMap m2)             = Right $ FlowBoolean (m1 == m2)
op_eq (FlowSubObject o1) (FlowSubObject o2) = Right $ FlowBoolean (o1 == o2)
op_eq FlowNull FlowNull                     = Right $ FlowBoolean True
op_eq FlowNull _                            = Right $ FlowBoolean False
op_eq _        FlowNull                     = Right $ FlowBoolean False
op_eq v1 v2 = Left . RunShardError $ "Unsupported operator <eq> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_neq :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_neq (FlowInt n)     (FlowInt m)           = Right $ FlowBoolean (n /= m)
op_neq (FlowInt n)     (FlowFloat m)         = Right $ FlowBoolean (fromIntegral n /= m)
op_neq (FlowInt n)     (FlowNumeral m)       = Right $ FlowBoolean (fromIntegral n /= m)
op_neq (FlowFloat n)   (FlowFloat m)         = Right $ FlowBoolean (n /= m)
op_neq (FlowFloat n)   (FlowInt m)           = Right $ FlowBoolean (n /= fromIntegral m)
op_neq (FlowFloat n)   (FlowNumeral m)       = Right $ FlowBoolean (fromFloatDigits n /= m)
op_neq (FlowNumeral n) (FlowNumeral m)       = Right $ FlowBoolean (n /= m)
op_neq (FlowNumeral n) (FlowInt m)           = Right $ FlowBoolean (n /= fromIntegral m)
op_neq (FlowNumeral n) (FlowFloat m)         = Right $ FlowBoolean (n /= fromFloatDigits m)
op_neq (FlowBoolean b1) (FlowBoolean b2)     = Right $ FlowBoolean (b1 /= b2)
op_neq (FlowByte bs1) (FlowByte bs2)         = Right $ FlowBoolean (bs1 /= bs2)
op_neq (FlowText s1) (FlowText s2)           = Right $ FlowBoolean (s1 /= s2)
op_neq (FlowDate d1) (FlowDate d2)           = Right $ FlowBoolean (d1 /= d2)
op_neq (FlowTime t1) (FlowTime t2)           = Right $ FlowBoolean (t1 /= t2)
op_neq (FlowTimestamp t1) (FlowTimestamp t2) = Right $ FlowBoolean (t1 /= t2)
op_neq (FlowInterval i1) (FlowInterval i2)   = Right $ FlowBoolean (i1 /= i2)
op_neq (FlowJson o1) (FlowJson o2)           = Right $ FlowBoolean (o1 /= o2)
op_neq (FlowArray xs1) (FlowArray xs2)       = Right $ FlowBoolean (xs1 /= xs2)
op_neq (FlowMap m1) (FlowMap m2)             = Right $ FlowBoolean (m1 /= m2)
op_neq (FlowSubObject o1) (FlowSubObject o2) = Right $ FlowBoolean (o1 /= o2)
op_neq FlowNull FlowNull                     = Right $ FlowBoolean False
op_neq FlowNull _                            = Right $ FlowBoolean True
op_neq _        FlowNull                     = Right $ FlowBoolean True
op_neq v1 v2 = Left . RunShardError $ "Unsupported operator <neq> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_lt :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_lt (FlowInt n)     (FlowInt m)           = Right $ FlowBoolean (n < m)
op_lt (FlowInt n)     (FlowFloat m)         = Right $ FlowBoolean (fromIntegral n < m)
op_lt (FlowInt n)     (FlowNumeral m)       = Right $ FlowBoolean (fromIntegral n < m)
op_lt (FlowFloat n)   (FlowFloat m)         = Right $ FlowBoolean (n < m)
op_lt (FlowFloat n)   (FlowInt m)           = Right $ FlowBoolean (n < fromIntegral m)
op_lt (FlowFloat n)   (FlowNumeral m)       = Right $ FlowBoolean (fromFloatDigits n < m)
op_lt (FlowNumeral n) (FlowNumeral m)       = Right $ FlowBoolean (n < m)
op_lt (FlowNumeral n) (FlowInt m)           = Right $ FlowBoolean (n < fromIntegral m)
op_lt (FlowNumeral n) (FlowFloat m)         = Right $ FlowBoolean (n < fromFloatDigits m)
op_lt (FlowBoolean b1) (FlowBoolean b2)     = Right $ FlowBoolean (b1 < b2)
op_lt (FlowByte bs1) (FlowByte bs2)         = Right $ FlowBoolean (bs1 < bs2)
op_lt (FlowText s1) (FlowText s2)           = Right $ FlowBoolean (s1 < s2)
op_lt (FlowDate d1) (FlowDate d2)           = Right $ FlowBoolean (d1 < d2)
op_lt (FlowTime t1) (FlowTime t2)           = Right $ FlowBoolean (t1 < t2)
op_lt (FlowTimestamp t1) (FlowTimestamp t2) = Right $ FlowBoolean (t1 < t2)
op_lt (FlowInterval i1) (FlowInterval i2)   = Right $ FlowBoolean (i1 < i2)
op_lt (FlowJson o1) (FlowJson o2)           = Right $ FlowBoolean (o1 < o2)
op_lt (FlowArray xs1) (FlowArray xs2)       = Right $ FlowBoolean (xs1 < xs2)
op_lt (FlowMap m1) (FlowMap m2)             = Right $ FlowBoolean (m1 < m2)
op_lt (FlowSubObject o1) (FlowSubObject o2) = Right $ FlowBoolean (o1 < o2)
op_lt FlowNull FlowNull                     = Right $ FlowBoolean False
op_lt FlowNull _                            = Right $ FlowNull
op_lt _        FlowNull                     = Right $ FlowNull
op_lt v1 v2 = Left . RunShardError $ "Unsupported operator <lt> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_gt :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_gt (FlowInt n)     (FlowInt m)           = Right $ FlowBoolean (n > m)
op_gt (FlowInt n)     (FlowFloat m)         = Right $ FlowBoolean (fromIntegral n > m)
op_gt (FlowInt n)     (FlowNumeral m)       = Right $ FlowBoolean (fromIntegral n > m)
op_gt (FlowFloat n)   (FlowFloat m)         = Right $ FlowBoolean (n > m)
op_gt (FlowFloat n)   (FlowInt m)           = Right $ FlowBoolean (n > fromIntegral m)
op_gt (FlowFloat n)   (FlowNumeral m)       = Right $ FlowBoolean (fromFloatDigits n > m)
op_gt (FlowNumeral n) (FlowNumeral m)       = Right $ FlowBoolean (n > m)
op_gt (FlowNumeral n) (FlowInt m)           = Right $ FlowBoolean (n > fromIntegral m)
op_gt (FlowNumeral n) (FlowFloat m)         = Right $ FlowBoolean (n > fromFloatDigits m)
op_gt (FlowBoolean b1) (FlowBoolean b2)     = Right $ FlowBoolean (b1 > b2)
op_gt (FlowByte bs1) (FlowByte bs2)         = Right $ FlowBoolean (bs1 > bs2)
op_gt (FlowText s1) (FlowText s2)           = Right $ FlowBoolean (s1 > s2)
op_gt (FlowDate d1) (FlowDate d2)           = Right $ FlowBoolean (d1 > d2)
op_gt (FlowTime t1) (FlowTime t2)           = Right $ FlowBoolean (t1 > t2)
op_gt (FlowTimestamp t1) (FlowTimestamp t2) = Right $ FlowBoolean (t1 > t2)
op_gt (FlowInterval i1) (FlowInterval i2)   = Right $ FlowBoolean (i1 > i2)
op_gt (FlowJson o1) (FlowJson o2)           = Right $ FlowBoolean (o1 > o2)
op_gt (FlowArray xs1) (FlowArray xs2)       = Right $ FlowBoolean (xs1 > xs2)
op_gt (FlowMap m1) (FlowMap m2)             = Right $ FlowBoolean (m1 > m2)
op_gt (FlowSubObject o1) (FlowSubObject o2) = Right $ FlowBoolean (o1 > o2)
op_gt FlowNull FlowNull                     = Right $ FlowBoolean False
op_gt FlowNull _                            = Right $ FlowNull
op_gt _        FlowNull                     = Right $ FlowNull
op_gt v1 v2 = Left . RunShardError $ "Unsupported operator <gt> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_leq :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_leq v1 v2 = do
  eq_result <- op_eq v1 v2
  lt_result <- op_lt v1 v2
  case eq_result of
    FlowBoolean result_eq ->
      case lt_result of
        FlowNull              -> Right FlowNull
        FlowBoolean result_lt -> Right $ FlowBoolean (result_eq || result_lt)
        _                     -> Left ImpossibleError
    _                     -> Left ImpossibleError

op_geq :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_geq v1 v2 = do
  eq_result <- op_eq v1 v2
  gt_result <- op_gt v1 v2
  case eq_result of
    FlowBoolean result_eq ->
      case gt_result of
        FlowNull              -> Right FlowNull
        FlowBoolean result_gt -> Right $ FlowBoolean (result_eq || result_gt)
        _                     -> Left ImpossibleError
    _                     -> Left ImpossibleError

op_contain :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_contain (FlowArray xs) v = Right $ FlowBoolean (v `L.elem` xs)
op_contain FlowNull _       = Right $ FlowNull
op_contain _ FlowNull       = Right $ FlowNull
op_contain v1 v2 = Left . RunShardError $ "Unsupported operator <contain> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_except :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_except (FlowArray xs) (FlowArray ys) = Right $ FlowArray (L.nub xs L.\\ ys)
op_except FlowNull _                    = Right $ FlowNull
op_except _ FlowNull                    = Right $ FlowNull
op_except v1 v2 = Left . RunShardError $ "Unsupported operator <except> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_intersect :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_intersect (FlowArray xs) (FlowArray ys) = Right $ FlowArray (L.nub $ xs `L.intersect` ys)
op_intersect FlowNull _ = Right FlowNull
op_intersect _ FlowNull = Right FlowNull
op_intersect v1 v2 = Left . RunShardError $ "Unsupported operator <intersect> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_remove :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_remove (FlowArray xs) v = Right $ FlowArray (L.filter (/= v) xs)
op_remove FlowNull _       = Right $ FlowNull
op_remove _ FlowNull       = Right $ FlowNull
op_remove v1 v2 = Left . RunShardError $ "Unsupported operator <remove> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_union :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_union (FlowArray xs) (FlowArray ys) = Right $ FlowArray (L.nub $ xs <> ys)
op_union FlowNull _                    = Right $ FlowNull
op_union _ FlowNull                    = Right $ FlowNull
op_union v1 v2 = Left . RunShardError $ "Unsupported operator <union> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_arrJoin' :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_arrJoin' (FlowArray xs) (FlowText str) = Right $ FlowText (arrJoinPrim xs (Just str))
op_arrJoin' FlowNull _                    = Right $ FlowNull
op_arrJoin' _ FlowNull                    = Right $ FlowNull
op_arrJoin' v1 v2 = Left . RunShardError $ "Unsupported operator <arrJoin> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_ifNull :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_ifNull FlowNull v = Right v
op_ifNull _        _ = Right FlowNull

op_nullIf :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_nullIf v1 v2 = do
  eq_result <- op_eq v1 v2
  case eq_result of
    FlowBoolean result_eq ->
      if result_eq     then
        Right FlowNull else
        Right v1
    _                     -> Left ImpossibleError

op_dateStr :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_dateStr (FlowNumeral n) (FlowText fmt) = Right $ FlowText (dateToStrGMT n fmt)
op_dateStr FlowNull _                     = Right $ FlowNull
op_dateStr _ FlowNull                     = Right $ FlowNull
op_dateStr v1 v2 = Left . RunShardError $ "Unsupported operator <dateStr> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_strDate :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_strDate (FlowText date) (FlowText fmt) = Right $ FlowNumeral (strToDateGMT date fmt)
op_strDate FlowNull _                     = Right $ FlowNull
op_strDate _ FlowNull                     = Right $ FlowNull
op_strDate v1 v2 = Left . RunShardError $ "Unsupported operator <strDate> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_split :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_split (FlowText x) (FlowText xs) = Right . FlowArray $ FlowText <$>
  (if T.length x == 1
    then T.split (== T.head x)
    else T.splitOn x) xs
op_split FlowNull _ = Right FlowNull
op_split _ FlowNull = Right FlowNull
op_split v1 v2 = Left . RunShardError $ "Unsupported operator <split> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_chunksOf :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_chunksOf (FlowInt n) (FlowText xs)     = Right . FlowArray $ FlowText <$> T.chunksOf n xs
op_chunksOf (FlowNumeral n) (FlowText xs) =
  case toBoundedInteger n of
    Just x  -> Right . FlowArray $ FlowText <$> T.chunksOf x xs
    Nothing -> Left . RunShardError $ "Operation <chunksOf> on chunks of size " <> T.pack (show n) <> " is not supported"
op_chunksOf FlowNull _ = Right FlowNull
op_chunksOf _ FlowNull = Right FlowNull
op_chunksOf v1 v2 = Left . RunShardError $ "Unsupported operator <chunksOf> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_take :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_take (FlowInt n) (FlowText xs)     = Right . FlowText $ T.take n xs
op_take (FlowNumeral n) (FlowText xs) =
  case toBoundedInteger n of
    Just x  -> Right . FlowText $ T.take x xs
    Nothing -> Left . RunShardError $ "Operation <take> on size " <> T.pack (show n) <> " is not supported"
op_take FlowNull _ = Right FlowNull
op_take _ FlowNull = Right FlowNull
op_take v1 v2 = Left . RunShardError $ "Unsupported operator <take> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_takeEnd :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_takeEnd (FlowInt n) (FlowText xs) = Right . FlowText $ T.take n xs
op_takeEnd (FlowNumeral n) (FlowText xs) =
  case toBoundedInteger n of
    Just x  -> Right . FlowText $ T.takeEnd x xs
    Nothing -> Left . RunShardError $ "Operation <takeEnd> on size " <> T.pack (show n) <> " is not supported"
op_takeEnd FlowNull _ = Right FlowNull
op_takeEnd _ FlowNull = Right FlowNull
op_takeEnd v1 v2 = Left . RunShardError $ "Unsupported operator <takeEnd> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_drop :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_drop (FlowInt n) (FlowText xs) = Right . FlowText $ T.take n xs
op_drop (FlowNumeral n) (FlowText xs) =
  case toBoundedInteger n of
    Just x  -> Right . FlowText $ T.drop x xs
    Nothing -> Left . RunShardError $ "Operation <drop> on size " <> T.pack (show n) <> " is not supported"
op_drop FlowNull _ = Right FlowNull
op_drop _ FlowNull = Right FlowNull
op_drop v1 v2 = Left . RunShardError $ "Unsupported operator <drop> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"

op_dropEnd :: FlowValue -> FlowValue -> Either DiffFlowError FlowValue
op_dropEnd (FlowInt n) (FlowText xs) = Right . FlowText $ T.take n xs
op_dropEnd (FlowNumeral n) (FlowText xs) =
  case toBoundedInteger n of
    Just x  -> Right . FlowText $ T.dropEnd x xs
    Nothing -> Left . RunShardError $ "Operation <dropEnd> on size " <> T.pack (show n) <> " is not supported"
op_dropEnd FlowNull _ = Right FlowNull
op_dropEnd _ FlowNull = Right FlowNull
op_dropEnd v1 v2 = Left . RunShardError $ "Unsupported operator <dropEnd> on value <" <> T.pack (show v1) <> "> and <" <> T.pack (show v2) <> ">"
