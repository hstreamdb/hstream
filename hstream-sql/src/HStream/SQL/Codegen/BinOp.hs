module HStream.SQL.Codegen.BinOp where

import qualified Data.List                 as L
import           Data.Scientific
import qualified Data.Text                 as T
import           GHC.Stack
import           HStream.SQL.AST
import           HStream.SQL.Codegen.Utils
import           HStream.SQL.Exception

--------------------------------------------------------------------------------
binOpOnValue :: HasCallStack => BinaryOp -> FlowValue -> FlowValue -> FlowValue
binOpOnValue OpAdd v1 v2 = op_add v1 v2
binOpOnValue OpSub v1 v2 = op_sub v1 v2
binOpOnValue OpMul v1 v2 = op_sub v1 v2
binOpOnValue OpAnd v1 v2 = op_and v1 v2
binOpOnValue OpOr  v1 v2 = op_or  v1 v2
binOpOnValue OpEQ  v1 v2 = op_eq  v1 v2
binOpOnValue OpNEQ v1 v2 = op_neq v1 v2
binOpOnValue OpLT  v1 v2 = op_lt  v1 v2
binOpOnValue OpGT  v1 v2 = op_gt  v1 v2
binOpOnValue OpLEQ v1 v2 = op_leq v1 v2
binOpOnValue OpGEQ v1 v2 = op_geq v1 v2
binOpOnValue OpContain v1 v2 = op_contain v1 v2
binOpOnValue OpExcept v1 v2 = op_except v1 v2
binOpOnValue OpIntersect v1 v2 = op_intersect v1 v2
binOpOnValue OpRemove v1 v2 = op_remove v1 v2
binOpOnValue OpUnion v1 v2 = op_union v1 v2
binOpOnValue OpArrJoin' v1 v2 = op_arrJoin' v1 v2
binOpOnValue OpIfNull v1 v2 = op_ifNull v1 v2
binOpOnValue OpNullIf v1 v2 = op_nullIf v1 v2
binOpOnValue OpDateStr v1 v2 = op_dateStr v1 v2
binOpOnValue OpStrDate v1 v2 = op_strDate v1 v2
binOpOnValue OpSplit v1 v2 = op_split v1 v2
binOpOnValue OpChunksOf v1 v2 = op_chunksOf v1 v2
binOpOnValue OpTake v1 v2 = op_take v1 v2
binOpOnValue OpTakeEnd v1 v2 = op_takeEnd v1 v2
binOpOnValue OpDrop v1 v2 = op_drop v1 v2
binOpOnValue OpDropEnd v1 v2 = op_dropEnd v1 v2
binOpOnValue op v1 v2 = throwRuntimeException $ "Unsupported operator <" <> show op <> "> on value <" <> show v1 <> "> and <" <> show v2 <> ">"

--------------------------------------------------------------------------------
op_add :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_add (FlowInt n)     (FlowInt m)     = FlowInt (n+m)
op_add (FlowInt n)     (FlowFloat m)   = FlowFloat (fromIntegral n + m)
op_add (FlowInt n)     (FlowNumeral m) = FlowNumeral (fromIntegral n + m)
op_add (FlowFloat n)   (FlowFloat m)   = FlowFloat (n+m)
op_add (FlowFloat n)   (FlowInt m)     = FlowFloat (n + fromIntegral m)
op_add (FlowFloat n)   (FlowNumeral m) = FlowNumeral (fromFloatDigits n + m)
op_add (FlowNumeral n) (FlowNumeral m) = FlowNumeral (n+m)
op_add (FlowNumeral n) (FlowInt m)     = FlowNumeral (n + fromIntegral m)
op_add (FlowNumeral n) (FlowFloat m)   = FlowNumeral (n + fromFloatDigits m)
op_add FlowNull        _               = FlowNull
op_add _               FlowNull        = FlowNull

op_sub :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_sub (FlowInt n)     (FlowInt m)     = FlowInt (n-m)
op_sub (FlowInt n)     (FlowFloat m)   = FlowFloat (fromIntegral n - m)
op_sub (FlowInt n)     (FlowNumeral m) = FlowNumeral (fromIntegral n - m)
op_sub (FlowFloat n)   (FlowFloat m)   = FlowFloat (n-m)
op_sub (FlowFloat n)   (FlowInt m)     = FlowFloat (n - fromIntegral m)
op_sub (FlowFloat n)   (FlowNumeral m) = FlowNumeral (fromFloatDigits n - m)
op_sub (FlowNumeral n) (FlowNumeral m) = FlowNumeral (n-m)
op_sub (FlowNumeral n) (FlowInt m)     = FlowNumeral (n - fromIntegral m)
op_sub (FlowNumeral n) (FlowFloat m)   = FlowNumeral (n - fromFloatDigits m)
op_sub FlowNull        _               = FlowNull
op_sub _               FlowNull        = FlowNull

op_mul :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_mul (FlowInt n)     (FlowInt m)     = FlowInt (n*m)
op_mul (FlowInt n)     (FlowFloat m)   = FlowFloat (fromIntegral n * m)
op_mul (FlowInt n)     (FlowNumeral m) = FlowNumeral (fromIntegral n * m)
op_mul (FlowFloat n)   (FlowFloat m)   = FlowFloat (n*m)
op_mul (FlowFloat n)   (FlowInt m)     = FlowFloat (n * fromIntegral m)
op_mul (FlowFloat n)   (FlowNumeral m) = FlowNumeral (fromFloatDigits n * m)
op_mul (FlowNumeral n) (FlowNumeral m) = FlowNumeral (n*m)
op_mul (FlowNumeral n) (FlowInt m)     = FlowNumeral (n * fromIntegral m)
op_mul (FlowNumeral n) (FlowFloat m)   = FlowNumeral (n * fromFloatDigits m)
op_mul FlowNull        _               = FlowNull
op_mul _               FlowNull        = FlowNull

op_and :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_and (FlowBoolean b1) (FlowBoolean b2) = FlowBoolean (b1 && b2)
op_and FlowNull        _                 = FlowNull
op_and _               FlowNull          = FlowNull

op_or :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_or (FlowBoolean b1) (FlowBoolean b2) = FlowBoolean (b1 || b2)
op_or FlowNull        _                 = FlowNull
op_or _               FlowNull          = FlowNull

op_eq :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_eq (FlowInt n)     (FlowInt m)     = FlowBoolean (n == m)
op_eq (FlowInt n)     (FlowFloat m)   = FlowBoolean (fromIntegral n == m)
op_eq (FlowInt n)     (FlowNumeral m) = FlowBoolean (fromIntegral n == m)
op_eq (FlowFloat n)   (FlowFloat m)   = FlowBoolean (n == m)
op_eq (FlowFloat n)   (FlowInt m)     = FlowBoolean (n == fromIntegral m)
op_eq (FlowFloat n)   (FlowNumeral m) = FlowBoolean (fromFloatDigits n == m)
op_eq (FlowNumeral n) (FlowNumeral m) = FlowBoolean (n == m)
op_eq (FlowNumeral n) (FlowInt m)     = FlowBoolean (n == fromIntegral m)
op_eq (FlowNumeral n) (FlowFloat m)   = FlowBoolean (n == fromFloatDigits m)
op_eq (FlowBoolean b1) (FlowBoolean b2) = FlowBoolean (b1 == b2)
op_eq (FlowByte bs1) (FlowByte bs2) = FlowBoolean (bs1 == bs2)
op_eq (FlowText s1) (FlowText s2) = FlowBoolean (s1 == s2)
op_eq (FlowDate d1) (FlowDate d2) = FlowBoolean (d1 == d2)
op_eq (FlowTime t1) (FlowTime t2) = FlowBoolean (t1 == t2)
op_eq (FlowTimestamp t1) (FlowTimestamp t2) = FlowBoolean (t1 == t2)
op_eq (FlowInterval i1) (FlowInterval i2) = FlowBoolean (i1 == i2)
op_eq (FlowJson o1) (FlowJson o2) = FlowBoolean (o1 == o2)
op_eq (FlowArray xs1) (FlowArray xs2) = FlowBoolean (xs1 == xs2)
op_eq (FlowMap m1) (FlowMap m2) = FlowBoolean (m1 == m2)
op_eq (FlowSubObject o1) (FlowSubObject o2) = FlowBoolean (o1 == o2)
op_eq FlowNull FlowNull = FlowBoolean True
op_eq FlowNull _        = FlowBoolean False
op_eq _        FlowNull = FlowBoolean False

op_neq :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_neq (FlowInt n)     (FlowInt m)     = FlowBoolean (n /= m)
op_neq (FlowInt n)     (FlowFloat m)   = FlowBoolean (fromIntegral n /= m)
op_neq (FlowInt n)     (FlowNumeral m) = FlowBoolean (fromIntegral n /= m)
op_neq (FlowFloat n)   (FlowFloat m)   = FlowBoolean (n /= m)
op_neq (FlowFloat n)   (FlowInt m)     = FlowBoolean (n /= fromIntegral m)
op_neq (FlowFloat n)   (FlowNumeral m) = FlowBoolean (fromFloatDigits n /= m)
op_neq (FlowNumeral n) (FlowNumeral m) = FlowBoolean (n /= m)
op_neq (FlowNumeral n) (FlowInt m)     = FlowBoolean (n /= fromIntegral m)
op_neq (FlowNumeral n) (FlowFloat m)   = FlowBoolean (n /= fromFloatDigits m)
op_neq (FlowBoolean b1) (FlowBoolean b2) = FlowBoolean (b1 /= b2)
op_neq (FlowByte bs1) (FlowByte bs2) = FlowBoolean (bs1 /= bs2)
op_neq (FlowText s1) (FlowText s2) = FlowBoolean (s1 /= s2)
op_neq (FlowDate d1) (FlowDate d2) = FlowBoolean (d1 /= d2)
op_neq (FlowTime t1) (FlowTime t2) = FlowBoolean (t1 /= t2)
op_neq (FlowTimestamp t1) (FlowTimestamp t2) = FlowBoolean (t1 /= t2)
op_neq (FlowInterval i1) (FlowInterval i2) = FlowBoolean (i1 /= i2)
op_neq (FlowJson o1) (FlowJson o2) = FlowBoolean (o1 /= o2)
op_neq (FlowArray xs1) (FlowArray xs2) = FlowBoolean (xs1 /= xs2)
op_neq (FlowMap m1) (FlowMap m2) = FlowBoolean (m1 /= m2)
op_neq (FlowSubObject o1) (FlowSubObject o2) = FlowBoolean (o1 /= o2)
op_neq FlowNull FlowNull = FlowBoolean False
op_neq FlowNull _        = FlowBoolean True
op_neq _        FlowNull = FlowBoolean True

op_lt :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_lt (FlowInt n)     (FlowInt m)     = FlowBoolean (n < m)
op_lt (FlowInt n)     (FlowFloat m)   = FlowBoolean (fromIntegral n < m)
op_lt (FlowInt n)     (FlowNumeral m) = FlowBoolean (fromIntegral n < m)
op_lt (FlowFloat n)   (FlowFloat m)   = FlowBoolean (n < m)
op_lt (FlowFloat n)   (FlowInt m)     = FlowBoolean (n < fromIntegral m)
op_lt (FlowFloat n)   (FlowNumeral m) = FlowBoolean (fromFloatDigits n < m)
op_lt (FlowNumeral n) (FlowNumeral m) = FlowBoolean (n < m)
op_lt (FlowNumeral n) (FlowInt m)     = FlowBoolean (n < fromIntegral m)
op_lt (FlowNumeral n) (FlowFloat m)   = FlowBoolean (n < fromFloatDigits m)
op_lt (FlowBoolean b1) (FlowBoolean b2) = FlowBoolean (b1 < b2)
op_lt (FlowByte bs1) (FlowByte bs2) = FlowBoolean (bs1 < bs2)
op_lt (FlowText s1) (FlowText s2) = FlowBoolean (s1 < s2)
op_lt (FlowDate d1) (FlowDate d2) = FlowBoolean (d1 < d2)
op_lt (FlowTime t1) (FlowTime t2) = FlowBoolean (t1 < t2)
op_lt (FlowTimestamp t1) (FlowTimestamp t2) = FlowBoolean (t1 < t2)
op_lt (FlowInterval i1) (FlowInterval i2) = FlowBoolean (i1 < i2)
op_lt (FlowJson o1) (FlowJson o2) = FlowBoolean (o1 < o2)
op_lt (FlowArray xs1) (FlowArray xs2) = FlowBoolean (xs1 < xs2)
op_lt (FlowMap m1) (FlowMap m2) = FlowBoolean (m1 < m2)
op_lt (FlowSubObject o1) (FlowSubObject o2) = FlowBoolean (o1 < o2)
op_lt FlowNull FlowNull = FlowBoolean False
op_lt FlowNull _        = FlowNull
op_lt _        FlowNull = FlowNull

op_gt :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_gt (FlowInt n)     (FlowInt m)     = FlowBoolean (n > m)
op_gt (FlowInt n)     (FlowFloat m)   = FlowBoolean (fromIntegral n > m)
op_gt (FlowInt n)     (FlowNumeral m) = FlowBoolean (fromIntegral n > m)
op_gt (FlowFloat n)   (FlowFloat m)   = FlowBoolean (n > m)
op_gt (FlowFloat n)   (FlowInt m)     = FlowBoolean (n > fromIntegral m)
op_gt (FlowFloat n)   (FlowNumeral m) = FlowBoolean (fromFloatDigits n > m)
op_gt (FlowNumeral n) (FlowNumeral m) = FlowBoolean (n > m)
op_gt (FlowNumeral n) (FlowInt m)     = FlowBoolean (n > fromIntegral m)
op_gt (FlowNumeral n) (FlowFloat m)   = FlowBoolean (n > fromFloatDigits m)
op_gt (FlowBoolean b1) (FlowBoolean b2) = FlowBoolean (b1 > b2)
op_gt (FlowByte bs1) (FlowByte bs2) = FlowBoolean (bs1 > bs2)
op_gt (FlowText s1) (FlowText s2) = FlowBoolean (s1 > s2)
op_gt (FlowDate d1) (FlowDate d2) = FlowBoolean (d1 > d2)
op_gt (FlowTime t1) (FlowTime t2) = FlowBoolean (t1 > t2)
op_gt (FlowTimestamp t1) (FlowTimestamp t2) = FlowBoolean (t1 > t2)
op_gt (FlowInterval i1) (FlowInterval i2) = FlowBoolean (i1 > i2)
op_gt (FlowJson o1) (FlowJson o2) = FlowBoolean (o1 > o2)
op_gt (FlowArray xs1) (FlowArray xs2) = FlowBoolean (xs1 > xs2)
op_gt (FlowMap m1) (FlowMap m2) = FlowBoolean (m1 > m2)
op_gt (FlowSubObject o1) (FlowSubObject o2) = FlowBoolean (o1 > o2)
op_gt FlowNull FlowNull = FlowBoolean False
op_gt FlowNull _        = FlowNull
op_gt _        FlowNull = FlowNull

op_leq :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_leq v1 v2 =
  let (FlowBoolean result_eq) = op_eq v1 v2
   in case op_lt v1 v2 of
        FlowNull              -> FlowNull
        FlowBoolean result_lt -> FlowBoolean (result_eq || result_lt)

op_geq :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_geq v1 v2 =
  let (FlowBoolean result_eq) = op_eq v1 v2
   in case op_gt v1 v2 of
        FlowNull              -> FlowNull
        FlowBoolean result_gt -> FlowBoolean (result_eq || result_gt)

op_contain :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_contain (FlowArray xs) v = FlowBoolean (v `L.elem` xs)
op_contain FlowNull _       = FlowNull
op_contain _ FlowNull       = FlowNull

op_except :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_except (FlowArray xs) (FlowArray ys) = FlowArray (L.nub xs L.\\ ys)
op_except FlowNull _                    = FlowNull
op_except _ FlowNull                    = FlowNull

op_intersect :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_intersect (FlowArray xs) (FlowArray ys) = FlowArray (L.nub $ xs `L.intersect` ys)
op_intersect FlowNull _ = FlowNull
op_intersect _ FlowNull = FlowNull

op_remove :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_remove (FlowArray xs) v = FlowArray (L.filter (/= v) xs)
op_remove FlowNull _       = FlowNull
op_remove _ FlowNull       = FlowNull

op_union :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_union (FlowArray xs) (FlowArray ys) = FlowArray (L.nub $ xs <> ys)
op_union FlowNull _                    = FlowNull
op_union _ FlowNull                    = FlowNull

op_arrJoin' :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_arrJoin' (FlowArray xs) (FlowText str) = FlowText (arrJoinPrim xs (Just str))
op_arrJoin' FlowNull _                    = FlowNull
op_arrJoin' _ FlowNull                    = FlowNull

op_ifNull :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_ifNull FlowNull v = v
op_ifNull _        v = v

op_nullIf :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_nullIf v1 v2 =
  let (FlowBoolean result_eq) = op_eq v1 v2
   in if result_eq then FlowNull else v1

op_dateStr :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_dateStr (FlowNumeral n) (FlowText fmt) = FlowText (dateToStrGMT n fmt)
op_dateStr FlowNull _                     = FlowNull
op_dateStr _ FlowNull                     = FlowNull

op_strDate :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_strDate (FlowText date) (FlowText fmt) = FlowNumeral (strToDateGMT date fmt)
op_strDate FlowNull _                     = FlowNull
op_strDate _ FlowNull                     = FlowNull

op_split :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_split (FlowText x) (FlowText xs) = FlowArray $ FlowText <$>
  (if T.length x == 1
    then T.split (== T.head x)
    else T.splitOn x) xs
op_split FlowNull _ = FlowNull
op_split _ FlowNull = FlowNull

op_chunksOf :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_chunksOf (FlowInt n) (FlowText xs) = FlowArray $ FlowText <$> T.chunksOf n xs
op_chunksOf (FlowNumeral n) (FlowText xs) = FlowArray $ FlowText <$>
  T.chunksOf (case toBoundedInteger n of
    Just x -> x
    _      -> throwRuntimeException
      ("Operation OpChunksOf on chunks of size " ++ show n ++ " is not supported")) xs
op_chunksOf FlowNull _ = FlowNull
op_chunksOf _ FlowNull = FlowNull

op_take :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_take (FlowInt n) (FlowText xs) = FlowText $ T.take n xs
op_take (FlowNumeral n) (FlowText xs) = FlowText $
  T.take (case toBoundedInteger n of
    Just x -> x
    _      -> throwRuntimeException
      ("Operation OpTake on size " ++ show n ++ " is not supported")) xs
op_take FlowNull _ = FlowNull
op_take _ FlowNull = FlowNull

op_takeEnd :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_takeEnd (FlowInt n) (FlowText xs) = FlowText $ T.take n xs
op_takeEnd (FlowNumeral n) (FlowText xs) = FlowText $
  T.takeEnd (case toBoundedInteger n of
    Just x -> x
    _      -> throwRuntimeException
      ("Operation OpTakeEnd on size " ++ show n ++ " is not supported")) xs
op_takeEnd FlowNull _ = FlowNull
op_takeEnd _ FlowNull = FlowNull

op_drop :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_drop (FlowInt n) (FlowText xs) = FlowText $ T.take n xs
op_drop (FlowNumeral n) (FlowText xs) = FlowText $
  T.drop (case toBoundedInteger n of
    Just x -> x
    _      -> throwRuntimeException
      ("Operation OpDrop on size " ++ show n ++ " is not supported")) xs
op_drop FlowNull _ = FlowNull
op_drop _ FlowNull = FlowNull

op_dropEnd :: HasCallStack => FlowValue -> FlowValue -> FlowValue
op_dropEnd (FlowInt n) (FlowText xs) = FlowText $ T.take n xs
op_dropEnd (FlowNumeral n) (FlowText xs) = FlowText $
  T.dropEnd (case toBoundedInteger n of
    Just x -> x
    _      -> throwRuntimeException
      ("Operation OpDropEnd on size " ++ show n ++ " is not supported")) xs
op_dropEnd FlowNull _ = FlowNull
op_dropEnd _ FlowNull = FlowNull
