{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Internal.Codegen where

import           Control.Exception     (throw)
import           Data.Aeson
import qualified Data.HashMap.Strict   as HM
import qualified Data.List             as L
import           Data.Scientific
import qualified Data.Text             as T
import           Data.Time             (DiffTime, diffTimeToPicoseconds)
import qualified Data.Vector           as V
import           GHC.Stack             (callStack)
import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeRuntimeException (..),
                                        SomeSQLException (..),
                                        throwSQLException)
import           HStream.Utils
import qualified Prelude               as Prelude
import           RIO
import           Text.StringRandom     (stringRandomIO)
import qualified Z.Data.CBytes         as ZCB
import qualified Z.Data.Text           as ZT
import           Z.IO.Time

import           DiffFlow.Graph        (Joiner (..))

constantKeygen :: FlowObject -> FlowObject
constantKeygen _ =
  HM.fromList [(SKey "key" Nothing Nothing, FlowText "__constant_key__")]

makeExtra :: Text -> FlowObject -> FlowObject
makeExtra extra =
  HM.mapKeys (\(SKey f s_m _) -> SKey f s_m (Just extra))

getExtra :: Text -> FlowObject -> FlowObject
getExtra extra =
  HM.filterWithKey (\(SKey f s_m extra_m) v -> extra_m == Just extra)

getExtraAndReset :: Text -> FlowObject -> FlowObject
getExtraAndReset extra o =
  HM.mapKeys (\(SKey f s_m _) -> SKey f s_m Nothing) $
  HM.filterWithKey (\(SKey f s_m extra_m) v -> extra_m == Just extra) o

getField :: HasCallStack => FlowObject -> Text -> (SKey, FlowValue)
getField o k =
  case HM.toList (HM.filterWithKey (\(SKey f _ _) _ -> f == k) o) of
    [] -> throw
      SomeRuntimeException
      { runtimeExceptionMessage = "Key " <> show k <> " is not found in object " <> show o <> ": " <> show callStack
      , runtimeExceptionCallStack = callStack
      }
    [(skey,v)] -> (skey,v)
    xs -> throw
      SomeRuntimeException
      { runtimeExceptionMessage = "!!! Same field name with different <stream> and/or <extra>: " <> show xs <> ": " <> show callStack
      , runtimeExceptionCallStack = callStack
      }

--------------------------------------------------------------------------------
genRandomSinkStream :: IO Text
genRandomSinkStream = stringRandomIO "[a-zA-Z]{20}"

--------------------------------------------------------------------------------
compareValue :: HasCallStack => Value -> Value -> Ordering
compareValue (Number x1) (Number x2) = x1 `compare` x2
compareValue (String x1) (String x2) = x1 `compare` x2
compareValue _ _                     =
  throwSQLException CodegenException Nothing "Value does not support comparison"

binOpOnValue :: HasCallStack => BinaryOp -> FlowValue -> FlowValue -> FlowValue
binOpOnValue OpAdd (FlowInt n) (FlowInt m) = FlowInt (n+m)
binOpOnValue OpAdd (FlowFloat n) (FlowFloat m) = FlowFloat (n+m)
binOpOnValue OpAdd (FlowNumeral n) (FlowNumeral m) = FlowNumeral (n+m)
binOpOnValue OpAdd (FlowNumeral n) (FlowInt m) = FlowNumeral (n + (fromIntegral m))
{-
binOpOnValue OpSub (Number n) (Number m) = Number (n-m)
binOpOnValue OpMul (Number n) (Number m) = Number (n*m)
binOpOnValue OpAnd (Bool b1)  (Bool b2)  = Bool (b1 && b2)
binOpOnValue OpOr  (Bool b1)  (Bool b2)  = Bool (b1 || b2)
binOpOnValue OpContain (Array xs) x      = Bool (x `V.elem` xs)
binOpOnValue OpExcept    Null       _          = Null
binOpOnValue OpExcept    _          Null       = Null
binOpOnValue OpExcept    (Array xs) (Array ys) = Array  (nub xs \\ ys)
binOpOnValue OpIntersect Null       _          = Null
binOpOnValue OpIntersect _          Null       = Null
binOpOnValue OpIntersect (Array xs) (Array ys) = Array  (nub $ xs `intersect`  ys)
binOpOnValue OpRemove    Null       _          = Null
binOpOnValue OpRemove    (Array xs) x          = Array  (V.filter (/= x) xs)
binOpOnValue OpUnion     Null       _          = Null
binOpOnValue OpUnion     _          Null       = Null
binOpOnValue OpUnion     (Array xs) (Array ys) = Array  (nub $ xs <> ys)
binOpOnValue OpArrJoin'  (Array xs) (String s) = String (arrJoinPrim xs (Just s))
binOpOnValue OpIfNull Null x = x
binOpOnValue OpIfNull x    _ = x
binOpOnValue OpNullIf x y = if x == y then Null else x
binOpOnValue OpDateStr (Number date) (String fmt) = String $ dateToStrGMT date fmt
binOpOnValue OpStrDate (String date) (String fmt) = Number $ strToDateGMT date fmt
binOpOnValue OpSplit Null _    = Null
binOpOnValue OpSplit _    Null = Null
binOpOnValue OpSplit (String x) (String xs) = Array . V.fromList $ String <$>
  (if T.length x == 1
    then T.split (== T.head x)
    else T.splitOn x) xs
binOpOnValue OpChunksOf _ Null = Null
binOpOnValue OpChunksOf (Number n) (String xs) = Array . V.fromList $ String <$>
  T.chunksOf (case toBoundedInteger n of
    Just x -> x
    _ -> throwSQLException CodegenException Nothing
      ("Operation OpChunksOf on chunks of size " ++ show n ++ " is not supported")) xs
binOpOnValue OpTake _ Null = Null
binOpOnValue OpTake (Number n) (String xs) = String $
  T.take (case toBoundedInteger n of
    Just x -> x
    _ -> throwSQLException CodegenException Nothing
      ("Operation OpTake on size " ++ show n ++ " is not supported")) xs
binOpOnValue OpTakeEnd _ Null = Null
binOpOnValue OpTakeEnd (Number n) (String xs) = String $
  T.takeEnd (case toBoundedInteger n of
    Just x -> x
    _ -> throwSQLException CodegenException Nothing
      ("Operation OpTakeEnd on size " ++ show n ++ " is not supported")) xs
binOpOnValue OpDrop _ Null = Null
binOpOnValue OpDrop (Number n) (String xs) = String $
  T.drop (case toBoundedInteger n of
    Just x -> x
    _ -> throwSQLException CodegenException Nothing
      ("Operation OpDrop on size " ++ show n ++ " is not supported")) xs
binOpOnValue OpDropEnd _ Null = Null
binOpOnValue OpDropEnd (Number n) (String xs) = String $
  T.dropEnd (case toBoundedInteger n of
    Just x -> x
    _ -> throwSQLException CodegenException Nothing
      ("Operation OpDropEnd on size " ++ show n ++ " is not supported")) xs
-}
binOpOnValue op v1 v2 =
  throwSQLException CodegenException Nothing ("Operation " <> show op <> " on " <> show v1 <> " and " <> show v2 <> " is not supported")

unaryOpOnValue :: HasCallStack => UnaryOp -> FlowValue -> FlowValue
unaryOpOnValue OpSin (FlowInt n) = FlowNumeral (fromFloatDigits $ sin (fromIntegral n))
unaryOpOnValue OpSin (FlowFloat n) = FlowNumeral (fromFloatDigits $ sin n)
unaryOpOnValue OpSin (FlowNumeral n) = FlowNumeral (funcOnScientific sin n)
{-
unaryOpOnValue OpSin     (Number n)  = Number (funcOnScientific sin n)
unaryOpOnValue OpSinh    (Number n) = Number (funcOnScientific sinh n)
unaryOpOnValue OpAsin    (Number n)
  | n >= (-1) && n <= 1             = Number (funcOnScientific asin n)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpAsinh   (Number n) = Number (funcOnScientific asinh n)
unaryOpOnValue OpCos     (Number n) = Number (funcOnScientific cos n)
unaryOpOnValue OpCosh    (Number n) = Number (funcOnScientific cosh n)
unaryOpOnValue OpAcos    (Number n)
  | n >= (-1) && n <= 1             = Number (funcOnScientific acos n)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpAcosh   (Number n)
  | n >= 1                          = Number (funcOnScientific acosh n)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpTan     (Number n) = Number (funcOnScientific tan n)
unaryOpOnValue OpTanh    (Number n) = Number (funcOnScientific tanh n)
unaryOpOnValue OpAtan    (Number n) = Number (funcOnScientific atan n)
unaryOpOnValue OpAtanh   (Number n)
  | n > (-1) && n < 1               = Number (funcOnScientific atanh n)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpAbs     (Number n) = Number (abs n)
unaryOpOnValue OpCeil    (Number n) = Number (scientific (ceiling n) 0)
unaryOpOnValue OpFloor   (Number n) = Number (scientific (floor n) 0)
unaryOpOnValue OpRound   (Number n) = Number (scientific (round n) 0)
unaryOpOnValue OpSqrt    (Number n) = Number (funcOnScientific sqrt n)
unaryOpOnValue OpSign    (Number n)
  | n >  0                          = Number (1)
  | n == 0                          = Number (0)
  | n < 0                           = Number (-1)
unaryOpOnValue OpSign    Null       = Null
unaryOpOnValue OpLog     (Number n)
  | n > 0                           = Number (funcOnScientific log n)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpLog2    (Number n)
  | n > 0                           = Number (fromFloatDigits $ log (toRealFloat n) / log 2)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpLog10   (Number n)
  | n > 0                           = Number (fromFloatDigits $ log (toRealFloat n) / log 10)
  | otherwise                       = throwSQLException CodegenException Nothing "Mathematical error"
unaryOpOnValue OpExp     (Number n) = Number (funcOnScientific exp n)
unaryOpOnValue OpIsInt   (Number _) = Bool True
unaryOpOnValue OpIsInt   _          = Bool False
unaryOpOnValue OpIsFloat (Number _) = Bool True
unaryOpOnValue OpIsFloat _          = Bool False
unaryOpOnValue OpIsNum   (Number _) = Bool True
unaryOpOnValue OpIsNum   _          = Bool False
unaryOpOnValue OpIsBool  (Bool _)   = Bool True
unaryOpOnValue OpIsBool  _          = Bool False
unaryOpOnValue OpIsStr   (String _) = Bool True
unaryOpOnValue OpIsStr   _          = Bool False
unaryOpOnValue OpIsMap   (Object _) = Bool True
unaryOpOnValue OpIsMap   _          = Bool False
unaryOpOnValue OpIsArr   (Array _)  = Bool True
unaryOpOnValue OpIsArr   _          = Bool False
unaryOpOnValue OpIsDate  _          = throwSQLException CodegenException Nothing ("Operation " <> show OpIsDate <> "is not supported")
unaryOpOnValue OpIsTime  _          = throwSQLException CodegenException Nothing ("Operation " <> show OpIsTime <> " is not supported")
unaryOpOnValue OpToStr   v          = String (T.pack $ show v)
unaryOpOnValue OpToLower (String s) = String (T.toLower s)
unaryOpOnValue OpToUpper (String s) = String (T.toUpper s)
unaryOpOnValue OpTrim    (String s) = String (T.strip s)
unaryOpOnValue OpLTrim   (String s) = String (T.stripStart s)
unaryOpOnValue OpRTrim   (String s) = String (T.stripEnd s)
unaryOpOnValue OpReverse (String s) = String (T.reverse s)
unaryOpOnValue OpStrLen  (String s) = Number (scientific (toInteger (T.length s)) 0)
unaryOpOnValue OpDistinct(Array xs) = Array  (nub xs)
unaryOpOnValue OpDistinct Null      = Null
unaryOpOnValue OpLength   Null      = Null
unaryOpOnValue OpLength  (Array xs) = Number (scientific (toInteger $ V.length xs) 0)
unaryOpOnValue OpArrJoin (Array xs) = String (arrJoinPrim xs Nothing)
unaryOpOnValue OpArrMax  Null       = Null
unaryOpOnValue OpArrMax  (Array xs) = V.maximum xs
unaryOpOnValue OpArrMin  Null       = Null
unaryOpOnValue OpArrMin  (Array xs) = V.minimum xs
unaryOpOnValue OpSort    Null       = Null
unaryOpOnValue OpSort    (Array xs) = Array (V.fromList $ L.sort $ V.toList xs)
-}
unaryOpOnValue op v =
  throwSQLException CodegenException Nothing ("Operation " <> show op <> " on " <> show v <> " is not supported")

funcOnScientific :: RealFloat a => (a -> a) -> Scientific -> Scientific
funcOnScientific f = fromFloatDigits . f . toRealFloat

--------------------------------------------------------------------------------
jsonOpOnObject :: JsonOp -> FlowObject -> FlowObject -> Text -> Maybe Text -> FlowObject
jsonOpOnObject op o1 o2 field startStream_m = case op of
  JOpArrow -> let v2 = L.head (HM.elems o2)
               in case v2 of
                    FlowText t ->
                      let (_,v) = getField o1 t
                       in HM.fromList [(SKey field startStream_m Nothing, v)]
                    _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator ->")
  JOpLongArrow -> let v2 = L.head (HM.elems o2)
                   in case v2 of
                        FlowText t ->
                          let (_,v) = getField o1 t
                           in HM.fromList [(SKey field startStream_m Nothing, FlowText (T.pack $ show v))] -- FIXME: show FlowValue
                        _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator ->>")
  JOpHashArrow -> let v2 = L.head (HM.elems o2)
                   in case v2 of
                        FlowArray arr ->
                          let v = go (FlowSubObject o1) arr
                           in HM.fromList [(SKey field startStream_m Nothing, v)]
                        _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator #>")
  JOpHashLongArrow -> let v2 = L.head (HM.elems o2)
                       in case v2 of
                            FlowArray arr ->
                              let v = go (FlowSubObject o1) arr
                               in HM.fromList [(SKey field startStream_m Nothing, FlowText (T.pack $ show v))]
                            _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator #>>")
  where
    go :: FlowValue -> [FlowValue] -> FlowValue
    go value [] = value
    go value (v:vs) =
      case v of
        FlowText t -> let (FlowSubObject object) = value
                          (_,value') = getField object t
                       in go value' vs
        FlowInt n -> let (FlowArray arr) = value
                         value' = arr L.!! n
                      in go value' vs
        _ -> throwSQLException CodegenException Nothing (show v <> " is not supported on the right of operator #> and #>>")

--------------------------------------------------------------------------------
diffTimeToMs :: DiffTime -> Int64
diffTimeToMs diff = fromInteger $ diffTimeToPicoseconds diff `div` 10^9

composeColName :: Maybe StreamName -> FieldName -> Text
composeColName Nothing field       = field
composeColName (Just stream) field = stream <> "." <> field

--------------------------------------------------------------------------------
-- | Remove the first occurrence in `xs` of each element of `ys`.
infix 5 \\
(\\) :: Eq a => Vector a -> Vector a -> Vector a
xs \\ ys | null xs   = V.empty
         | null ys   = xs
         | otherwise = V.fromList $ V.toList xs L.\\ V.toList ys

-- | The intersect function takes the vector intersection of two vectors.
-- >>> :set -XOverloadedLists
-- >>> [1, 2, 3, 4] `intersect` [2, 4, 6, 8]
-- [2, 4]
-- >>> [1, 2, 2, 3, 4] `intersect` [6, 4, 4, 2]
-- [2, 2, 4] -- If the first list contains duplicates, so will the result.
intersect :: Eq a => Vector a -> Vector a -> Vector a
intersect xs ys | null xs || null ys = V.empty
                | otherwise = V.fromList $ V.toList xs `L.intersect` V.toList ys

-- | Removes duplicate elements from a vector.
nub :: Eq a => Vector a -> Vector a
nub xs | null xs   = xs
       | otherwise = V.fromList $ L.nub (V.toList xs)

-- | Creates a flat string representation of all the elements contained in the given array.
-- Elements in the resulting string are separated by
-- the chosen 'delimiter :: Text', which is an optional
-- parameter that falls back to a comma @,@.
arrJoinPrim :: Vector Value -> Maybe T.Text -> T.Text
arrJoinPrim xs delimiterM | null xs = T.empty
  | otherwise = T.dropEnd 1 $ foldl' h T.empty xs where
  delimiter :: T.Text
  delimiter = fromMaybe "," delimiterM
  h :: T.Text -> Value -> T.Text
  h txt val = txt <> (case val of -- Test whether the given value is a primary value.
    String str -> str
    Number n   -> T.pack (show n)
    Bool   b   -> T.pack (show b)
    Null       -> "NULL"
    notPrim    -> throwSQLException CodegenException Nothing
      ("Operation OpArrJoin on " <> show notPrim <> " is not supported")
    ) <> delimiter

-- | Where the argument type is complex, for example, 'Array' or 'Map', the
-- contents of the complex type are not inspected.
-- Return 'Null' when all arguments are 'Null'.
ifNull :: Value -> Value -> Value
ifNull Null = id
ifNull x    = const x

strToDateGMT :: T.Text -> T.Text -> Scientific
strToDateGMT date fmt = parseSystemTimeGMT (case timeFmt fmt of
  Just x -> x
  _ -> throwSQLException CodegenException Nothing
    ("Operation OpStrDate on time format " <> show fmt <> " is not supported")) (textToCBytes date)
      & systemSeconds & Prelude.toInteger & flip scientific 0

dateToStrGMT :: Scientific -> T.Text -> T.Text
dateToStrGMT date fmt =
  let sysTime = MkSystemTime (case toBoundedInteger date of
        Just x -> x
        _ -> throwSQLException CodegenException Nothing "Impossible happened...") 0
  in formatSystemTimeGMT (case timeFmt fmt of
  Just x -> x
  _ -> throwSQLException CodegenException Nothing
    ("Operation OpDateStr on time format " <> show fmt <> " is not supported")) sysTime
      & cBytesToText

timeFmt :: T.Text -> Maybe ZCB.CBytes
timeFmt fmt
  | textToCBytes fmt == simpleDateFormat  = Just simpleDateFormat
  | textToCBytes fmt == iso8061DateFormat = Just iso8061DateFormat
  | textToCBytes fmt == webDateFormat     = Just webDateFormat
  | textToCBytes fmt == mailDateFormat    = Just mailDateFormat
  | otherwise                             = Nothing
