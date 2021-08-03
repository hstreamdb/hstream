{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.Utils
  ( getFieldByName
  , getFieldByName'
  , genRandomSinkStream
  , genMockSinkStream
  , compareValue
  , binOpOnValue
  , unaryOpOnValue
  , diffTimeToMs
  , composeColName
  , genJoiner
  ) where

import           Control.Exception     (throw)
import           Data.Aeson
import qualified Data.HashMap.Strict   as HM
import           Data.Scientific
import qualified Data.Text             as T
import           Data.Time             (DiffTime, diffTimeToPicoseconds)
import           GHC.Stack             (callStack)
import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeRuntimeException (..),
                                        SomeSQLException (..),
                                        throwSQLException)
import           RIO
import           Text.StringRandom     (stringRandomIO)

--------------------------------------------------------------------------------
getFieldByName :: HasCallStack => Object -> Text -> Value
getFieldByName o k =
  case HM.lookup k o of
    Nothing -> throw
      SomeRuntimeException
      { runtimeExceptionMessage = "Key " <> show k <> " is not found in object " <> show o
      , runtimeExceptionCallStack = callStack
      }
    Just v  -> v

getFieldByName' :: Object -> Text -> Maybe Value
getFieldByName' = flip HM.lookup

--------------------------------------------------------------------------------
genRandomSinkStream :: IO Text
genRandomSinkStream = stringRandomIO "[a-zA-Z]{20}"

genMockSinkStream :: IO Text
genMockSinkStream = return "demoSink"

--------------------------------------------------------------------------------
genJoiner :: StreamName -> StreamName -> Object -> Object -> Object
genJoiner s1 s2 o1 o2 = HM.union (HM.fromList l1') (HM.fromList l2')
  where l1 = HM.toList o1
        l2 = HM.toList o2
        l1' = (\(k,v) -> (s1 <> "." <> k, v)) <$> l1
        l2' = (\(k,v) -> (s2 <> "." <> k, v)) <$> l2

--------------------------------------------------------------------------------
compareValue :: HasCallStack => Value -> Value -> Ordering
compareValue (Number x1) (Number x2) = x1 `compare` x2
compareValue (String x1) (String x2) = x1 `compare` x2
compareValue _ _                     =
  throwSQLException CodegenException Nothing "Value does not support comparison"

binOpOnValue :: HasCallStack => BinaryOp -> Value -> Value -> Value
binOpOnValue OpAdd (Number n) (Number m) = Number (n+m)
binOpOnValue OpSub (Number n) (Number m) = Number (n-m)
binOpOnValue OpMul (Number n) (Number m) = Number (n*m)
binOpOnValue OpDiv (Number n) (Number m) = Number (n/m)
binOpOnValue OpAnd (Bool b1)  (Bool b2)  = Bool (b1 && b2)
binOpOnValue OpOr  (Bool b1)  (Bool b2)  = Bool (b1 || b2)
binOpOnValue op v1 v2 =
  throwSQLException CodegenException Nothing ("Operation " <> show op <> " on " <> show v1 <> " and " <> show v2 <> " is not supported")

unaryOpOnValue :: HasCallStack => UnaryOp -> Value -> Value
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
unaryOpOnValue op v =
  throwSQLException CodegenException Nothing ("Operation " <> show op <> " on " <> show v <> " is not supported")

funcOnScientific :: RealFloat a => (a -> a) -> Scientific -> Scientific
funcOnScientific f = fromFloatDigits . f . toRealFloat

--------------------------------------------------------------------------------
diffTimeToMs :: DiffTime -> Int64
diffTimeToMs diff = fromInteger $ diffTimeToPicoseconds diff `div` 10^9

composeColName :: Maybe StreamName -> FieldName -> Text
composeColName Nothing field       = field
composeColName (Just stream) field = stream <> "." <> field
