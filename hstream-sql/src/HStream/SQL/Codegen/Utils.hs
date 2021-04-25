{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE StrictData        #-}

module HStream.SQL.Codegen.Utils
  ( getFieldByName
  , getFieldByName'
  , genRandomSinkTopic
  , genMockSinkTopic
  , compareValue
  , binOpOnValue
  , unaryOpOnValue
  , diffTimeToMs
  , composeColName
  , genJoiner
  ) where

import           Data.Aeson
import qualified Data.HashMap.Strict   as HM
import           Data.Scientific
import           Data.Time             (DiffTime, diffTimeToPicoseconds)
import           HStream.SQL.AST
import           HStream.SQL.Exception (SomeSQLException (..),
                                        throwSQLException)
import           Prelude               (read)
import           RIO
import           Text.StringRandom     (stringRandomIO)

--------------------------------------------------------------------------------
getFieldByName :: Object -> Text -> Value
getFieldByName = (HM.!)

getFieldByName' :: Object -> Text -> Maybe Value
getFieldByName' = flip HM.lookup

--------------------------------------------------------------------------------
genRandomSinkTopic :: IO Text
genRandomSinkTopic = stringRandomIO "[a-zA-Z]{20}"

genMockSinkTopic :: IO Text
genMockSinkTopic = return "demoSink"

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
binOpOnValue OpAnd (Bool b1)  (Bool b2)  = Bool (b1 && b2)
binOpOnValue OpOr  (Bool b1)  (Bool b2)  = Bool (b1 || b2)
binOpOnValue op v1 v2 =
  throwSQLException CodegenException Nothing ("Operation " <> show op <> " on " <> show v1 <> " and " <> show v2 <> " is not supported")

unaryOpOnValue :: HasCallStack => UnaryOp -> Value -> Value
unaryOpOnValue OpSin (Number n) = Number (funcOnScientific n sin)
unaryOpOnValue OpAbs (Number n) = Number (abs n)
unaryOpOnValue op v =
  throwSQLException CodegenException Nothing ("Operation " <> show op <> " on " <> show v <> " is not supported")

funcOnScientific :: Scientific -> (Double -> Double) -> Scientific
funcOnScientific sci f = read . show . f . toRealFloat $ sci

--------------------------------------------------------------------------------
diffTimeToMs :: DiffTime -> Int64
diffTimeToMs diff = fromInteger $ diffTimeToPicoseconds diff `div` 10^9

composeColName :: Maybe StreamName -> FieldName -> Text
composeColName Nothing field       = field
composeColName (Just stream) field = stream <> "." <> field
