{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module Language.SQL.Codegen.Utils
  ( getFieldByName
  , getFieldByName'
  , genRandomSinkTopic
  , genMockSinkTopic
  , compareValue
  , opOnValue
  , diffTimeToMs
  , composeColName
  ) where

import           Data.Aeson
import qualified Data.HashMap.Strict as HM
import           Data.Time           (DiffTime, diffTimeToPicoseconds)
import           Language.SQL.AST
import           RIO
import           Text.StringRandom   (stringRandomIO)

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
compareValue :: Value -> Value -> Ordering
compareValue (Number x1) (Number x2) = x1 `compare` x2
compareValue (String x1) (String x2) = x1 `compare` x2
compareValue _ _                     = error "Value does not support comparison"

opOnValue :: BinaryOp -> Value -> Value -> Value
opOnValue OpAdd (Number n) (Number m) = Number (n+m)
opOnValue OpSub (Number n) (Number m) = Number (n-m)
opOnValue OpMul (Number n) (Number m) = Number (n*m)
opOnValue op v1 v2 = error $ "Operation " <> show op <> " on " <> show v1 <> " and " <> show v2 <> " is not supported"

--------------------------------------------------------------------------------
diffTimeToMs :: DiffTime -> Int64
diffTimeToMs diff = fromInteger $ diffTimeToPicoseconds diff `div` 10^9

composeColName :: Maybe StreamName -> FieldName -> Text
composeColName Nothing field       = field
composeColName (Just stream) field = stream <> "." <> field
