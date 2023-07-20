{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.JsonOp
  ( jsonOpOnValue
  ) where

import qualified Data.List                         as L
import qualified Data.Text                         as T
#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST
import           HStream.SQL.Codegen.ColumnCatalog
import           HStream.SQL.Rts
-- basic operators. defined in AST in schemaless version
#ifdef HStreamEnableSchema
import           HStream.SQL.Binder
#endif

#ifdef HStreamUseV2Engine
#define ERROR_TYPE DiffFlowError
#define ERR RunShardError
#else
#define ERROR_TYPE HStreamProcessingError
#define ERR OperationError
#endif

--------------------------------------------------------------------------------
jsonOpOnValue :: JsonOp -> FlowValue -> FlowValue -> Either ERROR_TYPE FlowValue
#ifndef HStreamEnableSchema
jsonOpOnValue op v1 v2 = case v1 of
  FlowSubObject o1 -> case op of
    JOpArrow ->
      case v2 of
        FlowText t -> case getField (ColumnCatalog t Nothing) o1 of
                        Nothing    -> Left. ERR $ "Can not get column: " <> t
                        Just (_,v) -> Right v
        _          -> Left . ERR $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator ->"
    JOpLongArrow ->
      case v2 of
        FlowText t -> case getField (ColumnCatalog t Nothing) o1 of
                        Nothing    -> Left. ERR $ "Can not get column: " <> t
                        Just (_,v) -> Right $ FlowText (T.pack $ show v) -- FIXME: show FlowValue
        _          -> Left . ERR $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator -->"
    JOpHashArrow ->
      case v2 of
        FlowArray arr -> go v1 arr
        _             ->
          Left . ERR $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator #>"
    JOpHashLongArrow ->
      case v2 of
        FlowArray arr -> case go v1 arr of
          Left e  -> Left e
          Right v -> Right $ FlowText (T.pack $ show v)
        _             ->
          Left . ERR $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator ##>"
  _ -> Left . ERR $ "Value " <> T.pack (show v1) <> " is not supported on the left of a JSON operator"
  where
    go :: FlowValue -> [FlowValue] -> Either ERROR_TYPE FlowValue
    go value [] = Right value
    go value (v:vs) =
      case v of
        FlowText t ->
          case value of
            FlowSubObject object -> let (_,value') = getField' (ColumnCatalog t Nothing) object
                                     in go value' vs
            _                    -> let e = ERR $ "Operator #> or #>>: type mismatch on " <> T.pack (show value) <> " and " <> T.pack (show v)
                                     in Left e
        FlowInt n ->
          case value of
            FlowArray arr -> if n >= 0 && n < L.length arr then
                               let value' = arr L.!! n in go value' vs else
                               let e = ERR $ "Index out of bound: " <> T.pack (show n) <> " on value" <> T.pack (show value) in
                                 Left e
            _             -> let e = ERR $ "Operator #> or #>>: type mismatch on " <> T.pack (show value) <> " and " <> T.pack (show v)
                              in Left e
        _ -> Left . ERR $ "Value " <> T.pack (show v) <> " is not supported on operator #> or #>>"

#else
jsonOpOnValue = undefined
#endif
