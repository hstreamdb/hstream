{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.JsonOp
  ( jsonOpOnObject
  ) where

import qualified Data.HashMap.Lazy         as HM
import qualified Data.List                 as L
import           Data.Text                 (Text)
import qualified Data.Text                 as T
import           DiffFlow.Error
import           HStream.SQL.AST
import           HStream.SQL.Codegen.SKey
import           HStream.SQL.Codegen.Utils (destructSingletonFlowObject)

--------------------------------------------------------------------------------
jsonOpOnObject :: JsonOp -> FlowObject -> FlowObject -> Text -> Either DiffFlowError FlowObject
jsonOpOnObject op o1 o2 field = case op of
  JOpArrow -> case destructSingletonFlowObject o2 of
                Left e       -> Left e
                Right (_,v2) -> case v2 of
                  FlowText t ->
                    let (_,v) = getField' t Nothing Nothing o1
                     in Right $ HM.fromList [(SKey field Nothing Nothing, v)]
                  _          -> Left . RunShardError $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator ->"
  JOpLongArrow -> case destructSingletonFlowObject o2 of
                    Left e       -> Left e
                    Right (_,v2) -> case v2 of
                      FlowText t ->
                          let (_,v) = getField' t Nothing Nothing o1
                           in Right $ HM.fromList [(SKey field Nothing Nothing, FlowText (T.pack $ show v))] -- FIXME: show FlowValue
                      _          -> Left . RunShardError $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator -->"
  JOpHashArrow -> let v2 = L.head (HM.elems o2)
                   in case v2 of
                        FlowArray arr ->
                          case go (FlowSubObject o1) arr of
                            Left e  -> Left e
                            Right v -> Right $ HM.fromList [(SKey field Nothing Nothing, v)]
                        _             ->
                          Left . RunShardError $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator #>"
  JOpHashLongArrow -> let v2 = L.head (HM.elems o2)
                       in case v2 of
                            FlowArray arr ->
                              case go (FlowSubObject o1) arr of
                                Left e  -> Left e
                                Right v -> Right $ HM.fromList [(SKey field Nothing Nothing, FlowText (T.pack $ show v))]
                            _             ->
                              Left . RunShardError $ "Value " <> T.pack (show v2) <> " is not supported on the right of operator ##>"
  where
    go :: FlowValue -> [FlowValue] -> Either DiffFlowError FlowValue
    go value [] = Right value
    go value (v:vs) =
      case v of
        FlowText t ->
          case value of
            FlowSubObject object -> let (_,value') = getField' t Nothing Nothing object
                                     in go value' vs
            _                    -> let e = RunShardError $ "Operator #> or #>>: type mismatch on " <> T.pack (show value) <> " and " <> T.pack (show v)
                                     in Left e
        FlowInt n ->
          case value of
            FlowArray arr -> if n >= 0 && n < L.length arr then
                               let value' = arr L.!! n in go value' vs else
                               let e = RunShardError $ "Index out of bound: " <> T.pack (show n) <> " on value" <> T.pack (show value) in
                                 Left e
            _             -> let e = RunShardError $ "Operator #> or #>>: type mismatch on " <> T.pack (show value) <> " and " <> T.pack (show v)
                              in Left e
        _ -> Left . RunShardError $ "Value " <> T.pack (show v) <> " is not supported on operator #> or #>>"
