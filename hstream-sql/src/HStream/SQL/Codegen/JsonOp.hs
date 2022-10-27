module HStream.SQL.Codegen.JsonOp where

import qualified Data.HashMap.Lazy        as HM
import qualified Data.List                as L
import           Data.Text                (Text)
import qualified Data.Text                as T
import           HStream.SQL.AST
import           HStream.SQL.Codegen.SKey
import           HStream.SQL.Exception

--------------------------------------------------------------------------------
jsonOpOnObject :: JsonOp -> FlowObject -> FlowObject -> Text -> FlowObject
jsonOpOnObject op o1 o2 field = case op of
  JOpArrow -> let v2 = L.head (HM.elems o2)
               in case v2 of
                    FlowText t ->
                      let (_,v) = getField' t Nothing Nothing o1
                       in HM.fromList [(SKey field Nothing Nothing, v)]
                    _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator ->")
  JOpLongArrow -> let v2 = L.head (HM.elems o2)
                   in case v2 of
                        FlowText t ->
                          let (_,v) = getField' t Nothing Nothing o1
                           in HM.fromList [(SKey field Nothing Nothing, FlowText (T.pack $ show v))] -- FIXME: show FlowValue
                        _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator ->>")
  JOpHashArrow -> let v2 = L.head (HM.elems o2)
                   in case v2 of
                        FlowArray arr ->
                          let v = go (FlowSubObject o1) arr
                           in HM.fromList [(SKey field Nothing Nothing, v)]
                        _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator #>")
  JOpHashLongArrow -> let v2 = L.head (HM.elems o2)
                       in case v2 of
                            FlowArray arr ->
                              let v = go (FlowSubObject o1) arr
                               in HM.fromList [(SKey field Nothing Nothing, FlowText (T.pack $ show v))]
                            _ -> throwSQLException CodegenException Nothing (show v2 <> " is not supported on the right of operator #>>")
  where
    go :: FlowValue -> [FlowValue] -> FlowValue
    go value [] = value
    go value (v:vs) =
      case v of
        FlowText t -> let (FlowSubObject object) = value
                          (_,value') = getField' t Nothing Nothing object
                       in go value' vs
        FlowInt n -> let (FlowArray arr) = value
                         value' = arr L.!! n
                      in go value' vs
        _ -> throwSQLException CodegenException Nothing (show v <> " is not supported on the right of operator #> and #>>")
