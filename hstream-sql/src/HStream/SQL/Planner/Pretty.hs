module HStream.SQL.Planner.Pretty where

import qualified Data.List                               as L
import qualified Data.Map                                as Map
import qualified Data.Text                               as T
import           Data.Text.Prettyprint.Doc
import           Data.Text.Prettyprint.Doc.Render.String

import           HStream.SQL.AST
import           HStream.SQL.Planner

instance Pretty RelationExpr where
  pretty expr = case expr of
                  StreamScan s                 ->
                    pretty "StreamScan" <+> pretty s
                  StreamRename r s             ->
                    pretty "StreamRename" <+> pretty s <> line <> (indent 2 $ pretty r)
                  CrossJoin r1 r2              ->
                    pretty "CrossJoin" <> line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)
                  LoopJoinOn r1 r2 e typ       ->
                    pretty "LoopJoin" <+> viaShow e <+> viaShow typ <> line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)
                  LoopJoinUsing r1 r2 cols typ ->
                    pretty "LoopJoinUsing" <+> viaShow cols <+> viaShow typ <> line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)
                  LoopJoinNatural r1 r2 typ    ->
                    pretty "LoopJoinNatural" <+> viaShow typ <> line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)
                  Filter r e                   ->
                    pretty "Filter" <+> viaShow e <> line <> (indent 2 $ pretty r)
                  Project r cols streams       ->
                    pretty "Project" <+>
                    hsep (L.map (\(cata,alias) -> pretty "(name=" <> viaShow cata <> pretty ", alias=" <> viaShow alias <> pretty ")") cols) <+>
                    viaShow streams <> line <> (indent 2 $ pretty r)
                  Affiliate r tups             ->
                    pretty "Affiliate" <+>
                    hsep (L.map (\(cata,scalar) -> pretty "(name=" <> viaShow cata <> pretty ", expr=" <> viaShow scalar <> pretty ")") tups) <+>
                    line <> (indent 2 $ pretty r)
                  Reduce r tups aggs           ->
                    pretty "Reduce" <+>
                    hsep (L.map (\(cata,scalar) -> pretty "(key=" <> viaShow cata <> pretty ", expr=" <> viaShow scalar <> pretty ")") tups) <+>
                    pretty "aggs=" <> viaShow aggs <> line <> (indent 2 $ pretty r)
                  Distinct r                   ->
                    pretty "Distinct" <> line <> (indent 2 $ pretty r)
                  TimeWindow r win             ->
                    pretty "TimeWindow" <+> viaShow win <> line <> (indent 2 $ pretty r)
                  Union r1 r2                  ->
                    pretty "Union" <> line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)

instance Show RelationExpr where
  show expr = renderString $ layoutPretty defaultLayoutOptions (pretty expr)

instance Show ScalarExpr where
  show expr = case expr of
                ColumnRef field stream_m -> case stream_m of
                                              Nothing -> "#(" <> T.unpack field <> ")"
                                              Just s  -> "#(" <> T.unpack s <> "." <> T.unpack field <> ")"
                Literal constant         -> show constant
                CallUnary op e           -> show op <> "(" <> show e <> ")"
                CallBinary op e1 e2      -> show op <> "(" <> show e1 <> ", " <> show e2 <> ")"
                CallCast e typ           -> show e <> "::" <> show typ
                CallJson op e1 e2        -> show e1 <> show op <> show e2
                ValueArray arr           -> "Array" <> show arr
                ValueMap m               -> "Map" <> "[" <> (L.intercalate ", " (L.map (\(ke,ve) -> show ke <> "=>" <> show ve) (Map.toList m))) <> "]"
                AccessArray e rhs        -> show e <> show rhs
                AccessMap em ek          -> show em <> "[" <> show ek <> "]"

instance HasName ScalarExpr where
  getName = show
