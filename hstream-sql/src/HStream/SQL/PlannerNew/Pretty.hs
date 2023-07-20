{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.SQL.PlannerNew.Pretty where

import qualified Data.IntMap                             as IntMap
import qualified Data.List                               as L
import qualified Data.Map                                as Map
import qualified Data.Text                               as T
import           Data.Text.Prettyprint.Doc
import           Data.Text.Prettyprint.Doc.Render.String

import           HStream.SQL.Binder
import           HStream.SQL.PlannerNew.Types

instance Pretty Schema where
  pretty (Schema _ cols) = "(" <>
    hcat (L.intersperse ", "
           (L.map (\ColumnCatalog{..} -> pretty columnStreamId <> "_" <> pretty columnName <> ":" <> viaShow columnType)
             (IntMap.elems cols)
           )
         ) <>
    ")"

instance Pretty RelationExpr where
  pretty expr = case expr of
                  StreamScan schema            ->
                     "Scan" <+> pretty (schemaOwner schema)
                     <+> "|" <+> pretty schema
                  LoopJoinOn schema r1 r2 e typ t       ->
                     "LoopJoin" <+>  "[" <> viaShow t <>  "ms]" <+> viaShow e <+> viaShow typ
                     <+> "|" <+> pretty schema <>
                    line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)
                  Filter schema r e                   ->
                     "Filter (" <+> viaShow e  <> ")"
                     <+> "|" <+> pretty schema
                     <> line <> (indent 2 $ pretty r)
                  Project schema r scalars       ->
                     "Project"
                     <+> "(" <> hcat (L.intersperse ", " $ L.map viaShow scalars) <> ")"
                     <+> "|" <+> pretty schema
                     <> line <> (indent 2 $ pretty r)
                  Reduce schema r scalars aggs win_m      ->
                     "Reduce"
                     <+> "(" <> hcat (L.intersperse "," $ L.map viaShow scalars) <> ")"
                     <+> "aggs=" <> viaShow aggs
                     <+> "window=" <> viaShow win_m
                     <+> "|" <+> pretty schema
                     <> line <> (indent 2 $ pretty r)
                  Distinct schema r                   ->
                     "Distinct"
                     <+> "|" <+> pretty schema
                     <> line <> (indent 2 $ pretty r)
                  Union schema r1 r2                  ->
                     "Union"
                     <+> "|" <+> pretty schema
                     <> line <> (indent 2 $ pretty r1) <> line <> (indent 2 $ pretty r2)

instance Show RelationExpr where
  show expr = renderString $ layoutPretty defaultLayoutOptions (pretty expr)
