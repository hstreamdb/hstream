{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module HStream.Format where

import qualified Data.Aeson                        as A
import           Data.Aeson.Encode.Pretty
import qualified Data.Aeson.Text                   as A
import qualified Data.HashMap.Strict               as HM
import qualified Data.Map.Strict                   as M
import           Data.Maybe                        (maybeToList)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import           Data.Text.Lazy.Builder            (toLazyText)
import qualified Data.Vector                       as V
import qualified HStream.SQL.Exception             as E
import qualified HStream.Server.HStreamApi         as HA
import           HStream.Server.Utils              (structToJsonObject,
                                                    structToZJsonObject,
                                                    valueToJsonValue)
import           Text.Layout.Table
import qualified ThirdParty.Google.Protobuf.Struct as P

type Width = Int

formatResult :: Width -> P.Struct -> String
formatResult width struct@(P.Struct kv) =
  case M.toList kv of
    [("SHOWSTREAMS", Just v)] -> unlines .  words . formatValue $ v
    [("SHOWQUERIES", Just (P.Value (Just (P.ValueKindListValue (P.ListValue xs)))))] ->
      renderJSONObjectsToTable width $ getObjects $ map valueToJsonValue $ V.toList xs
   --   unlines $ map (TL.unpack . toLazyText . encodePrettyToTextBuilder' defConfig {confIndent = Spaces 1} . valueToJsonValue) $ V.toList xs
    [("SELECT", Just (P.Value (Just (P.ValueKindStructValue struct))))] ->
      renderJSONObjectToTable width $ structToJsonObject struct
    x -> show x

formatSomeSQLException :: E.SomeSQLException -> String
formatSomeSQLException (E.ParseException   info) = "Parse exception " ++ formatParseExceptionInfo info
formatSomeSQLException (E.RefineException  info) = "Refine exception at " ++ show info
formatSomeSQLException (E.CodegenException info) = "Codegen exception at " ++ show info

formatParseExceptionInfo :: E.SomeSQLExceptionInfo -> String
formatParseExceptionInfo E.SomeSQLExceptionInfo{..} =
  case words sqlExceptionMessage of
    "syntax" : "error" : "at" : "line" : x : "column" : y : ss ->
      "at <line " ++ x ++ "column " ++ y ++ ">: syntax error " ++ unwords ss ++ "."
    _ -> posInfo ++ sqlExceptionMessage ++ "."
 where
   posInfo = case sqlExceptionPosition of
      Just (l,c) -> "at <line " ++ show l ++ ", column " ++ show c ++ ">"
      Nothing    -> ""

formatCommandQueryResponse :: HA.CommandQueryResponse -> String
formatCommandQueryResponse (HA.CommandQueryResponse (Just x)) = case x of
  HA.CommandQueryResponseKindSuccess _ ->
    "Command successfully executed."
  HA.CommandQueryResponseKindResultSet (HA.CommandQueryResultSet y) ->
    (unlines . map ( (++ ".") .formatStruct) . V.toList) y

--------------------------------------------------------------------------------

formatStruct :: P.Struct -> String
formatStruct (P.Struct kv) = unlines . map (\(x, y) -> TL.unpack x ++ (':' : (concat . maybeToList) y))
                                           . M.toList . fmap (fmap formatValue) $ kv

formatValue :: P.Value -> String
formatValue (P.Value Nothing)  = ""
formatValue (P.Value (Just x)) = formatValueKind x

formatValueKind :: P.ValueKind -> String
formatValueKind (P.ValueKindNullValue _)   = "NULL"
formatValueKind (P.ValueKindNumberValue n) = show n
formatValueKind (P.ValueKindStringValue s) = TL.unpack s
formatValueKind (P.ValueKindBoolValue   b) = show b
formatValueKind (P.ValueKindStructValue s) = formatStruct s
formatValueKind (P.ValueKindListValue (P.ListValue vs)) = unwords . map formatValue . V.toList $ vs

--------------------------------------------------------------------------------

renderJSONObjectToTable :: Width -> A.Object -> String
renderJSONObjectToTable w os = renderJSONObjectsToTable w [os]

renderJSONObjectsToTable :: Width -> [A.Object] -> String
renderJSONObjectsToTable _ [] = ""
renderJSONObjectsToTable l os@(o:_) =
  tableString colout unicodeRoundS (titlesH keys) $
  map (colsAllG center . renderContents (l `div` (size + 2))) elems
  where
    keys  = map T.unpack (HM.keys o)
    elems = map HM.elems os
    size  = length keys
    colout = replicate size
      $ column (expandUntil $ l `div` (size + 1)) left noAlign (singleCutMark "...")

renderContents :: Width -> [A.Value] -> [[String]]
renderContents width = map $ concatMap (justifyText width) . lines . TL.unpack . toLazyText
                           . encodePrettyToTextBuilder' defConfig {confIndent = Spaces 1}

renderJSONToTable :: Width -> A.Value -> String
renderJSONToTable width (A.Object hmap) = renderJSONObjectToTable width hmap
renderJSONToTable _ x                   = show x

--------------------------------------------------------------------------------

getObjects vs = [ object | A.Object object <- vs ]
getObjects :: [A.Value] -> [A.Object]
