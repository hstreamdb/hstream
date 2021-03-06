{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils.Format where

import qualified Data.Aeson                        as A
import qualified Data.Aeson.Text                   as A
import qualified Data.HashMap.Strict               as HM
import           Data.List                         (sort)
import qualified Data.Map.Strict                   as M
import           Data.Maybe                        (maybeToList)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import           Data.Time.Clock                   (NominalDiffTime)
import qualified Data.Vector                       as V
import           Text.Layout.Table                 (center, colsAllG, column,
                                                    expand, justify, left,
                                                    noAlign, singleCutMark,
                                                    tableString, titlesH,
                                                    unicodeRoundS)
import qualified ThirdParty.Google.Protobuf.Struct as P

import qualified HStream.Server.HStreamApi         as HA
import           HStream.Utils.Converter           (jsonValueToValue,
                                                    valueToJsonValue)

--------------------------------------------------------------------------------

type Width = Int

formatResult :: Width -> P.Struct -> String
formatResult width (P.Struct kv) =
  case M.toList kv of
    [("SHOWSTREAMS", Just v)] -> emptyNotice . unlines .  words . formatValue $ v
    [("SHOWVIEWS",   Just v)] -> emptyNotice . unlines .  words . formatValue $ v
    [("SELECT",      Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
    [("SELECTVIEW",  Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
    [("SHOWQUERIES", Just (P.Value (Just (P.ValueKindListValue (P.ListValue xs)))))] -> renderTableResult xs
    [("SHOWCONNECTORS", Just (P.Value (Just (P.ValueKindListValue (P.ListValue xs)))))] -> renderTableResult xs
    [("Error Message:", Just v)] -> "Error Message: " ++  formatValue v ++ "\n"
    x -> show x
  where
    renderTableResult = emptyNotice . renderJSONObjectsToTable width . getObjects . map valueToJsonValue . V.toList
    emptyNotice xs = if null (words xs) then "Succeeded. No Results\n" :: String else xs

formatCommandQueryResponse :: Width -> HA.CommandQueryResponse -> String
formatCommandQueryResponse w (HA.CommandQueryResponse (Just x)) = case x of
  HA.CommandQueryResponseKindSuccess _ ->
    "Command successfully executed.\n"
  HA.CommandQueryResponseKindResultSet (HA.CommandQueryResultSet [])  ->
    "No results.\n"
  HA.CommandQueryResponseKindResultSet (HA.CommandQueryResultSet [y]) ->
    formatResult w y
  HA.CommandQueryResponseKindResultSet (HA.CommandQueryResultSet ys)  ->
    "unknown behaviour" <> show ys
formatCommandQueryResponse _ _ = ""

--------------------------------------------------------------------------------

formatStruct :: P.Struct -> String
formatStruct (P.Struct kv) = unlines . map (\(x, y) -> TL.unpack x ++ (": " <> (concat . maybeToList) y))
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
renderJSONObjectsToTable _ [] = "\n"
renderJSONObjectsToTable l os@(o:_) =
  tableString colout unicodeRoundS (titlesH keys)
  (map (colsAllG center . renderContents ((l - size*2 - 6)`div` size)) elems) ++ "\n"
  where
    keys  = sort $ map T.unpack (HM.keys o)
    elems = map (map snd . sort . HM.toList) os
    size  = length o
    colout = replicate size
      $ column expand left noAlign (singleCutMark "...")

renderContents :: Width -> [A.Value] -> [[String]]
renderContents width = map $ concatMap (justify width . words') . lines . formatValue . jsonValueToValue

renderJSONToTable :: Width -> A.Value -> String
renderJSONToTable width (A.Object hmap) = renderJSONObjectToTable width hmap
renderJSONToTable _ x                   = show x

--------------------------------------------------------------------------------

approxNaturaltime :: NominalDiffTime -> String
approxNaturaltime n
  | n < 0 = ""
  | n == 0 = "0s"
  | n < 1 = show @Int (floor $ n * 1000) ++ "ms"
  | n < 60 = show @Int (floor n) ++ "s"
  | n < fromIntegral hour = show @Int (floor n `div` 60) ++ " min"
  | n < fromIntegral day  = show @Int (floor n `div` hour) ++ " hours"
  | n < fromIntegral year = show @Int (floor n `div` day) ++ " days"
  | otherwise = show @Int (floor n `div` year) ++ " years"
  where
    hour = 60 * 60
    day = hour * 24
    year = day * 365

--------------------------------------------------------------------------------

removePunc :: String -> String
removePunc xs = [ x | x <- xs, x `notElem` (['}','{','\"','\''] :: [Char])]

getObjects vs = [ object | A.Object object <- vs ]
getObjects :: [A.Value] -> [A.Object]

words' :: String -> [String]
words' s = let (m, n) = break (== '-') s in words m ++ words n
