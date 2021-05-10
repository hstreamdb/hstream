{-# LANGUAGE RecordWildCards #-}
module HStream.Format where

import qualified Data.Aeson                        as A
import qualified Data.Aeson.Text                   as A
import qualified Data.HashMap.Strict               as HM
import qualified Data.Map.Strict                   as M
import           Data.Maybe                        (maybeToList)
import qualified Data.Text                         as T
import qualified Data.Text.Lazy                    as TL
import qualified Data.Vector                       as V
import qualified HStream.SQL.Exception             as E
import qualified HStream.Server.HStreamApi         as HA
import qualified ThirdParty.Google.Protobuf.Struct as P

type Width = Int
type Box = (Width, String)
type Row = ([Width], [String])
type Table = ([Width], [Row])

vBorder :: [Width] -> String
vBorder = foldr ((++) . (:) '+' . flip replicate '-' . (+ 2)) "+"

box :: String -> Box
box str = (length str, str)

emptyRow :: ([Width], [String])
emptyRow = ([],[])

emptyTable :: ([Width], [Row])
emptyTable = (repeat 0, [])

addBox :: Row -> Box -> Row
addBox (ws, ss) (w, s)= (w : ws, s : ss)

addRow :: Table -> Row -> Table
addRow (tws, rs) (rws, ss) = let ws' = zipWith max rws tws in (ws', (ws', ss) : rs)

renderBox :: Box -> String
renderBox (w, s) = ' ' : s ++ replicate (w + 1 - length s) ' ' ++ "|"

renderRow :: Row -> String
renderRow (ws, ss) = '|' : (concatMap renderBox . zip ws) ss

renderTable :: Table -> [String]
renderTable (ws, rs) = vBorder ws : renderTable' rs ++ [vBorder ws]
  where
    renderTable' ((_, ss) : rs') = renderRow (ws, ss) : renderTable' rs'
    renderTable' []              = []

renderJSONObjectToTable :: A.Object -> Table
renderJSONObjectToTable hmap =
  emptyTable `addRow` foldr (flip addBox . box . TL.unpack . A.encodeToLazyText) emptyRow elems
             `addRow` foldr (flip addBox . box . T.unpack) emptyRow keys
  where
    keys  = HM.keys hmap
    elems = HM.elems hmap

renderJSONToTable :: A.Value -> Table
renderJSONToTable (A.Object hmap) = renderJSONObjectToTable hmap
renderJSONToTable x = emptyTable `addRow` (emptyRow `addBox` (box . show) x)

--------------------------------------------------------------------------------------------------
formatSomeSQLException :: E.SomeSQLException -> String
formatSomeSQLException (E.ParseException   info) = "Parse exception " ++ formatParseExceptionInfo info ++ "\n"
formatSomeSQLException (E.RefineException  info) = "Refine exception at " ++ show info ++ "\n"
formatSomeSQLException (E.CodegenException info) = "Codegen exception at " ++ show info ++ "\n"

formatParseExceptionInfo :: E.SomeSQLExceptionInfo -> String
formatParseExceptionInfo E.SomeSQLExceptionInfo{..} =
  case words sqlExceptionMessage of
    "syntax" : "error" : "at" : "line" : x : "column" : y : ss  -> "at <line " ++ x ++ "column " ++ y ++ ">: syntax error " ++ unwords ss ++ "."
    _ -> posInfo ++ sqlExceptionMessage ++ "."
 where
   posInfo = case sqlExceptionPosition of
      Just (l,c) -> "at <line " ++ show l ++ ", column " ++ show c ++ ">"
      Nothing    -> ""

formatCommandQueryResponse :: HA.CommandQueryResponse -> String
formatCommandQueryResponse (HA.CommandQueryResponse (Just x)) = case x of
  HA.CommandQueryResponseKindSuccess _ -> "Command successfully executed.\n"
  HA.CommandQueryResponseKindResultSet (HA.CommandQueryResultSet y) -> (unlines . map ( (++ ".") .formatStruct) . V.toList) y

formatStruct :: P.Struct -> String
formatStruct (P.Struct kv) = unwords . map (\(x, y) -> TL.unpack x ++ (' ' : (concat . maybeToList) y)) . M.toList . fmap (fmap formatValue) $ kv

formatValue :: P.Value -> String
formatValue (P.Value Nothing)  = ""
formatValue (P.Value (Just x)) = formatValueKind x

formatValueKind :: P.ValueKind -> String
formatValueKind (P.ValueKindNullValue _) = "NULL"
formatValueKind (P.ValueKindNumberValue n) = show n
formatValueKind (P.ValueKindStringValue s) = TL.unpack s
formatValueKind (P.ValueKindBoolValue   b) = show b
formatValueKind (P.ValueKindStructValue struct) = formatStruct struct
formatValueKind (P.ValueKindListValue (P.ListValue vs)) = unwords . map formatValue . V.toList $ vs
