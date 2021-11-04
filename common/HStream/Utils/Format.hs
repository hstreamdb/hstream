{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils.Format where

import qualified Data.Aeson                       as A
import qualified Data.Aeson.Text                  as A
import qualified Data.ByteString.Char8            as BS
import           Data.Default                     (def)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (sort)
import qualified Data.Map.Strict                  as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Data.Time.Clock                  (NominalDiffTime)
import qualified Data.Vector                      as V
import qualified Google.Protobuf.Empty            as Protobuf
import qualified Google.Protobuf.Struct           as P
import           Network.GRPC.HighLevel.Generated
import qualified Text.Layout.Table                as Table

import qualified HStream.Server.HStreamApi        as API
import           HStream.Utils.Converter          (valueToJsonValue)

--------------------------------------------------------------------------------

type Width = Int

renderTableResult :: Width -> V.Vector P.Value -> String
renderTableResult width = emptyNotice . renderJSONObjectsToTable width . getObjects . map valueToJsonValue . V.toList

emptyNotice :: String -> String
emptyNotice xs = if null (words xs) then "Succeeded. No Results\n" else xs

formatCommandQueryResponse :: Width -> API.CommandQueryResponse -> String
formatCommandQueryResponse w (API.CommandQueryResponse x) = case x of
  []  -> "Done. No results.\n"
  [y] -> formatResult w y
  ys  -> "unknown behaviour" <> show ys

class Format a where
  formatResult :: Width -> a -> String

instance Format Protobuf.Empty where
  formatResult _ = const "Done. No results.\n"

instance Format API.Stream where
  formatResult _ = (<> "\n") . T.unpack . API.streamStreamName

instance Format API.View where
  formatResult _ = show . API.viewViewId

instance Format [API.Stream] where
  formatResult w = emptyNotice . concatMap (formatResult w)

instance Format [API.View] where
  formatResult w = emptyNotice . unlines . map (formatResult w)

instance Format [API.Query] where
  formatResult w = emptyNotice . renderJSONObjectsToTable w . map ((\(A.Object o) -> o) . A.toJSON)

instance Format [API.Connector] where
  formatResult w = emptyNotice . renderJSONObjectsToTable w . map ((\(A.Object o) -> o) . A.toJSON)

instance Format a => Format (ClientResult 'Normal a) where
  formatResult w (ClientNormalResponse response _ _ _ _) = formatResult w response
  formatResult _  (ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInternal details)))
    = "Error: " <> BS.unpack (unStatusDetails details) <> "\n"
  formatResult _ (ClientErrorResponse err) = "Server Error: " <> show err <> "\n"

instance Format API.ListStreamsResponse where
  formatResult w = formatResult w . V.toList . API.listStreamsResponseStreams
instance Format API.ListViewsResponse where
  formatResult w = formatResult w . V.toList . API.listViewsResponseViews
instance Format API.ListQueriesResponse where
  formatResult w = formatResult w . V.toList . API.listQueriesResponseQueries
instance Format API.ListConnectorsResponse where
  formatResult w = formatResult w . V.toList . API.listConnectorsResponseConnectors

instance Format API.AppendResponse where
  formatResult _ = const "Done. No results.\n"

instance Format API.CreateQueryStreamResponse where
  formatResult _ = const "Done. No results.\n"

instance Format API.TerminateQueriesResponse where
  formatResult _ = const "Done. No results.\n"

instance Format P.Struct where
  formatResult _ (P.Struct kv) =
    case M.toList kv of
      [("SELECT",      Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("SELECTVIEW",  Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("Error Message:", Just v)] -> "Error Message: " ++ show v ++ "\n"
      x -> show x

--------------------------------------------------------------------------------

formatJSONObject :: A.Object -> String
formatJSONObject = unlines . map (\(x, y) -> T.unpack x ++ (": " <> y))
                . HM.toList . fmap formatJSONValue

formatJSONValue :: A.Value -> String
formatJSONValue (A.Object hmap)  = formatJSONObject hmap
formatJSONValue (A.Array  array) = unwords . V.toList $ formatJSONValue <$> array
formatJSONValue (A.String text)  = T.unpack text
formatJSONValue (A.Number sci)   = show sci
formatJSONValue (A.Bool   bool)  = show bool
formatJSONValue A.Null           = "NULL"

--------------------------------------------------------------------------------

renderJSONObjectToTable :: Width -> A.Object -> String
renderJSONObjectToTable w os = renderJSONObjectsToTable w [os]

renderJSONObjectsToTable :: Width -> [A.Object] -> String
renderJSONObjectsToTable _ [] = "\n"
renderJSONObjectsToTable l os@(o:_) =
  Table.tableString colout Table.unicodeRoundS (Table.titlesH keys)
  (map (Table.colsAllG Table.center . renderContents ((l - size*2 - 6)`div` size)) elems) ++ "\n"
  where
    keys  = sort $ map T.unpack (HM.keys o)
    elems = map (map snd . sort . HM.toList) os
    size  = length o
    colout = replicate size
      $ Table.column Table.expand Table.left Table.noAlign (Table.singleCutMark "...")

renderContents :: Width -> [A.Value] -> [[String]]
renderContents width = map $ concatMap (Table.justify width . words') . lines . formatJSONValue

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

--------------------------------------------------------------------------------

simpleShowTable :: [(String, Int, Table.Position Table.H)] -> [[String]] -> String
simpleShowTable _ [] = ""
simpleShowTable colconfs rols =
  let titles = map (\(t, _, _) -> t) colconfs
      colout = map (\(_, maxlen, pos) -> Table.column (Table.expandUntil maxlen) pos def def) colconfs
   in Table.tableString colout
                        Table.asciiS
                        (Table.titlesH titles)
                        [ Table.rowsG rols ]
