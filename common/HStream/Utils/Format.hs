{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils.Format
  ( Format (..)
  , formatCommandQueryResponse
  , approxNaturalTime
  , formatStatus
  ) where

import qualified Data.Aeson.Text                  as A
import qualified Data.ByteString.Char8            as BS
import           Data.Default                     (def)
import qualified Data.Map.Strict                  as M
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Data.Time.Clock                  (NominalDiffTime)
import qualified Data.Vector                      as V
import qualified Google.Protobuf.Empty            as Protobuf
import qualified Google.Protobuf.Struct           as P
import           Network.GRPC.HighLevel.Generated
import qualified Proto3.Suite                     as PB
import qualified Text.Layout.Table                as Table
import qualified Z.Data.CBytes                    as CB
import           Z.IO.Time                        (SystemTime (MkSystemTime),
                                                   formatSystemTimeGMT,
                                                   iso8061DateFormat)

import qualified HStream.Server.HStreamApi        as API
import           HStream.Utils.Converter          (valueToJsonValue)

--------------------------------------------------------------------------------

class Format a where
  formatResult ::a -> String

instance Format Protobuf.Empty where
  formatResult = const "Done.\n"

instance Format API.Stream where
  formatResult = (<> "\n") . T.unpack . API.streamStreamName

instance Format API.View where
  formatResult = show . API.viewViewId

instance Format [API.Stream] where
  formatResult = emptyNotice . concatMap formatResult

instance Format [API.View] where
  formatResult = emptyNotice . unlines . map formatResult

instance Format [API.Query] where
  formatResult = emptyNotice . renderQueriesToTable

instance Format [API.Connector] where
  formatResult = emptyNotice . renderConnectorsToTable

instance Format a => Format (ClientResult 'Normal a) where
  formatResult (ClientNormalResponse response _ _ _ _) = formatResult response
  formatResult (ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode StatusInternal details)))
    = "Error: " <> BS.unpack (unStatusDetails details) <> "\n"
  formatResult (ClientErrorResponse err) = "Server Error: " <> show err <> "\n"

instance Format API.ListStreamsResponse where
  formatResult = formatResult . V.toList . API.listStreamsResponseStreams
instance Format API.ListViewsResponse where
  formatResult = formatResult . V.toList . API.listViewsResponseViews
instance Format API.ListQueriesResponse where
  formatResult = formatResult . V.toList . API.listQueriesResponseQueries
instance Format API.ListConnectorsResponse where
  formatResult = formatResult . V.toList . API.listConnectorsResponseConnectors

instance Format API.AppendResponse where
  formatResult = const "Done.\n"

instance Format API.CreateQueryStreamResponse where
  formatResult = const "Done.\n"

instance Format API.TerminateQueriesResponse where
  formatResult = const "Done.\n"

instance Format P.Struct where
  formatResult (P.Struct kv) =
    case M.toList kv of
      [("SELECT",      Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("SELECTVIEW",  Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("Error Message:", Just v)] -> "Error Message: " ++ show v ++ "\n"
      x -> show x

--------------------------------------------------------------------------------
emptyNotice :: String -> String
emptyNotice xs = if null (words xs) then "Succeeded. No results.\n" else xs

formatCommandQueryResponse :: API.CommandQueryResponse -> String
formatCommandQueryResponse (API.CommandQueryResponse x) = case V.toList x of
  []  -> "Succeeded. No results.\n"
  [y] -> formatResult y
  ys  -> "unknown behaviour" <> show ys

renderQueriesToTable :: [API.Query] -> String
renderQueriesToTable queries =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = [ "Query ID"
             , "Status"
             , "Created Time"
             , "SQL Text"
             ]
    formatRow API.Query {..} =
      [ [T.unpack queryId]
      , [formatStatus queryStatus]
      , [CB.unpack $ formatSystemTimeGMT iso8061DateFormat (MkSystemTime queryCreatedTime 0)]
      , [T.unpack queryQueryText]
      ]
    rows = map formatRow queries
    colSpec = [ Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              ]

renderConnectorsToTable :: [API.Connector] -> String
renderConnectorsToTable connectors =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = [ "Connector ID"
             , "Status"
             , "Created Time"
             , "SQL Text"]
    formatRow API.Connector {..} =
      [ [T.unpack connectorName]
      , [T.unpack connectorStatus]
      -- , [CB.unpack $ formatSystemTimeGMT iso8061DateFormat (MkSystemTime connectorCreatedTime 0)]
      -- , [T.unpack connectorSql]
      ]
    rows = map formatRow connectors
    colSpec = [ Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              ]

approxNaturalTime :: NominalDiffTime -> String
approxNaturalTime n
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

formatStatus ::  PB.Enumerated API.TaskStatusPB -> String
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_RUNNING)) = "RUNNING"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_TERMINATED)) = "TERMINATED"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_CONNECTION_ABORT)) = "CONNECTION_ABORT"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_CREATING)) = "CREATING"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_CREATED)) = "CREATED"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_CREATION_ABORT)) = "CREATION_ABORT"
formatStatus _ = "Unknown Status"
