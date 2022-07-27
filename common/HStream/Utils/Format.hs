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

import qualified Data.Aeson                       as A
import qualified Data.Aeson.Text                  as A
import qualified Data.ByteString.Char8            as BS
import           Data.Default                     (def)
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
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
import           HStream.Utils.Converter          (structToJsonObject,
                                                   valueToJsonValue)
import           HStream.Utils.RPC                (showNodeStatus)

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
  formatResult = emptyNotice . renderStreamsToTable

instance Format [API.View] where
  formatResult = emptyNotice . renderViewsToTable

instance Format [API.Query] where
  formatResult = emptyNotice . renderQueriesToTable

instance Format [API.Connector] where
  formatResult = emptyNotice . renderConnectorsToTable

instance Format [API.ServerNode] where
  formatResult = emptyNotice . renderServerNodesToTable

instance Format [API.ServerNodeStatus] where
  formatResult = emptyNotice . renderServerNodesStatusToTable

instance Format a => Format (ClientResult 'Normal a) where
  formatResult (ClientNormalResponse response _ _ _ _) = formatResult response
  formatResult (ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _code details)))
    = "Client Error: " <> BS.unpack (unStatusDetails details) <> "\n"
  formatResult (ClientErrorResponse err) = "Error: " <> show err <> "\n"

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

instance Format API.TerminateQueriesResponse where
  formatResult = const "Done.\n"

instance Format P.Struct where
  formatResult (P.Struct kv) =
    case M.toList kv of
      [("SELECT",      Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("SELECTVIEW",  Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("Error Message:", Just v)] -> "Error Message: " ++ show v ++ "\n"
      x -> show x

instance Format API.CommandQueryResponse where
  formatResult = formatCommandQueryResponse

--------------------------------------------------------------------------------
emptyNotice :: String -> String
emptyNotice xs = if null (words xs) then "Done. No results.\n" else xs

formatCommandQueryResponse :: API.CommandQueryResponse -> String
formatCommandQueryResponse (API.CommandQueryResponse x) = case V.toList x of
  []  -> "Done. \n"
  [y] -> formatResult y
  ys  -> L.concatMap formatResult ys

renderQueriesToTable :: [API.Query] -> String
renderQueriesToTable [] = ""
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
renderConnectorsToTable [] = ""
renderConnectorsToTable connectors =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = [ "Name"
             , "Status"
             ]
    formatRow API.Connector {connectorInfo=Just info} =
      [ [toString $ infoJson HM.! "name"]
      , [toString $ infoJson HM.! "status"]
      ]
      where infoJson = structToJsonObject info
            toString (A.String v) = T.unpack v
    rows = map formatRow connectors
    colSpec = [ Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              ]

renderStreamsToTable :: [API.Stream] -> String
renderStreamsToTable [] = ""
renderStreamsToTable streams =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = [ "Stream Name"
             , "Replica"
             , "Retention Time"
             , "Shard Count"]
    formatRow API.Stream {..} =
      [ [T.unpack streamStreamName]
      , [show streamReplicationFactor]
      , [show streamBacklogDuration <> "sec"]
      , [show streamShardCount]
      ]
    rows = map formatRow streams
    colSpec = [ Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              ]

renderViewsToTable :: [API.View] -> String
renderViewsToTable [] = ""
renderViewsToTable views =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = [ "View Name"
             , "Status"
             , "Created Time"
             , "Schema"]
    formatRow API.View {..} =
      [ [T.unpack viewViewId]
      , [formatStatus viewStatus]
      , [CB.unpack $ formatSystemTimeGMT iso8061DateFormat (MkSystemTime viewCreatedTime 0)]
      , [show viewSchema]
      ]
    rows = map formatRow views
    colSpec = [ Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              , Table.column Table.expand Table.left def def
              ]

renderServerNodesToTable :: [API.ServerNode] -> String
renderServerNodesToTable values =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = ["Server Id"]
    formatRow API.ServerNode {..} = [[show serverNodeId]]
    rows = map formatRow values
    colSpec = [ Table.column Table.expand Table.left def def]

renderServerNodesStatusToTable :: [API.ServerNodeStatus] -> String
renderServerNodesStatusToTable values =
  Table.tableString colSpec Table.asciiS
    (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
    (Table.colsAllG Table.center <$> rows) ++ "\n"
  where
    titles = ["Server Id", "State", "Address"]
    formatRow API.ServerNodeStatus {serverNodeStatusNode = Just API.ServerNode{..}, ..} =
      [[show serverNodeId], [showNodeStatus serverNodeStatusState], [show serverNodeHost <> ":" <> show serverNodePort]]
    formatRow API.ServerNodeStatus {serverNodeStatusNode = Nothing} = []
    rows = map formatRow . L.sort $ values
    colSpec = replicate (length titles) $ Table.column Table.expand Table.left def def

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
