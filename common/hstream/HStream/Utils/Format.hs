{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.Utils.Format
  ( Format (..)
  , formatCommandQueryResponse
  , formatStatus
  ) where

import qualified Data.Aeson                       as A
import qualified Data.Aeson.Text                  as A
import qualified Data.ByteString.Char8            as BS
import           Data.Default                     (def)
import           Data.Int                         (Int64)
import qualified Data.List                        as L
import qualified Data.Map.Strict                  as M
import qualified Data.Text                        as T
import qualified Data.Text.Encoding               as T
import qualified Data.Text.Lazy                   as TL
import           Data.Time.Clock.System           (SystemTime (MkSystemTime))
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Proto3.Suite                     as PB
import qualified Text.Layout.Table                as Table
import qualified Z.Data.CBytes                    as CB

import           Data.Maybe                       (maybeToList)

import           HStream.Base.Time                (CTime (CTime),
                                                   UnixTime (UnixTime),
                                                   formatUnixTimeGMT,
                                                   iso8061DateFormat)
import qualified HStream.Server.HStreamApi        as API
import qualified HStream.ThirdParty.Protobuf      as PB
import qualified HStream.Utils.Aeson              as A
import           HStream.Utils.Converter          (structToJsonObject,
                                                   valueToJsonValue)
import           HStream.Utils.RPC                (showNodeStatus)

--------------------------------------------------------------------------------

class Format a where
  formatResult ::a -> String

instance Format PB.Empty where
  formatResult = const "Done.\n"

instance Format () where
  formatResult = const ""

instance Format API.Stream where
  formatResult = renderStreamsToTable . (:[])

instance Format API.View where
  formatResult = show . API.viewViewId

instance Format [API.Stream] where
  formatResult = renderStreamsToTable

instance Format [API.View] where
  formatResult = renderViewsToTable

instance Format API.Query where
  formatResult = renderQueriesToTable . (:[])

instance Format API.Connector where
  formatResult = renderConnectorsToTable . (:[])

instance Format API.Subscription where
  formatResult = renderSubscriptionsToTable . (:[])

instance Format [API.Query] where
  formatResult = renderQueriesToTable

instance Format [API.Connector] where
  formatResult = renderConnectorsToTable

instance Format [API.Subscription] where
  formatResult = renderSubscriptionsToTable

instance Format [API.ServerNode] where
  formatResult = renderServerNodesToTable

instance Format [API.ServerNodeStatus] where
  formatResult = renderServerNodesStatusToTable

instance Format a => Format (ClientResult 'Normal a) where
  formatResult (ClientNormalResponse response _ _ _ _) = formatResult response
  formatResult (ClientErrorResponse (ClientIOError (GRPCIOBadStatusCode _code details)))
    = "Server Error: " <> BS.unpack (unStatusDetails details) <> "\n"
  formatResult (ClientErrorResponse err) = "Error: " <> show err <> "\n"

instance Format API.ListStreamsResponse where
  formatResult = formatResult . V.toList . API.listStreamsResponseStreams
instance Format API.ListViewsResponse where
  formatResult = formatResult . V.toList . API.listViewsResponseViews
instance Format API.ListQueriesResponse where
  formatResult = formatResult . V.toList . API.listQueriesResponseQueries
instance Format API.ListConnectorsResponse where
  formatResult = formatResult . V.toList . API.listConnectorsResponseConnectors
instance Format API.ListSubscriptionsResponse where
  formatResult = formatResult . V.toList . API.listSubscriptionsResponseSubscription

instance Format API.GetStreamResponse where
  formatResult = formatResult . maybeToList . API.getStreamResponseStream
instance Format API.GetSubscriptionResponse where
  formatResult = formatResult . maybeToList . API.getSubscriptionResponseSubscription

instance Format API.AppendResponse where
  formatResult = const "Done.\n"

instance Format API.ExecuteViewQueryResponse where
  formatResult = concatMap ((<> "\n") . TL.unpack . A.encodeToLazyText . structToJsonObject ) . API.executeViewQueryResponseResults

instance Format PB.Struct where
  formatResult s@(PB.Struct kv) =
    case M.toList kv of
      [("SELECT",      Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("SELECTVIEW",  Just x)] -> (<> "\n") . TL.unpack . A.encodeToLazyText . valueToJsonValue $ x
      [("stream_query_id", Just x)] -> let (A.String qid) = valueToJsonValue x
                                        in "Done. Query ID: " <> T.unpack qid <> "\n"
      [("view_query_id", Just x)]   -> let (A.String qid) = valueToJsonValue x
                                        in "Done. Query ID: " <> T.unpack qid <> "\n"
      [("Error Message:", Just v)] -> "Error Message: " ++ show v ++ "\n"
      [("explain", Just plan)] -> let (A.String text) = valueToJsonValue plan
                                   in "-------- RAW PLAN --------\n" <> T.unpack text <> "\n"
      _ -> (<> "\n") . TL.unpack . A.encodeToLazyText . structToJsonObject $ s

instance Format API.CommandQueryResponse where
  formatResult = formatCommandQueryResponse

--------------------------------------------------------------------------------

formatCommandQueryResponse :: API.CommandQueryResponse -> String
formatCommandQueryResponse (API.CommandQueryResponse x) = case V.toList x of
  []  -> "Done. \n"
  [y] -> formatResult y
  ys  -> L.concatMap formatResult ys

renderQueriesToTable :: [API.Query] -> String
renderQueriesToTable queries = showTable titles rows
  where
    titles = ["Query ID", "Status", "Created Time", "SQL Text"]
    formatRow API.Query {..} =
      [ [T.unpack queryId]
      , [formatStatus queryStatus]
      , [formatTime queryCreatedTime]
      , [T.unpack queryQueryText]
      ]
    rows = map formatRow queries

renderSubscriptionsToTable :: [API.Subscription] -> String
renderSubscriptionsToTable subscriptions = showTable titles rows
  where
    titles = [ "Subscription ID"
             , "Stream Name"
             , "Ack Timeout"
             , "Max Unacked Records"
             ]
    formatRow API.Subscription {..} =
      [ [T.unpack subscriptionSubscriptionId]
      , [T.unpack subscriptionStreamName]
      , [show subscriptionAckTimeoutSeconds <> " seconds"]
      , [show subscriptionMaxUnackedRecords]
      ]
    rows = map formatRow subscriptions

renderConnectorsToTable :: [API.Connector] -> String
renderConnectorsToTable connectors = showTable titles rows
  where
    titles = ["Name", "Status"]
    formatRow API.Connector {..} =
      [ [T.unpack connectorName]
      , [T.unpack connectorStatus]
      ]
    rows = map formatRow connectors

renderStreamsToTable :: [API.Stream] -> String
renderStreamsToTable streams = showTable titles rows
  where
    titles = [ "Stream Name"
             , "Replica"
             , "Retention Time"
             , "Shard Count"]
    formatRow API.Stream {..} =
      [ [T.unpack streamStreamName]
      , [show streamReplicationFactor]
      , [show streamBacklogDuration <> " seconds"]
      , [show streamShardCount]
      ]
    rows = map formatRow streams

renderViewsToTable :: [API.View] -> String
renderViewsToTable views = showTable titles rows
  where
    titles = [ "View Name"
             , "Status"
             , "Created Time"
             , "Schema"
             , "Query Name"]
    formatRow API.View {..} =
      [ [T.unpack viewViewId]
      , [formatStatus viewStatus]
      , [formatTime viewCreatedTime]
      , [show viewSchema]
      , [T.unpack viewQueryName]
      ]
    rows = map formatRow views

renderServerNodesToTable :: [API.ServerNode] -> String
renderServerNodesToTable values = showTable titles rows
  where
    titles = ["Server Id"]
    formatRow API.ServerNode {..} = [[show serverNodeId]]
    rows = map formatRow values

renderServerNodesStatusToTable :: [API.ServerNodeStatus] -> String
renderServerNodesStatusToTable values = showTable titles rows
  where
    titles = ["Server Id", "State", "Address"]
    formatRow API.ServerNodeStatus {serverNodeStatusNode = Just API.ServerNode{..}, ..} =
      [[show serverNodeId], [showNodeStatus serverNodeStatusState], [T.unpack serverNodeHost <> ":" <> show serverNodePort]]
    formatRow API.ServerNodeStatus {serverNodeStatusNode = Nothing} = []
    rows = map formatRow . L.sort $ values

showTable :: [String] -> [[[String]]] -> String
showTable titles rows = Table.tableString t ++ "\n"
  where
    t =
      case rows of
        [] -> Table.headerlessTableS colSpec Table.asciiS (Table.colsAllG Table.center <$> [map (:[]) titles])
        _ -> Table.columnHeaderTableS
          colSpec
          Table.asciiS
          (Table.fullH (repeat $ Table.headerColumn Table.left Nothing) titles)
          (Table.colsAllG Table.center <$> rows)
    colSpec = map (const $ Table.column Table.expand Table.left def def) titles

formatStatus ::  PB.Enumerated API.TaskStatusPB -> String
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_RUNNING)) = "RUNNING"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_ABORTED)) = "ABORTED"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_CREATING)) = "CREATING"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_PAUSED)) = "PAUSED"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_RESUMING)) = "RESUMING"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_TERMINATED)) = "TERMINATED"
formatStatus (PB.Enumerated (Right API.TaskStatusPBTASK_UNKNOWN)) = "UNKNOWN"
formatStatus _ = "Unknown Status"

formatTime :: Int64 -> String
formatTime t = T.unpack . T.decodeUtf8 $
  formatUnixTimeGMT iso8061DateFormat (UnixTime (CTime t) 0)
