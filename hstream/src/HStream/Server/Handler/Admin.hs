{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}
{-# OPTIONS_GHC -Werror=incomplete-patterns #-}

module HStream.Server.Handler.Admin
  ( -- * For grpc-haskell
    adminCommandHandler
    -- * For hs-grpc-server
  , handleAdminCommand
  ) where

import           Control.Concurrent               (readMVar, tryReadMVar)
import           Control.Concurrent.STM.TVar      (readTVarIO)
import           Control.Monad                    (forM, void)
import           Data.Aeson                       ((.=))
import qualified Data.Aeson                       as Aeson
import qualified Data.Aeson.Text                  as Aeson
import qualified Data.HashMap.Strict              as HM
import qualified Data.List                        as L
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import qualified Data.Text.Lazy                   as TL
import qualified Data.Text.Lazy.Encoding          as TL
import qualified Data.Vector                      as V
import           GHC.Conc                         (threadStatus)
import qualified GHC.IO.Exception                 as E
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated
import qualified Options.Applicative              as O
import qualified Options.Applicative.Help         as O
import           Proto3.Suite                     (Enumerated (Enumerated),
                                                   HasDefault (def))
import qualified Z.Data.CBytes                    as CB
import           Z.Data.CBytes                    (CBytes)

import           Control.Exception                (throw)
import qualified HStream.Admin.Server.Types       as AT
import           HStream.Base                     (rmTrailingZeros)
import qualified HStream.Exception                as HE
import           HStream.Gossip                   (GossipContext (clusterReady),
                                                   getClusterStatus,
                                                   initCluster)
import qualified HStream.IO.Worker                as HC
import qualified HStream.Logger                   as Log
import qualified HStream.MetaStore.Types          as M
import           HStream.Server.Config            (MetaStoreAddr (..),
                                                   ServerOpts (ServerOpts, _metaStore))
import           HStream.Server.Core.Common       (lookupResource,
                                                   mkAllocationKey)
import qualified HStream.Server.Core.Stream       as HC
import qualified HStream.Server.Core.Subscription as HC
#ifdef HStreamEnableSchema
import qualified HStream.Server.Core.QueryNew     as HC
import qualified HStream.Server.Core.ViewNew      as HC
#else
import qualified HStream.Server.Core.Query        as HC
import qualified HStream.Server.Core.View         as HC
#endif
import           HStream.Common.Server.MetaData   (TaskAllocation,
                                                   renderTaskAllocationsToTable)
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.MetaData          (QVRelation, QueryInfo,
                                                   QueryStatus, ViewInfo,
                                                   renderQVRelationToTable,
                                                   renderQueryInfosToTable,
                                                   renderQueryStatusToTable,
                                                   renderViewInfosToTable)
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import           HStream.Utils                    (Interval (..), cBytesToText,
                                                   formatQueryType,
                                                   formatStatus, interval2ms,
                                                   returnResp, showNodeStatus,
                                                   structToJsonObject,
                                                   timestampToMsTimestamp)

-------------------------------------------------------------------------------
-- All command line data types are defined in 'HStream.Admin.Types'

cliParser :: O.Parser AT.Cli
cliParser = O.hsubparser $
  O.command "server" (O.info AT.cliParser (O.progDesc "Admin command"))

adminCommandHandler
  :: ServerContext
  -> ServerRequest 'Normal API.AdminCommandRequest API.AdminCommandResponse
  -> IO (ServerResponse 'Normal API.AdminCommandResponse)
adminCommandHandler sc req = defaultExceptionHandle $ do
  let (ServerNormalRequest _ (API.AdminCommandRequest cmd)) = req
  result <- runAdminCommand sc cmd
  returnResp $ API.AdminCommandResponse {adminCommandResponseResult = result}

handleAdminCommand
  :: ServerContext -> G.UnaryHandler API.AdminCommandRequest API.AdminCommandResponse
handleAdminCommand sc _ (API.AdminCommandRequest cmd) = catchDefaultEx $ do
  Log.debug $ "Receive amdin command: " <> Log.build cmd
  result <- runAdminCommand sc cmd
  Log.trace $ "Admin command result: " <> Log.build result
  pure $ API.AdminCommandResponse {adminCommandResponseResult = result}

-------------------------------------------------------------------------------

-- we only need the 'Command' in 'Cli'
parseAdminCommand :: [String] -> IO AT.AdminCommand
parseAdminCommand args = extractAdminCmd =<< execParser
  where
    extractAdminCmd AT.Cli{command = AT.ServerAdminCmd cmd} = return cmd
    extractAdminCmd _ = AT.throwParsingErr "Only admin commands are accepted"
    execParser = handleParseResult $ O.execParserPure O.defaultPrefs cliInfo args
    cliInfo = O.info cliParser (O.progDesc "The parser to use for admin commands")

runAdminCommand :: ServerContext -> Text -> IO Text
runAdminCommand sc@ServerContext{..} cmd = do
  let args = words (Text.unpack cmd)
  adminCommand <- parseAdminCommand args
  case adminCommand of
    AT.AdminStatsCommand c        -> runStats scStatsHolder c
    AT.AdminResetStatsCommand     -> runResetStats scStatsHolder
    AT.AdminStreamCommand c       -> runStream sc c
    AT.AdminSubscriptionCommand c -> runSubscription sc c
    AT.AdminViewCommand c         -> runView sc c
    AT.AdminQueryCommand c        -> runQuery sc c
    AT.AdminStatusCommand         -> runStatus sc
    AT.AdminInitCommand           -> runInit sc
    AT.AdminCheckReadyCommand     -> runCheckReady sc
    AT.AdminLookupCommand c       -> runLookup sc c
    AT.AdminConnectorCommand c    -> runConnector sc c
    AT.AdminMetaCommand c         -> runMeta sc c

handleParseResult :: O.ParserResult a -> IO a
handleParseResult (O.Success a) = return a
handleParseResult (O.Failure failure) = do
  let (h, _exit, _cols) = O.execFailure failure ""
      errmsg = (O.displayS . O.renderCompact . O.extractChunk $ O.helpError h) ""
  AT.throwParsingErr errmsg
handleParseResult (O.CompletionInvoked compl) = AT.throwParsingErr =<< O.execCompletion compl ""

-------------------------------------------------------------------------------
-- Admin Stats Command

-- NOTE: the headers name must match defines in hstream-admin/server/cbits/query/tables
runStats :: Stats.StatsHolder -> AT.StatsCommand -> IO Text
runStats statsHolder AT.StatsCommand{..} = do
  case statsCategory of
    AT.PerStreamStats            -> doStats Stats.stream_stat_getall "stream_name"
    AT.PerStreamTimeSeries       -> doPerStreamTimeSeries statsName statsIntervals
    AT.PerSubscriptionStats      -> doStats Stats.subscription_stat_getall "subscription_id"
    AT.PerSubscriptionTimeSeries -> doPerSubscriptionTimeSeries statsName statsIntervals
    AT.PerHandleTimeSeries       -> doPerHandleTimeSeries statsName statsIntervals
    AT.PerConnectorStats         -> doStats Stats.connector_stat_getall "task_name"
    AT.PerQueryStats             -> doStats Stats.query_stat_getall "query_name"
    AT.PerViewStats              -> doStats Stats.view_stat_getall "view_name"
    AT.ServerHistogram           -> doServerHistogram statsName
  where
    doStats getstats label = do
      m <- getstats statsHolder statsName
      let headers = [label, statsName]
          rows = Map.foldMapWithKey (\k v -> [[CB.unpack k, show v]]) m
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ AT.tableResponse content

    doPerStreamTimeSeries :: CBytes -> [Interval] -> IO Text
    doPerStreamTimeSeries name intervals =
      let cfun = Stats.stream_time_series_getall statsHolder
       in doTimeSeries name "stream_name" intervals cfun

    doPerSubscriptionTimeSeries :: CBytes -> [Interval] -> IO Text
    doPerSubscriptionTimeSeries name intervals =
      let cfun = Stats.subscription_time_series_getall statsHolder
       in doTimeSeries name "subscription_id" intervals cfun

    doPerHandleTimeSeries :: CBytes -> [Interval] -> IO Text
    doPerHandleTimeSeries name intervals =
      let cfun = Stats.handle_time_series_getall statsHolder
       in doTimeSeries name "handle_name" intervals cfun

    doServerHistogram name = do
      let strName = CB.unpack name
      ps <- Stats.serverHistogramEstimatePercentiles
              statsHolder (read strName) statsPercentiles
      -- 0.5 -> p50
      let headers = map (("p" ++) . show @Int . floor . (*100)) statsPercentiles
          rows = [map show ps]
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ AT.tableResponse content

doTimeSeries
  :: CBytes
  -> CBytes
  -> [Interval]
  -> (CBytes -> [Int] -> IO (Either String (Map CBytes [Double])))
  -> IO Text
doTimeSeries stat_name x intervals f = do
  m <- f stat_name (map interval2ms intervals)
  case m of
    Left errmsg -> return $ AT.errorResponse $ Text.pack errmsg
    Right stats -> do
      let headers = x : (((stat_name <> "_") <>) . formatInterval <$> intervals)
          rows = Map.foldMapWithKey (\k vs -> [CB.unpack k : (show @Int . floor <$> vs)]) stats
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ AT.tableResponse content

formatInterval :: Interval -> CBytes
formatInterval (Milliseconds x) = CB.pack (rmTrailingZeros x) <> "ms"
formatInterval (Seconds x)      = CB.pack (rmTrailingZeros x) <> "s"
formatInterval (Minutes x)      = CB.pack (rmTrailingZeros x) <> "min"
formatInterval (Hours x)        = CB.pack (rmTrailingZeros x) <> "h"

runResetStats :: Stats.StatsHolder -> IO Text
runResetStats stats_holder = do
  Stats.resetStatsHolder stats_holder
  return $ AT.plainResponse "OK"

-------------------------------------------------------------------------------
-- Admin Lookup Command

runLookup :: ServerContext -> AT.LookupCommand -> IO Text
runLookup ctx (AT.LookupCommand resType rId) = do
  API.ServerNode{..} <- lookupResource ctx (getResType resType) rId
  let headers = ["Resource ID" :: Text, "Host", "Port"]
      rows = [[rId, serverNodeHost, Text.pack .show $ serverNodePort]]
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content

getResType :: Text -> API.ResourceType
getResType resType =
  case resType of
    "stream"       -> API.ResourceTypeResStream
    "subscription" -> API.ResourceTypeResSubscription
    "query"        -> API.ResourceTypeResQuery
    "view"         -> API.ResourceTypeResView
    "connector"    -> API.ResourceTypeResConnector
    "shard"        -> API.ResourceTypeResShard
    "shard-reader" -> API.ResourceTypeResShardReader
    x              -> throw $ HE.InvalidResourceType (show x)
-------------------------------------------------------------------------------
-- Admin Meta Command

runMeta :: ServerContext -> AT.MetaCommand -> IO Text
runMeta ServerContext{..} (AT.MetaCmdList resType) = do
  case resType of
    "subscription" -> pure <$> AT.tableResponse . renderSubscriptionWrapToTable  =<< M.listMeta @SubscriptionWrap metaHandle
    "query-info"   -> pure <$> AT.plainResponse . renderQueryInfosToTable =<< M.listMeta @QueryInfo metaHandle
    "view-info"    -> pure <$> AT.plainResponse . renderViewInfosToTable =<< M.listMeta @ViewInfo metaHandle
    "qv-relation"  -> pure <$> AT.tableResponse . renderQVRelationToTable =<< M.listMeta @QVRelation metaHandle
    _ -> return $ AT.errorResponse "invalid resource type, try [subscription|query-info|view-info|qv-relateion]"
runMeta ServerContext{..} (AT.MetaCmdGet resType rId) = do
  case resType of
    "subscription" -> pure <$> maybe (AT.plainResponse "Not Found") (AT.tableResponse . renderSubscriptionWrapToTable .L.singleton) =<< M.getMeta @SubscriptionWrap rId metaHandle
    "query-info"   -> pure <$> maybe (AT.plainResponse "Not Found") (AT.plainResponse . renderQueryInfosToTable . L.singleton) =<< M.getMeta @QueryInfo rId metaHandle
    "query-status" -> pure <$> maybe (AT.plainResponse "Not Found") (AT.tableResponse . renderQueryStatusToTable . L.singleton) =<< M.getMeta @QueryStatus rId metaHandle
    "view-info"    -> pure <$> maybe (AT.plainResponse "Not Found") (AT.plainResponse . renderViewInfosToTable . L.singleton) =<< M.getMeta @ViewInfo rId metaHandle
    "qv-relation"  -> pure <$> maybe (AT.plainResponse "Not Found") (AT.tableResponse . renderQVRelationToTable . L.singleton) =<< M.getMeta @QVRelation rId metaHandle
    _ -> return $ AT.errorResponse "invalid resource type, try [subscription|query-info|query-status|view-info|qv-relateion]"
runMeta ServerContext{serverOpts=ServerOpts{..}} AT.MetaCmdInfo = do
  let headers = ["Meta Type" :: Text, "Connection Info"]
      rows = case _metaStore of
               ZkAddr addr   -> [["zookeeper", cBytesToText addr]]
               RqAddr addr   -> [["rqlite", addr]]
               FileAddr addr -> [["file", Text.pack addr]]
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
runMeta sc (AT.MetaCmdTask taskCmd) = runMetaTask sc taskCmd

runMetaTask :: ServerContext -> AT.MetaTaskCommand -> IO Text
runMetaTask ServerContext{..} (AT.MetaTaskGet resType rId) = do
  let metaId = mkAllocationKey (getResType resType) rId
  pure <$> maybe (AT.plainResponse "Not Found") (AT.tableResponse . renderTaskAllocationsToTable . L.singleton) =<< M.getMeta @TaskAllocation metaId metaHandle

-------------------------------------------------------------------------------
-- Admin Stream Command

runStream :: ServerContext -> AT.StreamCommand -> IO Text
runStream ctx AT.StreamCmdList = do
  let headers = ["Stream Name" :: Text, "Replication Factor"]
  streams <- HC.listStreams ctx API.ListStreamsRequest
  rows <- V.forM streams $ \stream -> do
    return [ API.streamStreamName stream
           , Text.pack (show $ API.streamReplicationFactor stream)
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
runStream ctx (AT.StreamCmdCreate stream) = do
  void $ HC.createStream ctx stream
  return $ AT.plainResponse "OK"
runStream ctx (AT.StreamCmdDelete stream force) = do
  void $ HC.deleteStream ctx def { API.deleteStreamRequestStreamName = stream
                                 , API.deleteStreamRequestForce = force
                                 }
  return $ AT.plainResponse "OK"
runStream ctx (AT.StreamCmdDescribe sName) = do
  API.GetStreamResponse { getStreamResponseStream = Just stream}
    <- HC.getStream ctx (def {API.getStreamRequestName = sName})
  let headers = ["Stream Name" :: Text, "Replication Factor"]
      rows = [[API.streamStreamName stream, Text.pack (show $ API.streamReplicationFactor stream)]]
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content

-------------------------------------------------------------------------------
-- Subscription Command

runSubscription :: ServerContext -> AT.SubscriptionCommand -> IO Text
runSubscription ctx AT.SubscriptionCmdList = do
  let headers = ["Subscription ID" :: Text, "Stream Name", "CreatedTime"]
  subs <- HC.listSubscriptions ctx
  rows <- V.forM subs $ \sub -> do
    return [ API.subscriptionSubscriptionId sub
           , API.subscriptionStreamName sub
           , Text.pack (maybe "Unknown" (show . timestampToMsTimestamp) $ API.subscriptionCreationTime sub)
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
runSubscription ctx (AT.SubscriptionCmdDelete sid force) = do
  let req = API.DeleteSubscriptionRequest
            { deleteSubscriptionRequestSubscriptionId = sid
            , deleteSubscriptionRequestForce = force
            }
  HC.deleteSubscription ctx req
  return $ AT.plainResponse "OK"
runSubscription ctx (AT.SubscriptionCmdDeleteAll force) = do
  subs <- HC.listSubscriptions ctx
  V.forM_ subs $ \sub -> do
    let sid = API.subscriptionSubscriptionId sub
        req = API.DeleteSubscriptionRequest
              { deleteSubscriptionRequestSubscriptionId = sid
              , deleteSubscriptionRequestForce = force
              }
    HC.deleteSubscription ctx req
  return $ AT.plainResponse "OK"
runSubscription ctx (AT.SubscriptionCmdCreate sub) = do
  HC.createSubscription ctx sub
  return $ AT.plainResponse "OK"
runSubscription ctx (AT.SubscriptionCmdDescribe sid) = do
  API.GetSubscriptionResponse { getSubscriptionResponseSubscription = Just subscription}
    <- HC.getSubscription ctx (def { API.getSubscriptionRequestId = sid})
  let headers = ["Subscription ID" :: Text, "Stream Name", "Timeout", "Max Unacked", "CreatedTime", "Sub Offset"]
      rows = [[ API.subscriptionSubscriptionId subscription
              , API.subscriptionStreamName subscription
              , Text.pack (show $ API.subscriptionAckTimeoutSeconds subscription)
              , Text.pack (show $ API.subscriptionMaxUnackedRecords subscription)
              , Text.pack (maybe "Unknown" (show . timestampToMsTimestamp) $ API.subscriptionCreationTime subscription)
              , formatSpecialOffset $ API.subscriptionOffset subscription
             ]]
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
 where
   formatSpecialOffset offset = case offset of
      Enumerated (Right API.SpecialOffsetEARLIEST) -> "EARLIEST"
      Enumerated (Right API.SpecialOffsetLATEST)   -> "LATEST"
      _                                            -> "Unknown Offset"

-------------------------------------------------------------------------------
-- Admin View Command

runView :: ServerContext -> AT.ViewCommand -> IO Text
runView serverContext AT.ViewCmdList = do
  let headers = ["View ID" :: Text, "Status", "CreatedTime"]
  views <- HC.listViews serverContext
  rows <- forM views $ \view -> do
    return [ API.viewViewId view
           , Text.pack . formatStatus . API.viewStatus $ view
           , Text.pack . show . API.viewCreatedTime $ view
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content

-------------------------------------------------------------------------------
-- Admin Query Command

runQuery :: ServerContext -> AT.QueryCommand -> IO Text
runQuery ServerContext{..} (AT.QueryCmdStatus qid) = do
  queries <- readMVar runningQueries
  case HM.lookup qid queries of
    Nothing -> return $ AT.errorResponse $ "Query " <> Text.pack (show qid) <> " not found, or already dead"
    Just (tid,closed_m) -> do
      closed <- readTVarIO closed_m
      status <- threadStatus tid
      let headers = ["Query ID" :: Text, "Thread Status", "Consumer Status"]
          rows = [[qid, Text.pack (show status), if closed then "Closed" else "Running"]]
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ AT.tableResponse content
runQuery sc (AT.QueryCmdDescribe qid) = do
  query <- HC.getQuery sc qid
  case query of
    Nothing -> return $ AT.errorResponse $ "Query " <> Text.pack (show qid) <> " not found, or already dead"
    Just API.Query{..} -> do
      let headers = ["Query ID" :: Text, "Type", "Status", "Source ID", "Result ID", "CreatedTime", "Node", "SQL"]
          rows =
            [[ queryId
             , Text.pack $ formatQueryType queryType
             , Text.pack $ formatStatus queryStatus
             , V.foldl' (\acc x -> if Text.null acc then x else acc <> "," <> x) Text.empty querySources
             , queryResultName
             , Text.pack . show $ queryCreatedTime
             , Text.pack .show $ queryNodeId
             , queryQueryText
            ]]
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ AT.tableResponse content
runQuery sc (AT.QueryCmdResume qid) = do
  HC.resumeQuery sc qid
  query <- HC.getQuery sc qid
  case query of
    Nothing -> return $ AT.errorResponse $ "Query " <> Text.pack (show qid) <> " not found, or already dead"
    Just API.Query{..} -> do
      let headers = ["Query ID" :: Text, "Status", "Node"]
          rows =
            [[ queryId
             , Text.pack $ formatStatus queryStatus
             , Text.pack .show $ queryNodeId
            ]]
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ AT.tableResponse content
runQuery sc AT.QueryCmdList = do
  let headers = ["Query ID" :: Text, "Type", "Status", "CreatedTime", "SQL"]
  queries <- HC.listQueries sc
  rows <- forM queries $ \API.Query{..} -> do
    return [ queryId
           , Text.pack $ formatQueryType queryType
           , Text.pack $ formatStatus queryStatus
           , Text.pack . show $ queryCreatedTime
           , queryQueryText
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content

-------------------------------------------------------------------------------
-- Admin Connector Command

runConnector :: ServerContext -> AT.ConnectorCommand -> IO Text
runConnector ServerContext{..} AT.ConnectorCmdList = do
  connectors <- HC.listIOTasks scIOWorker
  let headers = ["Connector Name" :: Text, "Type", "Target", "Status", "CreatedTime"]
  rows <- forM connectors $ \API.Connector{..} -> do
    return [ connectorName
           , connectorType
           , connectorTarget
           , connectorStatus
           , maybe "unknown" (Text.pack . show . timestampToMsTimestamp) connectorCreationTime
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
runConnector ServerContext{..} (AT.ConnectorCmdRecover cId) = do
  HC.recoverTask scIOWorker cId
  API.Connector{..} <- HC.showIOTask_ scIOWorker cId
  let headers = ["Connector Name" :: Text, "Type", "Target", "Status", "Config"]
      rows = [[ connectorName
              , connectorType
              , connectorTarget
              , connectorStatus
              , connectorConfig
             ]]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
runConnector ServerContext{..} (AT.ConnectorCmdDescribe cId) = do
  API.Connector{..} <- HC.showIOTask_ scIOWorker cId
  let headers = ["Connector Name" :: Text, "Task ID", "Node", "Target", "Status", "Offsets", "Config", "Docker Image", "Docker Status"]
      offsets = V.map (Aeson.encodeToLazyText . structToJsonObject) connectorOffsets
      rows = [[ connectorName
              , connectorTaskId
              , connectorNode
              , connectorTarget
              , connectorStatus
              , TL.toStrict $ V.foldl' (\acc x -> if TL.null acc then x else acc <> "," <> x) TL.empty offsets
              , connectorConfig
              , connectorImage
              , connectorDockerStatus
             ]]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content

-------------------------------------------------------------------------------
-- Admin Status Command

runStatus :: ServerContext -> IO Text.Text
runStatus ServerContext{..} = do
  values <- HM.elems <$> getClusterStatus gossipContext
  let headers = ["Node ID" :: Text.Text, "State", "Address"]
      rows = map consRow values
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ AT.tableResponse content
  where
    show' = Text.pack . show
    consRow API.ServerNodeStatus{..} =
      let nodeID   = maybe "UNKNOWN" (show' . API.serverNodeId) serverNodeStatusNode
          nodeHost = maybe "UNKNOWN" API.serverNodeHost serverNodeStatusNode
          nodePort = maybe "UNKNOWN" (show' . API.serverNodePort) serverNodeStatusNode
       in [ nodeID
          , showNodeStatus serverNodeStatusState
          , nodeHost <> ":" <> nodePort
          ]

-------------------------------------------------------------------------------
-- Admin Init Command

runInit :: ServerContext -> IO Text.Text
runInit ServerContext{..} = do
  initCluster gossipContext
  return $ AT.plainResponse "Server successfully received init signal"

runCheckReady :: ServerContext -> IO Text.Text
runCheckReady ServerContext{..} = do
  tryReadMVar (clusterReady gossipContext) >>= \case
    Just _  -> return $ AT.plainResponse "Cluster is ready"
    Nothing -> return $ AT.errorResponse "Cluster is not ready!"
