{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}

module HStream.Server.Handler.Admin (adminCommandHandler) where

import           Control.Monad                    (forM, void)
import           Data.Aeson                       ((.=))
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import           Data.Map.Strict                  (Map)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import qualified Data.Text.Lazy                   as TL
import qualified Data.Text.Lazy.Encoding          as TL
import qualified Data.Vector                      as V
import qualified GHC.IO.Exception                 as E
import           Network.GRPC.HighLevel.Generated
import qualified Options.Applicative              as O
import qualified Options.Applicative.Help         as O
import           Proto3.Suite                     (HasDefault (def))
import qualified Z.Data.CBytes                    as CB
import           Z.Data.CBytes                    (CBytes)

import qualified HStream.Admin.Types              as Admin
import qualified HStream.Logger                   as Log
import qualified HStream.Server.Core.Stream       as HC
import qualified HStream.Server.Core.Subscription as HC
import qualified HStream.Server.Core.View         as HC
import           HStream.Server.Exception         (defaultExceptionHandle)
import qualified HStream.Server.HStreamApi        as API
import           HStream.Server.Persistence       (getClusterStatus)
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import           HStream.Utils                    (Interval, formatStatus,
                                                   interval2ms, returnResp,
                                                   showNodeStatus)
import qualified HStream.Admin.Server.Types as AT
import qualified HStream.IO.Worker as IO
import qualified Data.Aeson.Text as Aeson

-------------------------------------------------------------------------------
-- All command line data types are defined in 'HStream.Admin.Types'

-- we only need the 'Command' in 'Cli'
parseAdminCommand :: [String] -> IO AT.AdminCommand
parseAdminCommand args = extractAdminCmd =<< execParser
  where
    extractAdminCmd (Admin.ServerCli AT.Cli{command = AT.ServerAdminCmd cmd}) = return cmd
    extractAdminCmd _ = throwParsingErr "Only admin commands are accepted"
    execParser = handleParseResult $ O.execParserPure O.defaultPrefs cliInfo args
    cliInfo = O.info Admin.cliParser (O.progDesc "The parser to use for admin commands")

adminCommandHandler
  :: ServerContext
  -> ServerRequest 'Normal API.AdminCommandRequest API.AdminCommandResponse
  -> IO (ServerResponse 'Normal API.AdminCommandResponse)
adminCommandHandler sc@ServerContext{..} req = defaultExceptionHandle $ do
  let (ServerNormalRequest _ (API.AdminCommandRequest cmd)) = req
  Log.info $ "Receive amdin command: " <> Log.buildText cmd

  let args = words (Text.unpack cmd)
  adminCommand <- parseAdminCommand args
  result <- case adminCommand of
              AT.AdminStatsCommand c        -> runStats scStatsHolder c
              AT.AdminResetStatsCommand     -> runResetStats scStatsHolder
              AT.AdminStreamCommand c       -> runStream sc c
              AT.AdminSubscriptionCommand c -> runSubscription sc c
              AT.AdminViewCommand c         -> runView sc c
              AT.AdminStatusCommand         -> runStatus sc
              AT.AdminIOCommand c           -> runIO sc c
  returnResp $ API.AdminCommandResponse {adminCommandResponseResult = result}

handleParseResult :: O.ParserResult a -> IO a
handleParseResult (O.Success a) = return a
handleParseResult (O.Failure failure) = do
  let (h, _exit, _cols) = O.execFailure failure ""
      errmsg = (O.displayS . O.renderCompact . O.extractChunk $ O.helpError h) ""
  throwParsingErr errmsg
handleParseResult (O.CompletionInvoked compl) = throwParsingErr =<< O.execCompletion compl ""

-------------------------------------------------------------------------------
-- Admin Stats Command

runStats :: Stats.StatsHolder -> AT.StatsCommand -> IO Text
runStats statsHolder AT.StatsCommand{..} = do
  case statsCategory of
    AT.PerStreamStats -> doPerStreamStats statsName
    AT.PerStreamTimeSeries -> doPerStreamTimeSeries statsName statsIntervals
    AT.PerSubscriptionStats -> doPerSubscriptionStats statsName
    AT.PerSubscriptionTimeSeries -> doPerSubscriptionTimeSeries statsName statsIntervals
    AT.ServerHistogram -> doServerHistogram statsName
  where
    doPerStreamStats name = do
      m <- Stats.stream_stat_getall statsHolder name
      let headers = ["stream_name", name]
          rows = Map.foldMapWithKey (\k v -> [[CB.unpack k, show v]]) m
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ tableResponse content

    doPerStreamTimeSeries :: CBytes -> [Interval] -> IO Text
    doPerStreamTimeSeries name intervals =
      let cfun = Stats.stream_time_series_getall_by_name' statsHolder
       in doTimeSeries name "stream_name" intervals cfun

    doPerSubscriptionStats name = do
      m <- Stats.subscription_stat_getall statsHolder name
      let headers = ["subscription_id", name]
          rows = Map.foldMapWithKey (\k v -> [[CB.unpack k, show v]]) m
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ tableResponse content

    doPerSubscriptionTimeSeries :: CBytes -> [Interval] -> IO Text
    doPerSubscriptionTimeSeries name intervals =
      let cfun = Stats.subscription_time_series_getall_by_name' statsHolder
       in doTimeSeries name "subscription_id" intervals cfun

    doServerHistogram name = do
      let strName = CB.unpack name
      ps <- Stats.serverHistogramEstimatePercentiles
              statsHolder (read strName) statsPercentiles
      -- 0.5 -> p50
      let headers = map (("p" ++) . show @Int . floor . (*100)) statsPercentiles
          rows = [map show ps]
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ tableResponse content

doTimeSeries :: CBytes
             -> CBytes
             -> [Interval]
             -> (CBytes -> [Int] -> IO (Either String (Map CBytes [Double])))
             -> IO Text
doTimeSeries stat_name x intervals f = do
  m <- f stat_name (map interval2ms intervals)
  case m of
    Left errmsg -> return $ errorResponse $ Text.pack errmsg
    Right stats -> do
      let headers = x : (((stat_name <> "_") <>) . CB.pack . show <$> intervals)
          rows = Map.foldMapWithKey (\k vs -> [CB.unpack k : (show @Int . floor <$> vs)]) stats
          content = Aeson.object ["headers" .= headers, "rows" .= rows]
      return $ tableResponse content

runResetStats :: Stats.StatsHolder -> IO Text
runResetStats stats_holder = do
  Stats.resetStatsHolder stats_holder
  return $ plainResponse "OK"

-------------------------------------------------------------------------------
-- Admin Stream Command

runStream :: ServerContext -> AT.StreamCommand -> IO Text
runStream ctx AT.StreamCmdList = do
  let headers = ["name" :: Text, "replication_property"]
  streams <- HC.listStreams ctx API.ListStreamsRequest
  rows <- V.forM streams $ \stream -> do
    return [ API.streamStreamName stream
           , "node:" <> Text.pack (show $ API.streamReplicationFactor stream)
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content
runStream ctx (AT.StreamCmdCreate stream) = do
  void $ HC.createStream ctx stream
  return $ plainResponse "OK"
runStream ctx (AT.StreamCmdDelete stream force) = do
  void $ HC.deleteStream ctx def { API.deleteStreamRequestStreamName = stream
                                 , API.deleteStreamRequestForce = force
                                 }
  return $ plainResponse "OK"

-------------------------------------------------------------------------------
-- Subscription Command

runSubscription :: ServerContext -> AT.SubscriptionCommand -> IO Text
runSubscription ctx AT.SubscriptionCmdList = do
  let headers = ["id" :: Text, "stream_name", "timeout"]
  subs <- HC.listSubscriptions ctx
  rows <- V.forM subs $ \sub -> do
    return [ API.subscriptionSubscriptionId sub
           , API.subscriptionStreamName sub
           , Text.pack . show $ API.subscriptionAckTimeoutSeconds sub
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content
runSubscription ctx (AT.SubscriptionCmdDelete subscription force) = do
  HC.deleteSubscription ctx subscription force
  return $ plainResponse "OK"
runSubscription ctx (AT.SubscriptionCmdCreate sub) = do
  HC.createSubscription ctx sub
  return $ plainResponse "OK"

-------------------------------------------------------------------------------
-- Admin View Command

runView :: ServerContext -> AT.ViewCommand -> IO Text
runView serverContext AT.ViewCmdList = do
  let headers = ["id" :: Text, "status", "createdTime"]
  views <- HC.listViews serverContext
  rows <- forM views $ \view -> do
    return [ API.viewViewId view
           , Text.pack . formatStatus . API.viewStatus $ view
           , Text.pack . show . API.viewCreatedTime $ view
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content

-------------------------------------------------------------------------------
-- Admin IO Command
runIO :: ServerContext -> AT.IOCommand -> IO Text
runIO ServerContext{..} (AT.IOCmdCreate dir) = do
  plainResponse <$> IO.createIOTaskFromDir scIOWorker dir
runIO ServerContext{..} (AT.IOCmdStart taskId) = do
  IO.startIOTask scIOWorker taskId
  return $ plainResponse "OK"
runIO ServerContext{..} (AT.IOCmdStop taskId) = do
  IO.stopIOTask scIOWorker taskId
  return $ plainResponse "OK"
runIO ServerContext {..} AT.IOCmdList = do
  let headers = ["id" :: Text, "status", "latest_state"]
  items <- IO.listIOTasks scIOWorker
  rows <- forM items $ \IO.IOTaskItem {..} ->
    return
      [ taskId,
        Text.pack $ show status,
        TL.toStrict $ Aeson.encodeToLazyText latestState
      ]
  return . tableResponse $ Aeson.object ["headers" .= headers, "rows" .= rows]

-------------------------------------------------------------------------------
-- Admin Status Command

runStatus :: ServerContext -> IO Text.Text
runStatus ServerContext{..} = do
  values <- HM.elems <$> getClusterStatus zkHandle
  let headers = ["node_id" :: Text.Text, "state", "address"]
      rows = map consRow values
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content
  where
    show' = Text.pack . show
    consRow API.ServerNodeStatus{..} =
      let nodeID = maybe "UNKNOWN" (show' . API.serverNodeId) serverNodeStatusNode
          nodeHost = maybe "UNKNOWN" API.serverNodeHost serverNodeStatusNode
          nodePort = maybe "UNKNOWN" (show' . API.serverNodePort) serverNodeStatusNode
       in [ nodeID
          , showNodeStatus serverNodeStatusState
          , nodeHost <> ":" <> nodePort
          ]

-------------------------------------------------------------------------------
-- Helpers

tableResponse :: Aeson.Value -> Text
tableResponse = jsonEncode' . AT.AdminCommandResponse AT.CommandResponseTypeTable

plainResponse :: Text -> Text
plainResponse = jsonEncode' . AT.AdminCommandResponse AT.CommandResponseTypePlain

errorResponse :: Text -> Text
errorResponse = jsonEncode' . AT.AdminCommandResponse AT.CommandResponseTypeError

throwParsingErr :: String -> IO a
throwParsingErr = err' "Parsing admin command error"

err' :: String -> String -> IO a
err' errloc errmsg = E.ioError $ E.IOError Nothing E.InvalidArgument errloc errmsg Nothing Nothing

jsonEncode :: Aeson.Value -> Text
jsonEncode = TL.toStrict . TL.decodeUtf8 . Aeson.encode

jsonEncode' :: Aeson.ToJSON a => a -> Text
jsonEncode' = jsonEncode . Aeson.toJSON
