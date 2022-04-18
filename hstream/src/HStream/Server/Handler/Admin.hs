{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}

module HStream.Server.Handler.Admin (adminCommandHandler) where

import           Control.Monad                    (forM)
import           Data.Aeson                       ((.=))
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Text.Lazy.Encoding          as TL
import qualified Data.Vector                      as V
import qualified GHC.IO.Exception                 as E
import           Network.GRPC.HighLevel.Generated
import qualified Options.Applicative              as O
import qualified Options.Applicative.Help         as O
import           Proto3.Suite                     (HasDefault (def))
import qualified Z.Data.CBytes                    as CB

import qualified HStream.Admin.Server.Types       as AT
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
import           HStream.Utils                    (formatStatus, interval2ms,
                                                   returnResp, showNodeStatus)

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
adminCommandHandler sc req = defaultExceptionHandle $ do
  let (ServerNormalRequest _ (API.AdminCommandRequest cmd)) = req
  Log.info $ "Receive amdin command: " <> Log.buildText cmd

  let args = words (T.unpack cmd)
  adminCommand <- parseAdminCommand args
  result <- case adminCommand of
              AT.AdminStatsCommand c        -> runStats sc c
              AT.AdminStreamCommand c       -> runStream sc c
              AT.AdminSubscriptionCommand c -> runSubscription sc c
              AT.AdminViewCommand c         -> runView sc c
              AT.AdminStatusCommand         -> runStatus sc
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

runStats :: ServerContext -> AT.StatsCommand -> IO Text
runStats ServerContext{..} AT.StatsCommand{..} = do
  let intervals = map interval2ms statsIntervals
  m <- Stats.stream_time_series_getall_by_name scStatsHolder statsType intervals
  let headers = "stream_name" : (("throughput_" <>) . T.pack . show <$> statsIntervals)
      rows = Map.foldMapWithKey (\k vs -> [CB.unpack k : (show @Int . floor <$> vs)]) m
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content

-------------------------------------------------------------------------------
-- Admin Stream Command

runStream :: ServerContext -> AT.StreamCommand -> IO Text
runStream ctx AT.StreamCmdList = do
  let headers = ["name" :: Text, "replication_property"]
  streams <- HC.listStreams ctx API.ListStreamsRequest
  rows <- V.forM streams $ \stream -> do
    return [ API.streamStreamName stream
           , "node:" <> T.pack (show $ API.streamReplicationFactor stream)
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content
runStream ctx (AT.StreamCmdCreate stream) = do
  HC.createStream ctx stream
  return $ plainResponse "OK"
runStream ctx (AT.StreamCmdDelete stream force) = do
  HC.deleteStream ctx def { API.deleteStreamRequestStreamName = stream
                          , API.deleteStreamRequestForce = force}
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
           , T.pack . show $ API.subscriptionAckTimeoutSeconds sub
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
           , T.pack . formatStatus . API.viewStatus $ view
           , T.pack . show . API.viewCreatedTime $ view
           ]
  let content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content

-------------------------------------------------------------------------------
-- Admin Status Command

runStatus :: ServerContext -> IO T.Text
runStatus ServerContext{..} = do
  values <- HM.elems <$> getClusterStatus zkHandle
  let headers = ["node_id" :: T.Text, "state", "address"]
      rows = map consRow values
      content = Aeson.object ["headers" .= headers, "rows" .= rows]
  return $ tableResponse content
  where
    show' = T.pack . show
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

throwParsingErr :: String -> IO a
throwParsingErr = err' "Parsing admin command error"

err' :: String -> String -> IO a
err' errloc errmsg = E.ioError $ E.IOError Nothing E.InvalidArgument errloc errmsg Nothing Nothing

jsonEncode :: Aeson.Value -> Text
jsonEncode = TL.toStrict . TL.decodeUtf8 . Aeson.encode

jsonEncode' :: Aeson.ToJSON a => a -> Text
jsonEncode' = jsonEncode . Aeson.toJSON
