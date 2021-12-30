{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs     #-}

module HStream.Server.Handler.Admin (adminCommandHandler) where

import           Data.Aeson                       ((.=))
import qualified Data.Aeson                       as Aeson
import qualified Data.HashMap.Strict              as HM
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Text.Lazy.Encoding          as TL
import qualified Data.Vector                      as V
import qualified GHC.IO.Exception                 as E
import           Network.GRPC.HighLevel.Generated
import qualified Options.Applicative              as O
import qualified Options.Applicative.Help         as O
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB

import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Persistence       (getClusterStatus)
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import qualified HStream.Store                    as S
import           HStream.Utils                    (Interval, interval2ms,
                                                   parserInterval, returnResp,
                                                   showNodeStatus)

------------------------------------------------------------------------------

adminCommandHandler
  :: ServerContext
  -> ServerRequest 'Normal AdminCommandRequest AdminCommandResponse
  -> IO (ServerResponse 'Normal AdminCommandResponse)
adminCommandHandler sc (ServerNormalRequest _ (AdminCommandRequest cmd)) = defaultExceptionHandle $ do
  let args = words (T.unpack cmd)
  Log.info $ "Receive amdin command: " <> Log.buildText cmd

  adminCommand <- handleParseResult $ O.execParserPure O.defaultPrefs adminCommandInfo args
  result <- case adminCommand of
              AdminStatsCommand c  -> runStats sc c
              AdminStreamCommand c -> runStream sc c
              AdminStatusCommand   -> runStatus sc
  returnResp $ AdminCommandResponse {adminCommandResponseResult = result}

handleParseResult :: O.ParserResult a -> IO a
handleParseResult (O.Success a) = return a
handleParseResult (O.Failure failure) = do
  let (h, _exit, _cols) = O.execFailure failure ""
      errmsg = (O.displayS . O.renderCompact . O.extractChunk $ O.helpError h) ""
      errloc = "Parsing admin command error"
  E.ioError $ E.IOError Nothing E.InvalidArgument errloc errmsg Nothing Nothing
handleParseResult (O.CompletionInvoked compl) = do
  errmsg <- O.execCompletion compl ""
  let errloc = "Parsing admin command error"
  E.ioError $ E.IOError Nothing E.InvalidArgument errloc errmsg Nothing Nothing

------------------------------------------------------------------------------

data AdminCommand
  = AdminStatsCommand StatsCommand
  | AdminStreamCommand StreamCommand
  | AdminStatusCommand

adminCommandInfo :: O.ParserInfo AdminCommand
adminCommandInfo = O.info adminCommandParser (O.progDesc "The parser to use for admin commands")

adminCommandParser :: O.Parser AdminCommand
adminCommandParser = O.subparser
  ( O.command "stats" (O.info (AdminStatsCommand <$> statsCmdParser) (O.progDesc "Get the stats of an operation on a stream"))
 <> O.command "stream" (O.info (AdminStreamCommand <$> streamCmdParser) (O.progDesc "Stream command"))
 <> O.command "status" (O.info (pure AdminStatusCommand) (O.progDesc "Get the status of the HServer cluster"))
  )

------------------------------------------------------------------------------
-- Admin Stats Command

data StatsCommand = StatsCommand
  { statsType      :: CBytes
  , statsIntervals :: [Interval]
  }

statsCmdParser :: O.Parser StatsCommand
statsCmdParser = StatsCommand
  <$> O.strArgument ( O.help "the operations to be collected" )
  <*> O.many ( O.option ( O.eitherReader parserInterval)
                        ( O.long "intervals" <> O.short 'i'
                       <> O.help "the list of intervals to be collected" )
             )

runStats :: ServerContext -> StatsCommand -> IO T.Text
runStats ServerContext{..} StatsCommand{..} = do
  let intervals = map interval2ms statsIntervals
  m <- Stats.stream_time_series_getall_by_name scStatsHolder statsType intervals
  let headers = "stream_name" : (("throughput_" <>) . T.pack . show <$> statsIntervals)
      rows = Map.foldMapWithKey (\k vs -> [CB.unpack k : (show @Int . floor <$> vs)]) m
  return . TL.toStrict . TL.decodeUtf8 . Aeson.encode $
    Aeson.object ["headers" .= headers, "rows" .= rows]

------------------------------------------------------------------------------
-- Admin Stream Command

data StreamCommand = StreamCmdList

streamCmdParser :: O.Parser StreamCommand
streamCmdParser = O.subparser
  ( O.command "list" (O.info (pure StreamCmdList) (O.progDesc "Get all streams")))

runStream :: ServerContext -> StreamCommand -> IO T.Text
runStream ServerContext{..} StreamCmdList = do
  let headers = ["name" :: T.Text, "replication_property"]
  streams <- S.findStreams scLDClient S.StreamTypeStream True
  rows <- V.forM (V.fromList streams) $ \stream -> do
    refactor <- S.getStreamReplicaFactor scLDClient stream
    return [T.pack . S.showStreamName $ stream, "node:" <> T.pack (show refactor)]
  return . TL.toStrict . TL.decodeUtf8 . Aeson.encode $
    Aeson.object ["headers" .= headers, "rows" .= rows]

--------------------------------------------------------------------------------
-- Admin Status Command

runStatus :: ServerContext -> IO T.Text
runStatus ServerContext{..} = do
  let headers = ["node_id" :: T.Text, "state", "address"]
  values <- HM.elems <$> getClusterStatus zkHandle
  let rows = V.fromList $ map (\ServerNodeStatus { serverNodeStatusNode = Just ServerNode{..}, ..} ->
        [ showT serverNodeId
        , showNodeStatus serverNodeStatusState
        , serverNodeHost
        <> showT serverNodePort])
        values
  return . TL.toStrict . TL.decodeUtf8 . Aeson.encode $
    Aeson.object ["headers" .= headers, "rows" .= rows]
  where
    showT = T.pack . show
