{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Handler.Admin where

import           Control.Monad                    (join)
import           Data.Aeson                       ((.=))
import qualified Data.Aeson                       as Aeson
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (maybeToList)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Data.Text.Lazy.Encoding          (decodeUtf8)
import qualified Data.Vector                      as V
import qualified GHC.IO.Exception                 as E
import           Network.GRPC.HighLevel.Generated
import qualified Options.Applicative              as O
import qualified Options.Applicative.Help         as O
import           System.Exit                      (ExitCode (..), exitSuccess,
                                                   exitWith)
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB

import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import           HStream.Utils                    (Interval, interval2ms,
                                                   parserInterval, returnResp)

newtype AdminCommand
  = AdminStatsCommand StatsCommand

adminCommandInfo :: O.ParserInfo AdminCommand
adminCommandInfo = O.info adminCommandParser (O.progDesc "The parser to use for admin commands")

adminCommandParser :: O.Parser AdminCommand
adminCommandParser = O.subparser
  ( O.command "stats" (O.info (AdminStatsCommand <$> statsOptsParser) (O.progDesc "Get the stats of an operation on a stream"))
  )

adminCommandHandler
  :: ServerContext
  -> ServerRequest 'Normal AdminCommandRequest AdminCommandResponse
  -> IO (ServerResponse 'Normal AdminCommandResponse)
adminCommandHandler sc (ServerNormalRequest _ (AdminCommandRequest cmd)) = defaultExceptionHandle $ do
  let args = words (T.unpack cmd)
  Log.info $ "Receive amdin command: " <> Log.buildText cmd

  adminCommand <- handleParseResult $ O.execParserPure O.defaultPrefs adminCommandInfo args
  result <- case adminCommand of
              AdminStatsCommand c -> runStats sc c
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
-- Admin Stats Command

data StatsCommand = StatsCommand
  { statsType      :: CBytes
  , statsIntervals :: [Interval]
  }

statsOptsParser :: O.Parser StatsCommand
statsOptsParser = StatsCommand
  <$> O.strArgument ( O.help "the operations to be collected" )
  <*> O.many ( O.option ( O.eitherReader parserInterval)
                        ( O.long "intervals" <> O.short 'i'
                       <> O.help "the list of intervals to be collected" )
             )

runStats :: ServerContext -> StatsCommand -> IO T.Text
runStats ServerContext {..} StatsCommand{..} = do
  let intervals = map interval2ms statsIntervals
  m <- Stats.stream_time_series_getall_by_name scStatsHolder statsType intervals
  let headers = "stream_name" : (("throughput_" <>) . T.pack . show <$> statsIntervals)
      rows = Map.foldMapWithKey (\k vs -> [(CB.unpack k) : (show . floor <$> vs)]) m
  return . TL.toStrict . decodeUtf8 . Aeson.encode $
    Aeson.object ["headers" .= headers, "rows" .= rows]
