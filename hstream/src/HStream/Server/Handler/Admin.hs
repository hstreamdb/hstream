{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.Server.Handler.Admin where

import           Control.Monad                    (join)
import qualified Data.Aeson                       as A
import           Data.Maybe                       (maybeToList)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           Data.Text.Lazy.Encoding          (decodeUtf8)
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Options.Applicative
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB

import           HStream.Server.Exception         (defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import qualified HStream.Stats                    as Stats
import           HStream.Utils                    (Interval, interval2ms,
                                                   parserInterval, returnResp)

data AdminCommand
  = AdminStatsCommand StatsCommand

adminCommandParser :: ServerContext -> ParserInfo (IO TL.Text)
adminCommandParser sc = info
  (subparser (
    command "stats" (info (runStats sc <$> parseStatsOpts)
                          (progDesc "Get the stats of an operation on a stream"))
    <> idm))
  (progDesc "The parser to use for admin commands")

adminCommandHandler :: ServerContext
  -> ServerRequest 'Normal AdminCommandRequest AdminCommandResponse
  -> IO (ServerResponse 'Normal AdminCommandResponse)
adminCommandHandler sc (ServerNormalRequest _ (AdminCommandRequest cmd)) = defaultExceptionHandle $ do
  result <- join . handleParseResult .
    execParserPure defaultPrefs (adminCommandParser sc) $
    words (TL.unpack cmd)
  let resp = AdminCommandResponse {adminCommandResponseResult = result}
  returnResp resp
------------------------------------------------------------------------------
-- Admin Stats Command

data StatsCommand = StatsCommand {
    statsType      :: CBytes
  , statsStream    :: CBytes
  , statsIntervals :: [Interval]
  }

parseStatsOpts :: Parser StatsCommand
parseStatsOpts = StatsCommand
  <$> strArgument ( help "the operations to be collected" )
  <*> strOption   ( long "stream-name" <> short 'n'
                  <> help "the name of the stream to be collected" )
  <*> many (option (eitherReader parserInterval) ( long "intervals" <> short 'i'
                        <> help "the list of intervals to be collected" ))

runStats :: ServerContext -> StatsCommand -> IO TL.Text
runStats ServerContext {..} StatsCommand{..} = do
  result <- concat . maybeToList <$>
    maybe (pure Nothing)
          (Stats.stream_time_series_get scStatsHolder statsType statsStream)
          (Just $ interval2ms <$> statsIntervals)
  let throughput = zip (("throughput_" <>) . T.pack . show <$> statsIntervals) (A.toJSON <$> result)
  let json = ("node_id", A.toJSON (CB.unpack serverName))
           : ("stream_name", A.toJSON (CB.unpack statsStream))
           : throughput
  return . decodeUtf8 . A.encode $ A.object json
