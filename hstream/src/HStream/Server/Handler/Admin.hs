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
import qualified Options.Applicative              as O
import           Z.Data.CBytes                    (CBytes)
import qualified Z.Data.CBytes                    as CB

import qualified HStream.Logger as Log
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
  let args = words (TL.unpack cmd)
  Log.info $ "Receice amdin command: " <> Log.buildLazyText cmd

  adminCommand <- O.handleParseResult $ O.execParserPure O.defaultPrefs adminCommandInfo args
  result <- case adminCommand of
              AdminStatsCommand c -> runStats sc c
  returnResp $ AdminCommandResponse {adminCommandResponseResult = result}

------------------------------------------------------------------------------
-- Admin Stats Command

data StatsCommand = StatsCommand
  { statsType      :: CBytes
  , statsStream    :: CBytes
  , statsIntervals :: [Interval]
  }

statsOptsParser :: O.Parser StatsCommand
statsOptsParser = StatsCommand
  <$> O.strArgument ( O.help "the operations to be collected" )
  <*> O.strOption   ( O.long "stream-name" <> O.short 'n'
                   <> O.help "the name of the stream to be collected" )
  <*> O.many ( O.option ( O.eitherReader parserInterval)
                        ( O.long "intervals" <> O.short 'i'
                       <> O.help "the list of intervals to be collected" )
             )

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
