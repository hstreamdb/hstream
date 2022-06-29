module HStream.Admin.Server.Types where

import           Data.Aeson                (FromJSON (..), ToJSON (..))
import qualified Data.Aeson                as Aeson
import           Data.Text                 (Text)
import           GHC.Generics              (Generic)
import           Options.Applicative       ((<|>))
import qualified Options.Applicative       as O
import qualified Text.Read                 as Read
import qualified Z.Data.CBytes             as CB
import           Z.Data.CBytes             (CBytes)
import           Z.IO.Network.SocketAddr   (PortNumber)

import qualified HStream.Logger            as Log
import qualified HStream.Server.HStreamApi as API
import qualified HStream.Utils             as U

-------------------------------------------------------------------------------

data Cli = Cli
  { cliOpts  :: CliOpts
  , logLevel :: Log.Level
  , command  :: Command
  }

cliParser :: O.Parser Cli
cliParser = Cli
  <$> cliOptsParser
  <*> logLevelParser
  <*> commandParser

-------------------------------------------------------------------------------

-- | Global command line options
data CliOpts = CliOpts
  { optServerHost :: CB.CBytes
  , optServerPort :: PortNumber
  } deriving (Show, Eq)

cliOptsParser :: O.Parser CliOpts
cliOptsParser = CliOpts
        <$> O.strOption ( O.long "host" <> O.metavar "SERVER-HOST"
                       <> O.showDefault <> O.value "127.0.0.1"
                       <> O.help "server host admin value"
                        )
        <*> O.option O.auto ( O.long "port" <> O.metavar "INT"
                           <> O.showDefault <> O.value 6570
                           <> O.help "server admin port value"
                            )

-------------------------------------------------------------------------------

data Command
  = ServerSqlCmd ServerSqlCmdOpts
  | ServerAdminCmd AdminCommand
  deriving (Show)

commandParser :: O.Parser Command
commandParser = ServerSqlCmd <$> sqlCommandParser
            <|> ServerAdminCmd <$> adminCommandParser

newtype ServerSqlCmdOpts = ServerSqlCmdOpts { serverSqlCmdRepl :: Maybe String }
  deriving (Show, Eq)

sqlCommandParser :: O.Parser ServerSqlCmdOpts
sqlCommandParser = O.hsubparser
  ( O.command "sql" (O.info serverSqlCmdOptsParser
                       (O.progDesc "Start an interactive SQL shell"))
  )

serverSqlCmdOptsParser :: O.Parser ServerSqlCmdOpts
serverSqlCmdOptsParser = ServerSqlCmdOpts
  <$> O.optional (O.strOption ( O.long "sql"
                         <> O.metavar "SQL"
                         <> O.short 'e'
                         <> O.help "Run sql expression non-interactively."
                          ))

logLevelParser :: O.Parser Log.Level
logLevelParser =
  O.option O.auto ( O.long "log-level" <> O.metavar "[critical|fatal|warning|info|debug]"
                 <> O.showDefault <> O.value (Log.Level Log.INFO)
                 <> O.help "log level"
                  )

-------------------------------------------------------------------------------
-- Server Admin Command

data AdminCommand
  = AdminStatsCommand StatsCommand
  | AdminResetStatsCommand
  | AdminStreamCommand StreamCommand
  | AdminSubscriptionCommand SubscriptionCommand
  | AdminViewCommand ViewCommand
  | AdminStatusCommand
  deriving (Show)

adminCommandParser :: O.Parser AdminCommand
adminCommandParser = O.hsubparser
  ( O.command "stats" (O.info (AdminStatsCommand <$> statsCmdParser)
                              (O.progDesc $ "Get the stats of an operation on a "
                                         <> "stream(or other) for only one specific server"))
 <> O.command "reset-stats" (O.info (pure AdminResetStatsCommand)
                                    (O.progDesc "Reset all counters to their initial values."))
 <> O.command "stream" (O.info (AdminStreamCommand <$> streamCmdParser)
                               (O.progDesc "Stream command"))
 <> O.command "sub" (O.info (AdminSubscriptionCommand <$> subscriptionCmdParser)
                            (O.progDesc "Subscription command"))
 <> O.command "view" (O.info (AdminViewCommand <$> viewCmdParser)
                             (O.progDesc "View command"))
 <> O.command "status" (O.info (pure AdminStatusCommand)
                               (O.progDesc "Get the status of the HServer cluster"))
  )

-------------------------------------------------------------------------------

data StreamCommand
  = StreamCmdList
  | StreamCmdCreate API.Stream
  | StreamCmdDelete Text Bool
  deriving (Show)

streamCmdParser :: O.Parser StreamCommand
streamCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure StreamCmdList) (O.progDesc "Get all streams"))
 <> O.command "create" (O.info (StreamCmdCreate <$> streamParser) (O.progDesc "Create a stream"))
 <> O.command "delete" (O.info (StreamCmdDelete <$> O.strOption ( O.long "name" <> O.metavar "STRING"
                                                               <> O.help "stream name")
                                                <*> O.switch ( O.long "force"
                                                            <> O.short 'f' ))
                               (O.progDesc "delete a stream (Warning: incomplete implementation)")
                        )
  )

streamParser :: O.Parser API.Stream
streamParser = API.Stream
  <$> O.strOption ( O.long "name"
                 <> O.metavar "STRING"
                 <> O.help "stream name"
                  )
  <*> O.option O.auto ( O.long "replication-factor"
                     <> O.short 'r'
                     <> O.metavar "INT"
                     <> O.showDefault
                     <> O.value 3
                     <> O.help "replication-factor"
                      )
  <*> O.option O.auto ( O.long "backlog-duration"
                     <> O.short 'b'
                     <> O.metavar "INT"
                     <> O.showDefault
                     <> O.value 0
                     <> O.help "Backlog duration in seconds"
                      )

-------------------------------------------------------------------------------

-- TODO:
-- SubscriptionWatchOnDifferentNode is not handled for delete command
data SubscriptionCommand
  = SubscriptionCmdList
  | SubscriptionCmdCreate API.Subscription
  | SubscriptionCmdDelete API.Subscription Bool
  deriving (Show)

subscriptionCmdParser :: O.Parser SubscriptionCommand
subscriptionCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure SubscriptionCmdList) (O.progDesc "get all subscriptions"))
 <> O.command "create" (O.info (SubscriptionCmdCreate <$> subscriptionParser)
                               (O.progDesc "create a subscription"))
 <> O.command "delete" (O.info (SubscriptionCmdDelete <$> subscriptionParser
                                                      <*> O.switch ( O.long "force"
                                                                  <> O.short 'f' ))
                               (O.progDesc "delete a subscription")
                       )
  )

subscriptionParser :: O.Parser API.Subscription
subscriptionParser = API.Subscription
  <$> O.strOption ( O.long "id" <> O.metavar "SubID"
                 <> O.help "subscription id" )
  <*> O.strOption ( O.long "stream" <> O.metavar "StreamName"
                 <> O.help "the stream associated with the subscription" )
  <*> O.option O.auto ( O.long "ack-timeout" <> O.metavar "INT" <> O.value 60
                     <> O.help "subscription timeout in seconds")
  <*> O.option O.auto ( O.long "max-unacked-records" <> O.metavar "INT"
                     <> O.value 10000
                     <> O.help "maximum count of unacked records")

-------------------------------------------------------------------------------

data ViewCommand
  = ViewCmdList
  deriving (Show)

viewCmdParser :: O.Parser ViewCommand
viewCmdParser = O.subparser
  ( O.command "list" (O.info (pure ViewCmdList) (O.progDesc "Get all views"))
  )

-------------------------------------------------------------------------------

-- TODO
data StatsCategory
  = PerStreamStats
  | PerStreamTimeSeries
  | PerSubscriptionStats
  | PerSubscriptionTimeSeries
  | ServerHistogram
  deriving (Show, Eq)

instance Read StatsCategory where
  readPrec = do
    l <- Read.lexP
    return $
      case l of
        Read.Ident "stream_counter" -> PerStreamStats
        Read.Ident "stream" -> PerStreamTimeSeries
        Read.Ident "subscription_counter" -> PerSubscriptionStats
        Read.Ident "subscription" -> PerSubscriptionTimeSeries
        Read.Ident "server_histogram" -> ServerHistogram
        x -> errorWithoutStackTrace $ "cannot parse StatsCategory: " <> show x

data StatsCommand = StatsCommand
  { statsCategory    :: StatsCategory
  , statsName        :: CBytes
  , statsIntervals   :: [U.Interval]
  , statsPercentiles :: [Double]
  } deriving (Show)

statsCmdParser :: O.Parser StatsCommand
statsCmdParser = StatsCommand
  <$> O.argument O.auto ( O.metavar "STATS"
                       <> O.help "the stats category, e.g. stream, subscription"
                        )
  <*> O.strArgument ( O.metavar "NAME"
                   <> O.help "the stats name to be collected, e.g. appends,sends"
                    )
  <*> ( O.some ( O.option ( O.eitherReader U.parserInterval )
                          ( O.long "intervals" <> O.short 'i'
                         <> O.help ( "the list of intervals to be collected, "
                                  <> "default is [1min, 5min, 10min], "
                                  <> "only needed for per time series stats"
                                   )
                          )
               )
    -- https://github.com/pcapriotti/optparse-applicative/issues/53
    <|> pure [U.Minutes 1, U.Minutes 5, U.Minutes 10]
      )
  <*> ( O.some ( O.option O.auto
                          ( O.long "percentiles" <> O.short 'p'
                         <> O.help ( "the list of percentiles to be collected, "
                                  <> "default is [0.5, 0.75, 0.95, 0.99], "
                                  <> "only needed for server histogram stats"
                                   )
                          )
               )
    <|> pure [0.5, 0.75, 0.95, 0.99]
      )

-------------------------------------------------------------------------------

data CommandResponseType
  = CommandResponseTypeTable
  | CommandResponseTypePlain
  | CommandResponseTypeError
  deriving (Show, Generic)

instance ToJSON CommandResponseType where
  toJSON     = Aeson.genericToJSON $ U.toQuietSnakeAesonOpt' "CommandResponseType"
  toEncoding = Aeson.genericToEncoding $ U.toQuietSnakeAesonOpt' "CommandResponseType"

instance FromJSON CommandResponseType where
  parseJSON = Aeson.genericParseJSON $ U.toQuietSnakeAesonOpt' "CommandResponseType"

data AdminCommandResponse a = AdminCommandResponse
  { acrType    :: CommandResponseType
  , acrContent :: a
  } deriving (Show, Generic)

instance ToJSON a => ToJSON (AdminCommandResponse a) where
  toJSON     = Aeson.genericToJSON $ U.toQuietSnakeAesonOpt "acr"
  toEncoding = Aeson.genericToEncoding $ U.toQuietSnakeAesonOpt "acr"

instance FromJSON a => FromJSON (AdminCommandResponse a) where
  parseJSON = Aeson.genericParseJSON $ U.toQuietSnakeAesonOpt "acr"

-------------------------------------------------------------------------------
-- Helpers
