module HStream.Admin.Server.Types where

import           Data.Aeson                (FromJSON (..), ToJSON (..))
import qualified Data.Aeson                as Aeson
import           Data.Text                 (Text)
import           GHC.Generics              (Generic)
import           Network.Socket            (PortNumber)
import           Options.Applicative       ((<|>))
import qualified Options.Applicative       as O
import           Proto3.Suite              (Enumerated (Enumerated))
import qualified Text.Read                 as Read
import qualified Z.Data.CBytes             as CB
import           Z.Data.CBytes             (CBytes)

import           HStream.Common.CliParsers (streamParser, subscriptionParser)
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
                 <> O.showDefault <> O.value Log.INFO
                 <> O.help "Log level"
                  )

-------------------------------------------------------------------------------
-- Server Admin Command

data AdminCommand
  = AdminStatsCommand StatsCommand
  | AdminResetStatsCommand
  | AdminLookupCommand LookupCommand
  | AdminStreamCommand StreamCommand
  | AdminSubscriptionCommand SubscriptionCommand
  | AdminViewCommand ViewCommand
  | AdminQueryCommand QueryCommand
  | AdminConnectorCommand ConnectorCommand
  | AdminStatusCommand
  | AdminInitCommand
  | AdminCheckReadyCommand
  deriving (Show)

adminCommandParser :: O.Parser AdminCommand
adminCommandParser = O.hsubparser
  ( O.command "stats" (O.info (AdminStatsCommand <$> statsCmdParser)
                              (O.progDesc $ "Get the stats of an operation on a "
                                         <> "stream(or other) for only one specific server"))
 <> O.command "reset-stats" (O.info (pure AdminResetStatsCommand)
                                    (O.progDesc "Reset all counters to their initial values."))
 <> O.command "lookup" (O.info (AdminLookupCommand <$> lookupCmdParser)
                               (O.progDesc "Lookup command"))
 <> O.command "stream" (O.info (AdminStreamCommand <$> streamCmdParser)
                               (O.progDesc "Stream command"))
 <> O.command "sub"    (O.info (AdminSubscriptionCommand <$> subscriptionCmdParser)
                               (O.progDesc "Subscription command"))
 <> O.command "view"   (O.info (AdminViewCommand <$> viewCmdParser)
                               (O.progDesc "View command"))
 <> O.command "query"  (O.info (AdminQueryCommand <$> queryCmdParser)
                               (O.progDesc "Query command"))
 <> O.command "status" (O.info (pure AdminStatusCommand)
                               (O.progDesc "Get the status of the HServer cluster"))
 <> O.command "init"   (O.info (pure AdminInitCommand)
                               (O.progDesc "Init an HServer cluster"))
 <> O.command "ready"  (O.info (pure AdminCheckReadyCommand)
                               (O.progDesc "Check if an HServer cluster is ready"))
 <> O.command "connector" (O.info (AdminConnectorCommand <$> connectorCmdParser)
                                  (O.progDesc "Connector command"))
  )

-------------------------------------------------------------------------------

data LookupCommand = LookupCommand Text Text deriving (Show)

lookupCmdParser :: O.Parser LookupCommand
lookupCmdParser = LookupCommand
  <$> O.strArgument ( O.metavar "RESOURCE_TYPE"
                   <> O.help "The type of resource, include: [stream|subscription|query|view|connector|shard|shard-reader]")
  <*> O.strArgument ( O.metavar "RESOURCE_ID" <> O.help "The id of resource")

data StreamCommand
  = StreamCmdList
  | StreamCmdCreate API.Stream
  | StreamCmdDelete Text Bool
  | StreamCmdDescribe Text
  deriving (Show)

streamCmdParser :: O.Parser StreamCommand
streamCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure StreamCmdList) (O.progDesc "Get all streams"))
 <> O.command "create" (O.info (StreamCmdCreate <$> streamParser) (O.progDesc "Create a stream"))
 <> O.command "describe" (O.info (StreamCmdDescribe <$> O.strArgument ( O.metavar "STREAM_NAME"
                                                                      <> O.help "The name of the stream"))
                               (O.progDesc "Get the details of a stream"))
 <> O.command "delete" (O.info (StreamCmdDelete <$> O.strArgument ( O.metavar "STREAM_NAME"
                                                               <> O.help "The name of the stream to delete")
                                                <*> O.switch ( O.long "force"
                                                            <> O.short 'f'
                                                            <> O.help "Whether to enable force deletion" ))
                               (O.progDesc "Delete a stream")
                        )
  )

-------------------------------------------------------------------------------

-- TODO:
-- SubscriptionWatchOnDifferentNode is not handled for delete command
data SubscriptionCommand
  = SubscriptionCmdList
  | SubscriptionCmdCreate API.Subscription
  | SubscriptionCmdDelete Text Bool
  | SubscriptionCmdDeleteAll Bool
  | SubscriptionCmdDescribe Text
  deriving (Show)

subscriptionCmdParser :: O.Parser SubscriptionCommand
subscriptionCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure SubscriptionCmdList) (O.progDesc "Get all subscriptions"))
 <> O.command "create" (O.info (SubscriptionCmdCreate <$> subscriptionParser)
                               (O.progDesc "Create a subscription"))
 <> O.command "describe" (O.info (SubscriptionCmdDescribe <$> O.strArgument ( O.metavar "SUB_ID"
                                                                           <> O.help "The ID of the subscription"))
                                 (O.progDesc "Get the details of a subscription"))
 <> O.command "delete" (O.info (SubscriptionCmdDelete <$> O.strArgument ( O.metavar "SUB_ID"
                                                                      <> O.help "The ID of the subscription")
                                                      <*> O.switch ( O.long "force"
                                                                  <> O.short 'f' ))
                               (O.progDesc "Delete a subscription. NOTE: make sure you send the request to the right server")
                       )
 <> O.command "deleteall" (O.info (SubscriptionCmdDeleteAll
                                    <$> O.switch (O.long "force" <> O.short 'f'))
                                  (O.progDesc "Delete all subscriptions. NOTE: make sure you send the request to the right server")
                          )
  )

-------------------------------------------------------------------------------

data ViewCommand
  = ViewCmdList
  deriving (Show)

viewCmdParser :: O.Parser ViewCommand
viewCmdParser = O.subparser
  ( O.command "list" (O.info (pure ViewCmdList) (O.progDesc "Get all views"))
  )

-------------------------------------------------------------------------------

data QueryCommand
  = QueryCmdStatus Text
  | QueryCmdDescribe Text
  | QueryCmdResume Text
  | QueryCmdList
  deriving Show

queryCmdParser :: O.Parser QueryCommand
queryCmdParser = O.subparser
  ( O.command "status" (O.info (QueryCmdStatus <$> O.strOption ( O.long "id"
                                                              <> O.short 'i'
                                                              <> O.metavar "QUERY_ID"
                                                              <> O.help "The ID of the query"))
                               (O.progDesc "Get the status of a query"))
  <> O.command "describe" (O.info (QueryCmdDescribe <$> O.strOption ( O.long "id"
                                                                   <> O.short 'i'
                                                                   <> O.metavar "QUERY_ID"
                                                                   <> O.help "The ID of the query"))
                                  (O.progDesc "Get the metadata of a query"))
  <> O.command "resume" (O.info (QueryCmdResume <$> O.strOption ( O.long "id"
                                                               <> O.short 'i'
                                                               <> O.metavar "QUERY_ID"
                                                               <> O.help "The ID of the query"))
                                (O.progDesc "Resume specific query"))
  <> O.command "list" (O.info (pure QueryCmdList) (O.progDesc "List all queries"))
  )

-------------------------------------------------------------------------------

data ConnectorCommand
  = ConnectorCmdList
  -- | ConnectorCmdDelete Text Bool
  -- | ConnectorCmdDescribe Text
  deriving (Show)

connectorCmdParser :: O.Parser ConnectorCommand
connectorCmdParser = O.subparser
  ( O.command "list" (O.info (pure ConnectorCmdList) (O.progDesc "Get all connectors"))
  )

-------------------------------------------------------------------------------

-- TODO: auto generate from commom/stats/include/*.inc
data StatsCategory
  = PerStreamStats
  | PerStreamTimeSeries
  | PerSubscriptionStats
  | PerSubscriptionTimeSeries
  | PerHandleTimeSeries
  | PerConnectorStats
  | PerQueryStats
  | PerViewStats
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
        Read.Ident "handle" -> PerHandleTimeSeries
        Read.Ident "connector_counter" -> PerConnectorStats
        Read.Ident "query_counter" -> PerQueryStats
        Read.Ident "view_counter" -> PerViewStats
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
                       <> O.help "the stats category, e.g. stream_counter, stream"
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
