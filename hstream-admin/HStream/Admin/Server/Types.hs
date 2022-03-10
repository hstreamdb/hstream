module HStream.Admin.Server.Types where

import           Data.Aeson                (FromJSON (..), ToJSON (..))
import qualified Data.Aeson                as Aeson
import           GHC.Generics              (Generic)
import           Options.Applicative       ((<|>))
import qualified Options.Applicative       as O
import           Z.Data.CBytes             (CBytes)
import qualified Z.Data.CBytes             as CB
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
  | AdminStreamCommand StreamCommand
  | AdminSubscriptionCommand SubscriptionCommand
  | AdminViewCommand ViewCommand
  | AdminStatusCommand
  deriving (Show)

adminCommandParser :: O.Parser AdminCommand
adminCommandParser = O.hsubparser
  ( O.command "stats" (O.info (AdminStatsCommand <$> statsCmdParser)
                              (O.progDesc $ "Get the stats of an operation on a"
                                         <> "stream for only one specific server"))
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
  deriving (Show)

streamCmdParser :: O.Parser StreamCommand
streamCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure StreamCmdList) (O.progDesc "Get all streams"))
 <> O.command "create" (O.info (StreamCmdCreate <$> streamParser) (O.progDesc "Create a stream"))
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

-------------------------------------------------------------------------------

-- TODO:
-- SubscriptionWatchOnDifferentNode is not handled for delete command
data SubscriptionCommand
  = SubscriptionCmdList
  | SubscriptionCmdCreate API.Subscription
  | SubscriptionCmdDelete API.Subscription
  deriving (Show)

subscriptionCmdParser :: O.Parser SubscriptionCommand
subscriptionCmdParser = O.hsubparser
  ( O.command "list" (O.info (pure SubscriptionCmdList) (O.progDesc "get all subscriptions"))
 <> O.command "create" (O.info (SubscriptionCmdCreate <$> subscriptionParser)
                               (O.progDesc "create a subscription"))
 <> O.command "delete" (O.info (SubscriptionCmdDelete <$> subscriptionParser)
                               (O.progDesc "delete a subscription (Warning: incomplete implementation)")
                       )
  )

subscriptionParser :: O.Parser API.Subscription
subscriptionParser = API.Subscription
  <$> O.strOption ( O.long "id" <> O.metavar "SubID"
                 <> O.help "subscription id" )
  <*> O.strOption ( O.long "stream" <> O.metavar "StreamName"
                 <> O.help "the stream associated with the subscription" )
  <*> O.option O.auto ( O.long "timeout" <> O.metavar "INT" <> O.value 60
                     <> O.help "subscription timeout in seconds")

-------------------------------------------------------------------------------

data ViewCommand
  = ViewCmdList
  deriving (Show)

viewCmdParser :: O.Parser ViewCommand
viewCmdParser = O.subparser
  ( O.command "list" (O.info (pure ViewCmdList) (O.progDesc "Get all views"))
  )

-------------------------------------------------------------------------------

data StatsCommand = StatsCommand
  { statsType      :: CBytes
  , statsIntervals :: [U.Interval]
  } deriving (Show)

statsCmdParser :: O.Parser StatsCommand
statsCmdParser = StatsCommand
  <$> O.strArgument ( O.metavar "STATS"
                   <> O.help "the operations to be collected, e.g. appends,reads"
                    )
  <*> O.many ( O.option ( O.eitherReader U.parserInterval)
                        ( O.long "intervals" <> O.short 'i'
                       <> O.help "the list of intervals to be collected" )
             )

-------------------------------------------------------------------------------

data CommandResponseType
  = CommandResponseTypeTable
  | CommandResponseTypePlain
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
