module HStream.Client.Types where

import           Control.Concurrent      (MVar)
import qualified Options.Applicative     as O
import           Z.Data.CBytes           (CBytes)
import           Z.IO.Network            (SocketAddr)
import           Z.IO.Network.SocketAddr (PortNumber)

import           HStream.Server.Types    (ServerID)

data HStreamCommand = HStreamCommand
  { cliConnOpts :: CliConnOpts
  , cliCommand  :: Command
  }

data Command
  = HStreamSql HStreamSqlOpts
  | HStreamNodes HStreamNodes
  | HStreamInit

commandParser :: O.Parser HStreamCommand
commandParser = HStreamCommand
  <$> (CliConnOpts <$> serverHost <*> serverPort)
  <*> O.hsubparser
    (  O.command "sql"   (O.info (HStreamSql <$> hstreamSqlOptsParser) (O.progDesc "Start HStream SQL Shell"))
    <> O.command "nodes" (O.info (HStreamNodes <$> hstreamNodesParser) (O.progDesc "Manage HStream Server Cluster"))
    <> O.command "init"  (O.info (pure HStreamInit)                    (O.progDesc "Init HStream Server Cluster"))
    )

data HStreamSqlContext = HStreamSqlContext
  { availableServers :: MVar [SocketAddr]
  , currentServer    :: MVar SocketAddr
  , updateInterval   :: Int
  }

data HStreamSqlOpts = HStreamSqlOpts
  { _updateInterval :: Int
  , _retryTimeout   :: Int
  }

hstreamSqlOptsParser :: O.Parser HStreamSqlOpts
hstreamSqlOptsParser = HStreamSqlOpts
  <$> O.option O.auto (O.long "update-interval" <> O.metavar "INT" <> O.showDefault <> O.value 30 <> O.help "interval to update available servers in seconds")
  <*> O.option O.auto (O.long "retry-timeout" <> O.metavar "INT" <> O.showDefault <> O.value 60 <> O.help "timeout to retry connecting to a server in seconds")

data HStreamNodes
  = HStreamNodesList
  | HStreamNodesStatus (Maybe ServerID)

hstreamNodesParser :: O.Parser HStreamNodes
hstreamNodesParser = O.hsubparser
  (  O.command "list" (O.info (pure HStreamNodesList) (O.progDesc "List all running nodes in the cluster"))
  <> O.command "status" (O.info (HStreamNodesStatus <$> (O.optional . O.option O.auto) (O.long "id" <> O.help "Specify the id of the node"))
                                (O.progDesc "Show the status of nodes specified, if not specified show the status of all nodes"))
  )

data CliConnOpts = CliConnOpts
  { _serverHost :: CBytes
  , _serverPort :: PortNumber
  } deriving (Show, Eq)

serverHost :: O.Parser CBytes
serverHost =
  O.strOption ( O.long "host" <> O.metavar "SERVER-HOST"
              <> O.showDefault <> O.value "127.0.0.1"
              <> O.help "Server host value"
              )

serverPort :: O.Parser PortNumber
serverPort =
  O.option O.auto ( O.long "port" <> O.metavar "INT"
                  <> O.showDefault <> O.value 6570
                  <> O.help "Server port value"
                  )

connOptsParser :: O.Parser CliConnOpts
connOptsParser = CliConnOpts
  <$> serverHost
  <*> serverPort
