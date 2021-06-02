module HStream.Store.Admin.Types where

import           Control.Monad
import           Data.Int
import           Data.Text               (Text)
import           Options.Applicative
import qualified Text.Read               as Read
import qualified Z.Data.Parser           as P
import           Z.Data.Vector           (Bytes)
import qualified Z.Data.Vector           as V

import qualified HStream.Store.Admin.API as AA

-------------------------------------------------------------------------------

data StatusFormat = JSONFormat | TabularFormat

instance Show StatusFormat where
  show JSONFormat    = "json"
  show TabularFormat = "tabular"

instance Read StatusFormat where
  readPrec = do
    i <- Read.lexP
    case i of
      Read.Ident "json" -> return JSONFormat
      Read.Ident "tabular" -> return TabularFormat
      x -> errorWithoutStackTrace $ "cannot parse value: " <> show x

data SimpleNodesFilter
  = StatusNodeIdx [Int]
  | StatusNodeName [Text]
  deriving (Show)

simpleNodesFilterParser :: Parser SimpleNodesFilter
simpleNodesFilterParser =
      (StatusNodeIdx <$> many (option auto (long "nodes" <> metavar "INT" <> help "list of node ids")))
  <|> (StatusNodeName <$> many (strOption (long "names" <> metavar "STRING" <> help "list of hostnames")))

fromSimpleNodesFilter :: SimpleNodesFilter -> [AA.NodesFilter]
fromSimpleNodesFilter s =
  let fromIdx idx = AA.NodesFilter (Just (AA.NodeID (Just idx) Nothing Nothing)) Nothing Nothing
      fromName name = AA.NodesFilter (Just (AA.NodeID Nothing Nothing (Just name))) Nothing Nothing
   in case s of
        StatusNodeIdx ids -> map (fromIdx . fromIntegral) ids
        StatusNodeName ns -> map fromName ns

runSimpleNodesFilter
  :: SimpleNodesFilter
  -> (Maybe AA.NodesFilter -> AA.ThriftM p c s b)
  -> AA.ThriftM p c s (Either b [b])
runSimpleNodesFilter s f = do
  case fromSimpleNodesFilter s of
    [] -> Left <$> f Nothing
    xs -> Right <$> forM xs (f . Just)

data StatusOpts = StatusOpts
  { statusFormat    :: StatusFormat
  , statusForce     :: Bool
  , statusFilter    :: SimpleNodesFilter
  , statusSortField :: Text
  } deriving (Show)

statusParser :: Parser StatusOpts
statusParser = StatusOpts
  <$> option auto ( long "format"
                 <> short 'f'
                 <> metavar "json|tabular"
                 <> showDefault
                 <> value TabularFormat
                 <> help "Possible output formats"
                  )
  <*> switch ( long "force"
            <> help "Sets the force flag in the Admin API call"
             )
  <*> simpleNodesFilterParser
  <*> strOption ( long "sort"
               <> metavar "STRING"
               <> showDefault
               <> value "ID"
               <> help "What field to sort by the tabular output"
                )

-------------------------------------------------------------------------------

data NodesConfigOpts
  = NodesConfigShow SimpleNodesFilter
  | NodesConfigBootstrap [ReplicationPropertyPair]
  deriving (Show)

nodesConfigBootstrapParser :: Parser NodesConfigOpts
nodesConfigBootstrapParser = NodesConfigBootstrap
  <$> many (option auto ( long "metadata-replicate-across"
                       <> short 'r'
                       <> metavar "STRING:INT"
                       <> help "Defines cross-domain replication for metadata logs"))

nodesConfigParser :: Parser NodesConfigOpts
nodesConfigParser = hsubparser
  ( command "show" (info (NodesConfigShow <$> simpleNodesFilterParser) (progDesc "Print tier's NodesConfig to stdout"))
 <> command "bootstrap" (info nodesConfigBootstrapParser (progDesc "Finalize the bootstrapping and allow the cluster to be used"))
  )

-------------------------------------------------------------------------------

parseShard :: ReadM AA.ShardID
parseShard = eitherReader $ parse . V.packASCII
  where
    parse :: Bytes -> Either String AA.ShardID
    parse bs =
      case P.parse' parser bs of
        Left er -> Left $ "cannot parse value: " <> show er
        Right i -> Right i
    parser = do
      P.skipSpaces
      P.char8 'N' <|> P.char8 'n'
      n <- P.int
      P.char8 ':'
      P.char8 'S' <|> P.char8 's'
      s <- P.int
      P.skipSpaces
      return $ AA.ShardID (AA.NodeID (Just n) Nothing Nothing) s

socketConfigParser :: Parser (AA.SocketConfig AA.AdminAPI)
socketConfigParser = AA.SocketConfig
  <$> strOption ( long "host"
               <> metavar "HOST"
               <> showDefault
               <> value "127.0.0.1"
               <> help "Admin server host, e.g. ::1"
                )
  <*> option auto ( long "port"
                 <> metavar "PORT"
                 <> help "Admin server port"
                  )
  <*> option auto ( long "protocol"
                 <> metavar "INT"
                 <> showDefault
                 <> value AA.binaryProtocolId
                 <> help "Protocol id, 0 for binary, 2 for compact"
                  )

headerConfigParser :: Parser (AA.HeaderConfig AA.AdminAPI)
headerConfigParser = AA.HeaderConfig
  <$> strOption ( long "host"
               <> metavar "STRING"
               <> showDefault
               <> value "127.0.0.1"
               <> help "Admin server host, e.g. ::1"
                )
  <*> option auto ( long "port"
                 <> metavar "INT"
                 <> help "Admin server port"
                  )
  <*> option auto ( long "protocol"
                 <> metavar "INT"
                 <> showDefault
                 <> value AA.binaryProtocolId
                 <> help "Protocol id, 0 for binary, 2 for compact"
                  )
  <*> option auto ( long "conntimeout"
                 <> metavar "INT"
                 <> showDefault
                 <> value 5000
                 <> help "ConnTimeout"
                  )
  <*> option auto ( long "sendtimeout"
                 <> metavar "INT"
                 <> showDefault
                 <> value 5000
                 <> help "SendTimeout"
                  )
  <*> option auto ( long "recvtimeout"
                 <> metavar "INT"
                 <> showDefault
                 <> value 5000
                 <> help "RecvTimeout"
                  )

nodeIdParser :: Parser AA.NodeID
nodeIdParser = AA.NodeID
  <$> option auto ( long "node"
                 <> help "Node index"
                  )
  -- TODO: SocketAddress
  <*> pure Nothing
  <*> optional (strOption (long "name" <> help "Node hostname"))

nodesFilterParser :: Parser AA.NodesFilter
nodesFilterParser = AA.NodesFilter
  <$> optional nodeIdParser
  <*> optional nodeRoleParser
  <*> optional (strOption ( long "location"
                         <> metavar "STRING"
                         <> help "Node location"
                          ))

instance Read AA.Role where
  readPrec = do
    i <- Read.lexP
    case i of
      Read.Ident "storage"   -> return AA.Role_STORAGE
      Read.Ident "sequencer" -> return AA.Role_SEQUENCER
      x -> errorWithoutStackTrace $ "cannot parse value: " <> show x

nodeRoleParser :: Parser AA.Role
nodeRoleParser =
  option auto ( long "role"
             <> metavar "[storage|sequencer]"
             <> showDefault
             <> value AA.Role_STORAGE
             <> help "Node role"
              )

nodeRoleParserMaybe :: Parser (Maybe AA.Role)
nodeRoleParserMaybe = optional nodeRoleParser

instance Read AA.LocationScope where
  readPrec = do
    i <- Read.lexP
    case i of
      Read.Ident "node"       -> return AA.LocationScope_NODE
      Read.Ident "rack"       -> return AA.LocationScope_RACK
      Read.Ident "row"        -> return AA.LocationScope_ROW
      Read.Ident "cluster"    -> return AA.LocationScope_CLUSTER
      Read.Ident "datacenter" -> return AA.LocationScope_DATA_CENTER
      Read.Ident "region"     -> return AA.LocationScope_REGION
      Read.Ident "root"       -> return AA.LocationScope_ROOT
      x -> errorWithoutStackTrace $ "cannot parse value: " <> show x

newtype ReplicationPropertyPair = ReplicationPropertyPair
  { unReplicationPropertyPair :: (AA.LocationScope, Int32) }
  deriving Show

instance Read ReplicationPropertyPair where
  readPrec = do
    scope :: AA.LocationScope <- Read.readPrec
    splitor <- Read.lexP
    case splitor of
      Read.Symbol ":" -> do factor :: Int32 <- Read.readPrec
                            return $ ReplicationPropertyPair (scope, factor)
      x -> errorWithoutStackTrace $ "cannot parse value: " <> show x

data ConfigCmdOpts = ConfigDump
  deriving (Show)

configCmdParser :: Parser ConfigCmdOpts
configCmdParser = hsubparser
  ( command "dump" (info (pure ConfigDump) (progDesc "Prints the server config in json format")))
