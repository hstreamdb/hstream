module HStream.Store.Admin.Types where

import           Control.Monad
import           Data.Int
import           Data.Text               (Text)
import           Options.Applicative
import qualified Text.Read               as Read

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
  :: forall b p. AA.Protocol p
  => SimpleNodesFilter
  -> (Maybe AA.NodesFilter -> AA.ThriftM p AA.SocketChannel AA.AdminAPI b)
  -> AA.ThriftM p AA.SocketChannel AA.AdminAPI (Either b [b])
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
