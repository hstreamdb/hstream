module HStream.Store.Admin.Types where

import           Control.Monad
import           Data.Int
import qualified Data.Map.Strict         as Map
import           Data.Text               (Text)
import           Options.Applicative
import qualified Text.Read               as Read
import           Z.Data.ASCII            (c2w)
import           Z.Data.CBytes           (CBytes, fromBytes)
import qualified Z.Data.Parser           as P
import           Z.Data.Vector           (Bytes)
import qualified Z.Data.Vector           as V

import qualified HStream.Store           as S
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
  = NodesConfigShow NodesShowOpts
  | NodesConfigBootstrap [ReplicationPropertyPair]
  | NodesConfigRemove SimpleNodesFilter
  | NodesConfigApply CBytes
  deriving (Show)

data NodesShowOpts = NodesShowOpts
  { nodesShowNodes :: SimpleNodesFilter
  , nodesShowFile  :: Maybe CBytes
  } deriving (Show)

nodesShowOptsParser :: Parser NodesShowOpts
nodesShowOptsParser = NodesShowOpts
  <$> simpleNodesFilterParser
  <*> optional (strOption ( long "file"
                        <> metavar "FILE"
                        <> short 'f'
                        <> help "The file to print config information"
                          ))

nodesEditFileParser :: Parser CBytes
nodesEditFileParser =
  strOption ( long "file"
           <> metavar "FILE"
           <> short 'f'
           <> help "The file to read configs"
            )

nodesConfigBootstrapParser :: Parser NodesConfigOpts
nodesConfigBootstrapParser = NodesConfigBootstrap
  <$> many (option auto ( long "metadata-replicate-across"
                       <> short 'r'
                       <> metavar "STRING:INT"
                       <> help "Defines cross-domain replication for metadata logs"))

nodesConfigParser :: Parser NodesConfigOpts
nodesConfigParser = hsubparser
  ( command "show" (info (NodesConfigShow <$> nodesShowOptsParser)
                     (progDesc "Print tier's NodesConfig to stdout"))
 <> command "bootstrap" (info nodesConfigBootstrapParser
                          (progDesc "Finalize the bootstrapping and allow the cluster to be used"))
 <> command "shrink" (info (NodesConfigRemove <$> simpleNodesFilterParser)
                       (progDesc $ "Shrinks the cluster by removing nodes from"
                                <> "the NodesConfig. This operation requires"
                                <> "that the removed nodes are empty"))
 <> command "apply" (info (NodesConfigApply <$> nodesEditFileParser)
                      (progDesc $ "Apply the node configuration, The passed node "
                               <> "configs should describe the desired final state"
                               <> "of the node (not the diff)"))
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

headerConfigParser :: Parser (AA.HeaderConfig AA.AdminAPI)
headerConfigParser = AA.HeaderConfig
  <$> strOption ( long "host"
               <> metavar "STRING"
               <> showDefault
               <> value "127.0.0.1"
               <> help "Admin server host, e.g. logdevice-admin-server-service"
                )
  <*> option auto ( long "port"
                 <> metavar "INT"
                 <> showDefault
                 <> value 6440
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

-------------------------------------------------------------------------------

data LogsConfigCmd
  = InfoCmd S.C_LogID
  | ShowCmd ShowLogsOpts
  | RenameCmd CBytes CBytes Bool
  | CreateCmd CreateLogsOpts
  | RemoveCmd RemoveLogsOpts
  | SetRangeCmd SetRangeOpts
  | UpdateCmd UpdateLogsOpts
  | LogsTrimCmd S.C_LogID S.LSN
  deriving (Show)

logsConfigCmdParser :: Parser LogsConfigCmd
logsConfigCmdParser = hsubparser $
    command "info" (info (InfoCmd <$> logIDParser)
                         (progDesc "Get current attributes of the tail/head of the log"))
 <> command "show" (info (ShowCmd <$> showLogsOptsParser)
                         (progDesc "Print the full logsconfig for this tier "))
 <> command "create" (info (CreateCmd <$> createLogsParser)
                           (progDesc ("Creates a log group under a specific directory"
                            <> " path in the LogsConfig tree. This only works"
                            <> " if the tier has LogsConfigManager enabled.")))
 <> command "rename" (info (RenameCmd <$> strOption (long "old-name" <> metavar "PATH")
                                      <*> strOption (long "new-name" <> metavar "PATH")
                                      <*> flag True False (long "warning"))
                           (progDesc "Renames a path in logs config to a new path"))
 <> command "remove" (info (RemoveCmd <$> removeLogsOptsParser)
                           (progDesc ("Removes a directory or a log-group under"
                            <> " a specific directory path in the LogsConfig tree."
                            <> " This will NOT delete the directory if it is not"
                            <> " empty by default, you need to use --recursive.")))
 <> command "trim" (info (LogsTrimCmd <$> option auto (long "id" <> metavar "INT" <> help "which log to trim")
                                      <*> option auto (long "lsn" <> metavar "INT" <> help "LSN"))
                         (progDesc "Trim the log up to and including the specified LSN"))
 <> command "set-range" (info (SetRangeCmd <$> setRangeOptsParser)
                              (progDesc ("This updates the log id range for the"
                               <> " LogGroup under a specific directory path in"
                               <> " the LogsConfig tree.")))
 <> command "update" (info (UpdateCmd <$> updateLogsOptsParser)
                           (progDesc (" This updates the LogAttributes for the"
                            <> " node (can be either a directory or a log group)"
                            <> " under a specific directory path in the "
                            <> " LogsConfig tree. ")))

data SetRangeOpts = SetRangeOpts
  { setRangePath    :: CBytes
  , setRangeStartId :: S.C_LogID
  , setRangeEndId   :: S.C_LogID
  } deriving (Show)

setRangeOptsParser :: Parser SetRangeOpts
setRangeOptsParser = SetRangeOpts
  <$> strOption ( long "path"
                <> metavar "PATH"
                <> help "Path of the the log group."
                )
  <*> option auto ( long "from"
                  <> metavar "INT"
                  <> help "The beginning of the logid range"
                  )
  <*> option auto ( long "to"
                  <> metavar "INT"
                  <> help "The end of the logid range"
                  )

data UpdateLogsOpts = UpdateLogsOpts
  { updatePath              :: CBytes
  , updateUnset             :: [CBytes]
  , updateReplicationFactor :: Maybe Int
  , updateExtras            :: Map.Map CBytes CBytes
  } deriving (Show)

updateLogsOptsParser :: Parser UpdateLogsOpts
updateLogsOptsParser = UpdateLogsOpts
  <$> strOption ( long "path"
                <> metavar "PATH"
                <> help "Path of the node you want to set attributes for"
                )
  <*> many (option str ( long "unset"
                        <> metavar "KEY"
                        <> help "The list of attribute names to unset"
                        ))
  <*> optional (option auto ( long "replication-factor"
                            <> metavar "INT"
                            <> help "number of nodes on which to persist a record"
                            ))
  <*> (Map.fromList <$> many (option parseLogExtraAttr
                              ( long "extra-attributes"
                              <> metavar "KEY:VALUE"
                              <> help "arbitrary fields that logdevice does not recognize"
                              )))

data RemoveLogsOpts = RemoveLogsOpts
  { rmPath      :: CBytes
  , rmRecursive :: Bool
  } deriving (Show)

removeLogsOptsParser :: Parser RemoveLogsOpts
removeLogsOptsParser = RemoveLogsOpts
    <$> strOption ( long "path"
                  <> metavar "PATH"
                  <> help "Path of the directory to be removed."
                  )
    <*> switch ( long "recursive"
               <> short 'r'
               <> help "Whether to remove the contents of the directory if it is not empty or not."
               )

data ShowLogsOpts = ShowLogsOpts
  { showPath     :: Maybe CBytes
  , showLogID    :: Maybe S.C_LogID
  , showMaxDepth :: Int
  , showVerbose  :: Bool
  } deriving (Show)

showLogsOptsParser :: Parser ShowLogsOpts
showLogsOptsParser = ShowLogsOpts
  <$> optional (strOption ( long "path"
                            <> metavar "PATH"
                            <> help "The path you want to print, if missing this prints the full tree"
                          ))
  <*> optional (option auto ( long "id"
                              <> metavar "LOGID"
                              <> help "Only the log-group that has this ID"
                            ))
  <*> option auto ( long "max-depth"
                  <> metavar "INT"
                  <> value 1000
                  <> showDefault
                  <> help "How many levels in the tree you want to see?"
                  )
  <*> switch ( long "verbose"
             <> short 'v'
             <> help "whether to print all the attributes or not"
             )

logIDParser :: Parser S.C_LogID
logIDParser = option auto ( long "id"
                          <> metavar "LOGID"
                          <> help "the log ID to query"
                          )

parseLogExtraAttr :: ReadM (CBytes, CBytes)
parseLogExtraAttr = eitherReader $ parse . V.packASCII
  where
    parse :: Bytes -> Either String (CBytes, CBytes)
    parse bs =
      case P.parse' parser bs of
        Left er -> Left $ "cannot parse value: " <> show er
        Right i -> Right i
    parser = do
      P.skipSpaces
      n <- P.takeTill (== c2w ':')
      P.char8 ':'
      s <- P.takeRemaining
      P.skipSpaces
      return (fromBytes n, fromBytes s)

logsAttrsParser :: Parser S.HsLogAttrs
logsAttrsParser = S.HsLogAttrs
  <$> option auto ( long "replication-factor"
                 <> metavar "INT"
                 <> showDefault
                 <> value 3
                 -- TODO: fix here if `replicate_across` field added
                 <> help "Number of nodes on which to persist a record. Default number is 3"
                  )
  <*> fmap Map.fromList (many (option parseLogExtraAttr ( long "extra-attributes"
                                                       <> metavar "STRING:STRING"
                                                       <> help "Arbitrary fields that logdevice does not recognize."
                                                        )
                              ))

data CreateLogsOpts = CreateLogsOpts
  { path           :: CBytes
  , fromId         :: Maybe S.C_LogID
  , toId           :: Maybe S.C_LogID
  , isDirectory    :: Bool
  -- TODO
  -- , showVersion    :: Bool
  , logsAttributes :: S.HsLogAttrs
  } deriving (Show)

createLogsParser :: Parser CreateLogsOpts
createLogsParser = CreateLogsOpts
  <$> strOption ( long "path"
               <> metavar "STRING"
               <> help "Path of the log group to be created."
                )
  <*> optional (option auto ( long "from"
                           <> metavar "INT"
                           <> help "The beginning of the logid range"
                            ))
  <*> optional (option auto ( long "to"
                           <> metavar "INT"
                           <> help "The end of the logid range"
                            ))
  <*> switch ( long "directory"
            <> help "Whether we should create a directory instead"
             )
  -- TODO
  -- <*> flag True False ( long "no-show-version"
  --                    <> help ("Should we show the version of the config tree after "
  --                    <> "the operation or not.")
  --                     )
  <*> logsAttrsParser

-------------------------------------------------------------------------------

-- FIXME: For now we only parse two state of AA.ShardStorageState
instance Read AA.ShardStorageState where
  readPrec = do
    i <- Read.lexP
    case i of
      Read.Ident "disabled"       -> return AA.ShardStorageState_DISABLED
      Read.Ident "readonly"       -> return AA.ShardStorageState_READ_ONLY
      x -> errorWithoutStackTrace $ "cannot parse value: " <> show x

data CheckImpactOpts = CheckImpactOpts
  { shards                              :: [AA.ShardID]
  , nodeIndexes                         :: [Int16]
  , nodeNames                           :: [Text]
  , targetState                         :: AA.ShardStorageState
  , safetyMargin                        :: [ReplicationPropertyPair]
  , skipMetaDataLogs                    :: Bool
  , skipInternalLogs                    :: Bool
  , logs                                :: [AA.Unsigned64]
  -- TODO : shorts :: Bool
  , maxUnavailableStorageCapacityPct    :: Int32
  , maxUnavailableSequencingCapacityPct :: Int32
  , skipCapacityChecks                  :: Bool
  , disableSequencers                   :: Bool
  } deriving (Show)

checkImpactOptsParser :: Parser CheckImpactOpts
checkImpactOptsParser = CheckImpactOpts
  <$> many (option parseShard ( long "shards"
                             <> metavar "NX:SY"
                             <> help ("List of strings in the format NX:SY where X is the "
                                   <> "node id and Y is the shard id")
                              ))
  <*> many (option auto ( long "node-index"
                       <> metavar "INT"
                       <> help "List of node indexes (TODO)"
                        ))
  <*> many (strOption ( long "node-name"
                     <> metavar "STRING"
                     <> help "List of node names either hosts or tw tasks (TODO)"
                      ))
  <*> option auto ( long "target-state"
                 <> metavar "[readonly|disabled]"
                 <> showDefault
                 <> value AA.ShardStorageState_DISABLED
                 <> help ("The storage state that we want to set the storage to. If you "
                       <> "would like to disable writes, then the target-state is readOnly. If you "
                       <> "would like to disable reads, then the target-state should be disabled")
                  )
  <*> many (option auto ( long "safety-margin"
                       <> metavar "STRING:INT"
                       <> help ("Extra domains which should be available. Format <scope><replication> "
                       <> "e.g. --safety-margin rack:0 --safety-margin node:1")
                        ))
  <*> option auto ( long "skip-metadata-logs"
                 <> metavar "BOOL"
                 <> showDefault
                 <> value False
                 <> help "Whether to check the metadata logs or not"
                  )
  <*> option auto ( long "skip-internal-logs"
                 <> metavar "BOOL"
                 <> showDefault
                 <> value False
                 <> help "whether to check the internal logs or not"
                  )
  <*> many (option auto ( long "logs"
                       <> metavar "INT"
                       <> help "If None, checks all logs, but you can specify the log-ids"
                        ))
  <*> option auto ( long "max-unavailable-storage-capacity-pct"
                 <> metavar "INT"
                 <> showDefault
                 <> value 25
                 <> help "The maximum percentage of storage capacity that can be unavailable"
                  )
  <*> option auto ( long "max-unavailable-sequencing-capacity-pct"
                 <> metavar "INT"
                 <> showDefault
                 <> value 25
                 <> help "The maximum percentage of sequencing capacity that can be unavailable"
                  )
  <*> option auto ( long "skip-capacity-checks"
                 <> metavar "BOOL"
                 <> showDefault
                 <> value False
                 <> help "Disable capacity checking altogether"
                  )
  <*> option auto ( long "disable-sequencers"
                 <> metavar "BOOL"
                 <> showDefault
                 <> value False
                 <> help "Do we want to validate if sequencers will be disabled on these nodes as well?"
                  )

-------------------------------------------------------------------------------

data MaintenanceOpts
  = MaintenanceListCmd MaintenanceListOpts
  deriving (Show)

data MaintenanceListOpts = MaintenanceListOpts
  { mntListIds         :: [Text]
  , mntListUsers       :: Maybe Text
  , mntListNodeIndexes :: [Int]
  , mntListNodeNames   :: [Text]
  , mntListBlocked     :: Bool
  , mntListCompleted   :: Bool
  , mntListInProgress  :: Bool
  , mntListPriority    :: Maybe AA.MaintenancePriority
  } deriving (Show)

instance Read AA.MaintenancePriority where
  readPrec = do
    i <- Read.lexP
    case i of
      Read.Ident "imminent" -> return AA.MaintenancePriority_IMMINENT
      Read.Ident "high"     -> return AA.MaintenancePriority_HIGH
      Read.Ident "medium"   -> return AA.MaintenancePriority_MEDIUM
      Read.Ident "low"      -> return AA.MaintenancePriority_LOW
      x -> errorWithoutStackTrace $ "cannot parse priority" <> show x

maintenanceOptsParser :: Parser MaintenanceOpts
maintenanceOptsParser = hsubparser $
  command "list" (info (MaintenanceListCmd <$> maintenanceListOptsParser)
                 (progDesc "Prints compact list of maintenances applied to the cluster"))

maintenanceListOptsParser :: Parser MaintenanceListOpts
maintenanceListOptsParser = MaintenanceListOpts
  <$> many (strOption ( long "ids"
                     <> metavar "STRING"
                     <> help "List only maintenances with specified Maintenance Group IDs"))
  <*> optional (strOption ( long "users"
                         <> metavar "STRING"
                         <> help "List only maintenances created by specified user"))
  <*> many (option auto ( long "node-indexes"
                       <> metavar "INT"
                       <> help "List only maintenances affecting specified nodes"))
  <*> many (strOption ( long "node-names"
                     <> metavar "STRING"
                     <> help "List only maintenances affecting specified nodes"))
  <*> switch ( long "blocked"
            <> help "List only maintenances which are blocked due to some reason")
  <*> switch ( long "completed"
            <> help "List only maintenances which are finished")
  <*> switch ( long "in-progress"
            <> help "List only maintenances which are in progress (including blocked)")
  <*> optional (option auto ( long "priority"
                           <> metavar "[imminent|high|medium|low]"
                           <> help "Show only maintenances with a given priority"))
