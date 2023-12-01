{-# LANGUAGE ApplicativeDo  #-}
{-# LANGUAGE CPP            #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Admin.Store.Types where

import qualified Control.Exception        as E
import           Control.Monad
import           Data.Char                (toLower)
import           Data.Int
import           Data.List                (intercalate)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text)
import qualified Data.Text                as Text
import           Options.Applicative
import qualified Options.Applicative      as O
import qualified Options.Applicative.Help as Opt
import qualified Text.Read                as Read
import           Z.Data.ASCII             (c2w)
import           Z.Data.CBytes            (CBytes, fromBytes)
import qualified Z.Data.Parser            as P
import qualified Z.Data.Text              as T
import qualified Z.Data.Vector            as V
import           Z.Data.Vector            (Bytes)

import qualified HStream.Admin.Store.API  as AA
import           HStream.Base             (withoutPrefix)
import qualified HStream.Logger           as Log
import           HStream.Store            (LogAttributes (..))
import qualified HStream.Store            as S

-------------------------------------------------------------------------------

data Cli = Cli
  { headerConfig :: AA.HeaderConfig AA.AdminAPI
  , logLevel     :: Log.Level
  , command      :: Command
  }

cliParser :: O.Parser Cli
cliParser = Cli
  <$> headerConfigParser
  <*> logLevelParser
  <*> commandParser

data Command
  = StatusCmd StatusOpts
  | NodesConfigCmd NodesConfigOpts
  | ConfigCmd ConfigCmdOpts
  | LogsCmd LogsConfigCmd
  | CheckImpactCmd CheckImpactOpts
  | MaintenanceCmd MaintenanceOpts
  | StartSQLReplCmd StartSQLReplOpts
  | AdminCommand AdminCommandOpts
  deriving (Show)

commandParser :: O.Parser Command
commandParser = O.hsubparser
  ( O.command "status" (O.info (StatusCmd <$> statusParser) (O.progDesc "Cluster status"))
 <> O.command "nodes-config" (O.info (NodesConfigCmd <$> nodesConfigParser) (O.progDesc "Manipulates the cluster's NodesConfig"))
 <> O.command "config" (O.info (ConfigCmd <$> configCmdParser) (O.progDesc "Commands about logdevice config"))
 <> O.command "logs" (O.info (LogsCmd <$> logsConfigCmdParser) (O.progDesc "Control the logs config of logdevice dynamically"))
 <> O.command "check-impact" (O.info (CheckImpactCmd <$> checkImpactOptsParser)
                                     (O.progDesc $ "Return true if performing"
                                                <> "operations to the given shards will cause loss of read/write availability or data loss.")
                             )
 <> O.command "maintenance" (O.info (MaintenanceCmd <$> maintenanceOptsParser)
                            (O.progDesc "Allows to manipulate maintenances in Maintenance Manager"))
 <> O.command "sql" (O.info (StartSQLReplCmd <$> startSQLReplOptsParser)
                      (O.progDesc "Start an interactive SQL shell"))
 <> O.command "admin-command"
              (O.info (AdminCommand <$> adminCommandOptsParser)
                      (O.progDesc "Internal admin commands, use with caution!"))
  )

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
  , statusVerbose   :: Bool
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
  <*> switch ( long "verbose"
            <> short 'v'
            <> help ( "Sets verbosity to true, this will also show "
                   <> "PACKAGE and UPTIME in the status result."
                    )
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
  ( O.command "show" (info (NodesConfigShow <$> nodesShowOptsParser)
                       (progDesc "Print tier's NodesConfig to stdout"))
 <> O.command "bootstrap" (info nodesConfigBootstrapParser
                            (progDesc "Finalize the bootstrapping and allow the cluster to be used"))
 <> O.command "shrink" (info (NodesConfigRemove <$> simpleNodesFilterParser)
                         (progDesc $ "Shrinks the cluster by removing nodes from"
                                  <> "the NodesConfig. This operation requires"
                                  <> "that the removed nodes are empty"))
 <> O.command "apply" (info (NodesConfigApply <$> nodesEditFileParser)
                        (progDesc $ "Apply the node configuration, The passed node "
                                 <> "configs should describe the desired final state"
                                 <> "of the node (not the diff)"))
  )

-------------------------------------------------------------------------------

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

prettyLocationScope :: AA.LocationScope -> String
prettyLocationScope = map toLower . withoutPrefix "LocationScope_" . show

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
  ( O.command "dump" (info (pure ConfigDump) (progDesc "Prints the server config in json format")))

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
    O.command "info" (info (InfoCmd <$> logIDParser)
                           (progDesc "Get current attributes of the tail/head of the log"))
 <> O.command "show" (info (ShowCmd <$> showLogsOptsParser)
                           (progDesc "Print the full logsconfig for this tier "))
 <> O.command "create" (info (CreateCmd <$> createLogsParser)
                             (progDesc ("Creates a log group under a specific directory"
                              <> " path in the LogsConfig tree. This only works"
                              <> " if the tier has LogsConfigManager enabled.")))
 <> O.command "rename" (info (RenameCmd <$> strOption (long "old-name" <> metavar "PATH")
                                        <*> strOption (long "new-name" <> metavar "PATH")
                                        <*> flag True False (long "warning"))
                             (progDesc "Renames a path in logs config to a new path"))
 <> O.command "remove" (info (RemoveCmd <$> removeLogsOptsParser)
                             (progDesc ("Removes a directory or a log-group under"
                              <> " a specific directory path in the LogsConfig tree."
                              <> " This will NOT delete the directory if it is not"
                              <> " empty by default, you need to use --recursive.")))
 <> O.command "trim" (info (LogsTrimCmd <$> option auto (long "id" <> metavar "INT" <> help "which log to trim")
                                        <*> option auto (long "lsn" <> metavar "INT" <> help "LSN"))
                           (progDesc "Trim the log up to and including the specified LSN"))
 <> O.command "set-range" (info (SetRangeCmd <$> setRangeOptsParser)
                                (progDesc ("This updates the log id range for the"
                                 <> " LogGroup under a specific directory path in"
                                 <> " the LogsConfig tree.")))
 <> O.command "update" (info (UpdateCmd <$> updateLogsOptsParser)
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
  , updateSyncedCopies      :: Maybe Int
  , updateBacklogDuration   :: Maybe Int
  , updateReplicateAcross   :: [(S.NodeLocationScope, Int)]
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
  <*> optional (option auto
      ( long "synced-copied"
     <> metavar "INT"
     <> help ( "The number of copies that must be acknowledged by storage nodes as"
            <> "synced to disk before the record is acknowledged to client as fully"
            <> "appended. Can be 0. Capped at replicationFactor."
             )
      ))
  <*> optional (option auto
      ( long "backlog"
     <> metavar "INT"
     <> help ( "Duration that a record can exist in the log before it expires and"
            <> "gets deleted (in senconds). Valid value must be at least 1 second.")
      ))
  <*> (many (option parseLogReplicateAcross
             ( long "replicate-across"
             <> metavar "SCOPE:REPLICATE"
             <> help "Cross-domain replication. Valid scopes: [node|rack|row|cluster|region|root]"
             )))
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

parseLogReplicateAcross :: ReadM (S.NodeLocationScope, Int)
parseLogReplicateAcross = eitherReader $ parse . V.packASCII
  where
    parse :: Bytes -> Either String (S.NodeLocationScope, Int)
    parse bs =
      case P.parse' parser bs of
        Left er -> Left $ "cannot parse value: " <> show er
        Right i -> Right i
    parser = do
      P.skipSpaces
      n <- P.takeTill (== c2w ':')
      P.char8 ':'
      s <- P.int
      P.skipSpaces
      return (bytesToNodeLocationScope n, s)

    bytesToNodeLocationScope :: Bytes -> S.NodeLocationScope
    bytesToNodeLocationScope bs =
      case fromBytes bs of
        "node"        -> S.NodeLocationScope_NODE
        "rack"        -> S.NodeLocationScope_RACK
        "row"         -> S.NodeLocationScope_ROW
        "cluster"     ->  S.NodeLocationScope_CLUSTER
        "data-center" -> S.NodeLocationScope_DATA_CENTER
        "region"      -> S.NodeLocationScope_REGION
        "root"        -> S.NodeLocationScope_ROOT
        _             -> S.NodeLocationScope_INVALID

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

-- TODO: consider moving this function to 'HStream.Store.Internal.LogDevice.LogAttributes'
logAttrsParser :: Parser S.LogAttributes
logAttrsParser = do
  logReplicationFactor <- S.defAttr1 <$> option auto
      ( long "replication-factor"
     <> metavar "INT"
     <> showDefault
     <> value 3
     -- TODO: fix here if `replicate_across` field added
     <> help "Number of nodes on which to persist a record."
      )
  logBacklogDuration <- maybe S.def (S.defAttr1 . Just) <$> optional (option auto
      ( long "backlog"
     <> metavar "INT"
     <> help ( "Duration that a record can exist in the log before it expires and "
            <> "gets deleted (in senconds). Valid value must be at least 1 second."
             )
      ))
  logAttrsExtras <- Map.fromList <$> many (option parseLogExtraAttr
      ( long "extra-attributes"
     <> metavar "STRING:STRING"
     <> help "Arbitrary fields that logdevice does not recognize."
      ))
  pure S.def{logReplicationFactor, logBacklogDuration, logAttrsExtras}

data CreateLogsOpts = CreateLogsOpts
  { path                :: CBytes
  , fromId              :: Maybe S.C_LogID
  , toId                :: Maybe S.C_LogID
  , isDirectory         :: Bool
  -- TODO
  -- , showVersion    :: Bool
  , createLogsOptsAttrs :: S.LogAttributes
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
  <*> logAttrsParser

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
  , ciShort                             :: Bool
  , maxUnavailableStorageCapacityPct    :: Int32
  , maxUnavailableSequencingCapacityPct :: Int32
  , skipCapacityChecks                  :: Bool
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
  <*> switch (long "short" <> help "Disables the long detailed description of the output")
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

-------------------------------------------------------------------------------

data MaintenanceOpts
  = MaintenanceListCmd MaintenanceListOpts
  | MaintenanceShowCmd MaintenanceShowOpts
  | MaintenanceApplyCmd MaintenanceApplyOpts
  | MaintenanceRemoveCmd MaintenanceRemoveOpts
  | MaintenanceTakeSnapShot Int64
  | MaintenanceMarkDataUnrecoverable MaintenanceMarkDataUnrecoverableOpts
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

data MaintenanceShowOpts = MaintenanceShowOpts
  { mntShowIds                :: [Text]
  , mntShowUsers              :: Maybe Text
  , mntShowNodeIndexes        :: [Int]
  , mntShowNodeNames          :: [Text]
  , mntShowBlocked            :: Bool
  , mntShowCompleted          :: Bool
  , mntShowInProgress         :: Bool
  , mntShowExpandShards       :: Bool
  , mntShowSafetyCheckResults :: Bool
  } deriving (Show)

data MaintenanceApplyOpts = MaintenanceApplyOpts
  { mntApplyReason                 :: Text
  , mntApplyNodeIndexes            :: [Int]
  , mntApplyNodeNames              :: [Text]
  , mntApplyShards                 :: [AA.ShardID]
  , mntApplyShardTargetState       :: AA.ShardOperationalState
  , mntApplySequencerNodeIndexes   :: [Int]
  , mntApplySequencerNodeNames     :: [Text]
  , mntApplyUser                   :: Maybe Text
  , mntApplyGroup                  :: Bool
  , mntApplySkipSafetyChecks       :: Bool
  , mntApplySkipCapacityChecks     :: Bool
  , mntApplyTtl                    :: Int
  , mntApplyAllowPassiveDrains     :: Bool
  , mntApplyForceRestoreRebuilding :: Bool
  , mntApplyPriority               :: AA.MaintenancePriority
  } deriving (Show)

data MaintenanceRemoveOpts = MaintenanceRemoveOpts
  { mntRemoveReason          :: Text
  , mntRemoveIds             :: [Text]
  , mntRemoveUsers           :: Maybe Text
  , mntRemoveNodeIndexes     :: [Int]
  , mntRemoveNodeNames       :: [Text]
  , mntRemoveBlocked         :: Bool
  , mntRemoveCompleted       :: Bool
  , mntRemoveInProgress      :: Bool
  , mntRemoveLogUser         :: Maybe Text
  , mntRemovePriority        :: Maybe AA.MaintenancePriority
  , mntRemoveIncludeInternal :: Bool
  } deriving (Show)

data MaintenanceMarkDataUnrecoverableOpts = MaintenanceMarkDataUnrecoverableOpts
  { mntMarkDataUnrecoverableUser   :: Maybe Text
  , mntMarkDataUnrecoverableReason :: Text
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
    O.command "list" (info (MaintenanceListCmd <$> maintenanceListOptsParser)
                       (progDesc "Prints compact list of maintenances applied to the cluster"))
 <> O.command "show" (info (MaintenanceShowCmd <$> maintenanceShowOptsParser)
                       (progDesc "Shows maintenances in expanded format with more information"))
 <> O.command "apply" (info (MaintenanceApplyCmd <$> maintenanceApplyOptsParser)
                        (progDesc "Applies new maintenance to Maintenance Manager"))
 <> O.command "remove" (info (MaintenanceRemoveCmd <$> maintenanceRemoveOptsParser)
                         (progDesc "Removes maintenances specified by filters."))
 <> O.command "take-snapshot" (info (MaintenanceTakeSnapShot <$> versionParser)
                                (progDesc $ "Asks the Admin Server to take an immediate snapshot of "
                                         <> "the maintenance internal log."))
 <> O.command "mark-data-unrecoverable"
        (info (MaintenanceMarkDataUnrecoverable <$> maintenanceMarkDataUnrecoverableOptsParser)
              (progDesc $ "[DANGER] Marks all the UNAVAILABLE shards (stuck on DATA_MIGRATION "
                       <> "storage state) as unrecoverable. This will advice the readers to not"
                       <> " wait for data on these shards and issue data loss gaps if necessary.")
        )

maintenanceMarkDataUnrecoverableOptsParser :: Parser MaintenanceMarkDataUnrecoverableOpts
maintenanceMarkDataUnrecoverableOptsParser = MaintenanceMarkDataUnrecoverableOpts
  <$> optional (strOption ( long "log-user"
                         <> metavar "STRING"
                         <> help "The user doing the removal operation, this is used for maintenance auditing and logging"))
  <*> strOption ( long "log-reason"
               <> metavar "STRING"
               <> help "The reason of removing the maintenance")

versionParser :: Parser Int64
versionParser =
  option auto ( long "min-version"
             <> value 0
             <> showDefault
             <> help ("The minimum version that you would like "
                   <> "to ensure that the snapshot has, 0 means any version"))

maintenanceRemoveOptsParser :: Parser MaintenanceRemoveOpts
maintenanceRemoveOptsParser = MaintenanceRemoveOpts
  <$> strOption ( long "log-reason"
               <> metavar "STRING"
               <> help "The reason of removing the maintenance")
  <*> many (strOption ( long "ids"
                     <> metavar "STRING"
                     <> help "Remove maintenances with specified Maintenance Group IDs"))
  <*> optional (strOption ( long "user"
                         <> metavar "STRING"
                         <> help "Remove maintenances created by specified user"))
  <*> many (option auto ( long "node-indexes"
                       <> metavar "INT"
                       <> help "Remove maintenances to specified nodes"))
  <*> many (strOption ( long "node-names"
                     <> metavar "STRING"
                     <> help "Remove maintenances to specified nodes"))
  <*> switch ( long "blocked"
            <> help "Remove maintenances which are blocked due to some reason")
  <*> switch ( long "completed"
            <> help "Remove maintenances which are finished")
  <*> switch ( long "in-progress"
            <> help "Remove maintenances which are in progress (including blocked)")
  <*> optional (strOption ( long "log-user"
                         <> metavar "STRING"
                         <> help ("The user doing the removal operation, this is"
                                    <> " used for maintenance auditing and logging")))
  <*> optional (option auto ( long "priority"
                           <> metavar "[imminent|high|medium|low]"
                           <> help "Remove maintenances with a given priority"))
  <*> switch ( long "remove-include-internal"
            <> help "Should we include internal maintenances in our removal request?")

maintenanceApplyOptsParser :: Parser MaintenanceApplyOpts
maintenanceApplyOptsParser = MaintenanceApplyOpts
  <$> strOption ( long "reason"
               <> metavar "STRING"
               <> help "Reason for logging and auditing")
  <*> many (option auto ( long "node-indexes"
                       <> metavar "INT..."
                       <> help "Apply maintenance to specified nodes"))
  <*> many (strOption ( long "node-names"
                     <> metavar "STRING..."
                     <> help "Apply maintenance to specified nodes"))
  <*> many (option parseShard ( long "shards"
                             <> metavar "NX:SY..."
                             <> help ("Apply maintenance to specified shards "
                                     <> "in notation like \"N1:S2\", \"N3:S4\", \"N165:S14\"")))
  <*> option auto ( long "shard_target_state"
                 <> metavar "[may-disappear|drained]"
                 <> value AA.ShardOperationalState_MAY_DISAPPEAR
                 <> showDefault
                 <> help "Shard Target State, either \"may-disappear\" or \"drained\"")
  <*> many (option auto ( long "sequencer-node-indexes"
                       <> metavar "INT..."
                       <> help "Apply maintenance to specified sequencers"))
  <*> many (strOption ( long "sequencer-node-names"
                     <> metavar "STRING..."
                     <> help "Apply maintenance to specified sequencers"))
  <*> optional (strOption ( long "user"
                         <> help "User for logging and auditing, by default taken from environment"))
  <*> flag False True ( long "no-group"
                     <> help "Defines should MaintenanceManager group this maintenance or not")
  <*> switch ( long "skip_safety_checks"
            <> help "If set safety-checks will be skipped")
  <*> switch ( long "skip_capacity_checks"
            <> help "If set capacity-checks will be skipped")
  <*> option auto ( long "ttl"
                 <> value 0
                 <> showDefault
                 <> help "If set this maintenance will be auto-expired after given number of seconds")
  <*> switch ( long "allow_passive_drains"
            <> help "If set passive drains will be allowed")
  <*> switch ( long "force_restore_rebuilding"
            <> help "Forces rebuilding to run in RESTORE mode")
  <*> option auto ( long "priority"
                  <> metavar "[imminent|high|medium|low]"
                  <> value AA.MaintenancePriority_MEDIUM
                  <> showDefault
                  <> help "Show only maintenances with a given priority")

instance Read AA.ShardOperationalState where
  readPrec = do
    i <- Read.lexP
    case i of
      Read.Ident "may-disappear" -> return AA.ShardOperationalState_MAY_DISAPPEAR
      Read.Ident "drained"       -> return AA.ShardOperationalState_DRAINED
      x -> errorWithoutStackTrace $ "cannot parse state" <> show x

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

maintenanceShowOptsParser :: Parser MaintenanceShowOpts
maintenanceShowOptsParser = MaintenanceShowOpts
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
  <*> switch ( long "expand-shards"
            <> help "Show also per-shard information")
  <*> switch ( long "show-safety-check-results"
            <> help "Show the entire output (includes all logs) of the impact check")

-------------------------------------------------------------------------------

data StartSQLReplOpts = StartSQLReplOpts
  { startSQLReplTimeout :: Int64
  , startSQLReplUseSsl  :: Bool
  , startSQLReplSQL     :: Maybe String
  } deriving (Show)

startSQLReplOptsParser :: Parser StartSQLReplOpts
startSQLReplOptsParser = StartSQLReplOpts
  <$> option auto ( long "timeout"
                 <> value 5000
                 <> showDefault
                 <> help ("Timeout when retrieve data from a LD node"
                       <> "through its admin command port, milliseconds"))
  <*> switch ( long "use-ssl"
            <> help "whether ldquery should connect to admin command port using SSL/TLS")
  <*> optional (strOption ( long "sql"
                         <> metavar "SQL"
                         <> short 'e'
                         <> help "Run sql expression non-interactively."
                          ))

-------------------------------------------------------------------------------

data AdminCommandOpts
  = AdminCommandRaw Text
  | AdminCommandCompactAll
  deriving (Show)

adminCommandOptsParser :: Parser AdminCommandOpts
adminCommandOptsParser = adminCommandOptsRawParser <|> adminCommandOptsQuickCmdParser

adminCommandOptsRawParser :: Parser AdminCommandOpts
adminCommandOptsRawParser = AdminCommandRaw
  <$> (strOption (long "command" <> short 'c'
               <> metavar "STRING" <> help "The command"))

adminCommandOptsQuickCmdParser :: Parser AdminCommandOpts
adminCommandOptsQuickCmdParser = hsubparser $
  O.command "compact-all"
            (O.info (pure AdminCommandCompactAll)
                    (O.progDesc "TODO: Quick to compact all"))

-------------------------------------------------------------------------------

logLevelParser :: Parser Log.Level
logLevelParser =
  option auto ( long "log-level" <> metavar "[critical|fatal|warning|info|debug]"
             <> showDefault <> value Log.INFO
             <> help "log level"
              )

parseShard :: ReadM AA.ShardID
parseShard = eitherReader parseShard'

allShards :: AA.ShardIndex
allShards = -1

-- | Parses a list of strings and intrepret as ShardID objects.
--
-- Accepted examples:
--     0 => ShardID(0, -1)
--     N0 => ShardID(0, -1)
--     0:2 => ShardID(0, 2)
--     N0:2 => ShardID(0, 2)
--     N0:S2 => ShardID(0, 2)
parseShard' :: String -> Either String AA.ShardID
parseShard' i = parse $ V.packASCII i
  where
    skipn w = w == c2w 'N' || w == c2w 'n'
    skips w = w == c2w 'S' || w == c2w 's'
    parse :: Bytes -> Either String AA.ShardID
    parse bs =
      case P.parse' parser bs of
        Left _  -> Left $ "cannot parse ShardID: " <> i
        Right o -> Right o

    parser = do
      (n, s) <- parseN <|> parseNS
      return $ AA.ShardID (AA.NodeID (Just n) Nothing Nothing) s

    parseN = do
      P.skipSpaces
      P.skipWhile skipn
      n <- P.int
      P.skipSpaces
      P.endOfInput
      return (n, allShards)

    parseNS = do
      P.skipSpaces
      P.skipWhile skipn
      n <- P.int
      P.char8 ':'
      P.skipWhile skips
      s <- P.int
      P.skipSpaces
      P.endOfInput
      return (n, s)

prettyShardID :: AA.ShardID -> String
prettyShardID AA.ShardID{..} =
    "N"
  <> maybe "x" show (AA.nodeID_node_index shardID_node)
  <> ":S"
  <> show shardID_shard_index

prettyShardStorageState :: AA.ShardStorageState -> String
prettyShardStorageState = withoutPrefix "ShardStorageState_" . show

prettySahrdDataHealth :: AA.ShardDataHealth -> String
prettySahrdDataHealth = withoutPrefix "ShardDataHealth_" . show

impacts2string :: [AA.OperationImpact] -> String
impacts2string xs = intercalate ", " $ map (withoutPrefix "OperationImpact_" . show) xs

-------------------------------------------------------------------------------

#define TAKE_BY_SPLITOR(NAME, FUN) \
  take##NAME :: Text.Text -> Text.Text -> Text.Text; \
  take##NAME splitor = FUN . Text.splitOn splitor; \
  take##NAME##' :: Text.Text -> String -> String; \
  take##NAME##' splitor = Text.unpack . take##NAME splitor . Text.pack;

TAKE_BY_SPLITOR(Last, last)
TAKE_BY_SPLITOR(Tail, Text.intercalate splitor . tail)

handleStoreError :: IO () -> IO ()
handleStoreError act =
  let putErr = Opt.putDoc . Opt.red . Opt.string . (\s -> "Error: " <> s <> "\n") .  T.toString . S.sseDescription
   in act `E.catches` [ E.Handler (\(S.StoreError ex) -> putErr ex)
                      , E.Handler (\(ex :: S.SomeHStoreException) -> print ex)
                      ]
