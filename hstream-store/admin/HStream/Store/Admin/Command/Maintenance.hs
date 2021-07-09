module HStream.Store.Admin.Command.Maintenance
  ( runMaintenanceCmd
  ) where

import           Data.List                  (intersect, nub, union)
import           Data.Maybe                 (fromMaybe, mapMaybe)
import           Data.Text                  (Text, intercalate, pack)
import qualified Data.Text                  as Text
import           System.IO.Unsafe           (unsafePerformIO)
import qualified Text.Layout.Table          as Table
import           Z.Data.CBytes              (unpack)
import           Z.IO.Time                  (SystemTime (MkSystemTime),
                                             formatSystemTime, simpleDateFormat)

import           HStream.Store.Admin.API
import qualified HStream.Store.Admin.API    as AA
import           HStream.Store.Admin.Format (simpleShowTable)
import           HStream.Store.Admin.Types

runMaintenanceCmd :: HeaderConfig AdminAPI -> MaintenanceOpts -> IO ()
runMaintenanceCmd conf (MaintenanceListCmd s) = runMaintenanceList conf s

data MaintenanceFilter = MaintenanceFilter
  { mntFilterIndexes    :: [Int]
  , mntFilterNames      :: [Text]
  , mntFilterBlocked    :: Bool
  , mntFilterCompleted  :: Bool
  , mntFilterInProgress :: Bool
  , mntFilterPriority   :: Maybe MaintenancePriority
  } deriving (Show)

mntFilter :: MaintenanceFilter -> [MaintenanceDefinition] -> [MaintenanceDefinition]
mntFilter MaintenanceFilter{..} = filter predicate
  where
    predicate mnt =
      let affectedNodes = maintenanceDefinition_sequencer_nodes mnt `union`
                          (nub . fmap shardID_node . maintenanceDefinition_shards $ mnt)
          indexes = mapMaybe nodeID_node_index affectedNodes
          names = mapMaybe nodeID_name affectedNodes
          progress = maintenanceDefinition_progress mnt
          priority = fromMaybe MaintenancePriority_MEDIUM $ maintenanceDefinition_priority mnt
      in
        (null mntFilterIndexes && null mntFilterNames
          || indexes `intersect` (fromIntegral <$> mntFilterIndexes) /= []
          || names `intersect` mntFilterNames /= [])
        && (not mntFilterBlocked || progress == MaintenanceProgress_BLOCKED_UNTIL_SAFE)
        && (not mntFilterCompleted || progress == MaintenanceProgress_COMPLETED)
        && (not mntFilterInProgress || progress == MaintenanceProgress_IN_PROGRESS)
        && maybe True (== priority) mntFilterPriority

getNodeState :: HeaderConfig AdminAPI -> NodeID -> IO [NodeState]
getNodeState conf nodeId = do
  resp <- AA.sendAdminApiRequest conf $
    getNodesState $ NodesStateRequest (Just $ NodesFilter (Just nodeId) Nothing Nothing) Nothing
  return $ nodesStateResponse_states resp

getShardProgress :: NodeState -> (Int, Int)
getShardProgress NodeState{..} =
  case nodeState_shard_states of
    Nothing -> (0, 0)
    Just shards ->
      let shardsInMaintenance = mapMaybe shardState_maintenance shards
          shardsTotal = length shardsInMaintenance
          shardsInProgress = length $
            filter ((== MaintenanceStatus_COMPLETED) . shardMaintenanceProgress_status) shardsInMaintenance
      in (shardsInProgress, shardsTotal)

getSequencerProgress :: NodeState -> (Int, Int)
getSequencerProgress NodeState{..} =
  case nodeState_sequencer_state of
    Nothing -> (0, 0)
    Just sequencer ->
      case sequencerState_maintenance sequencer of
        Nothing -> (0, 0)
        Just state ->
          if sequencerMaintenanceProgress_status state == MaintenanceStatus_COMPLETED
          then (1, 1)
          else (0, 1)

getStatesProgress :: (NodeState -> (Int, Int)) -> [NodeState] -> (Int, Int)
getStatesProgress f = foldr (pairAdd . f) (0,0)
  where pairAdd (a, b) (c, d) = (a + c, b + d)

runMaintenanceList :: HeaderConfig AdminAPI -> MaintenanceListOpts -> IO ()
runMaintenanceList conf MaintenanceListOpts{..} = do
  resp <- AA.sendAdminApiRequest conf $
    getMaintenances $ MaintenancesFilter mntListIds mntListUsers

  let titles = ["MNT. ID", "AFFECTED", "STATUS", "SHARDS", "SEQUENCERS",
                "PRIORITY", "CREATED BY", "REASON", "CREATED AT", "EXPIRES IN"]
  let getShardNodes = nub . fmap shardID_node . maintenanceDefinition_shards
  let shardProgress mnt = unsafePerformIO $ getStatesProgress getShardProgress . concat
        <$> traverse (getNodeState conf) (getShardNodes mnt)
  let sequencerProgress mnt = unsafePerformIO $ getStatesProgress getSequencerProgress . concat
        <$> traverse (getNodeState conf) (maintenanceDefinition_sequencer_nodes mnt)
  -- Affected nodes include the sequencer nodes and the nodes where affected shards reside
  let getAffectedNodesIndex mnt = mapMaybe nodeID_node_index $
        maintenanceDefinition_sequencer_nodes mnt `union` getShardNodes mnt
  let getShards mnt = printShardOperationalState (maintenanceDefinition_shard_target_state mnt)
        <> "(" <> (show . fst . shardProgress $ mnt)
        <> "/" <> (show . snd . shardProgress $ mnt) <> ")"
  let getSequencers mnt = printSequencingState (maintenanceDefinition_sequencer_target_state mnt)
        <> "(" <> (show . fst . sequencerProgress $ mnt)
        <> "/" <> (show . snd . sequencerProgress $ mnt) <> ")"
  let fmtTimeStamp ts = unsafePerformIO $ formatSystemTime simpleDateFormat (MkSystemTime (ts `div` 1000) 0)
  let lenses = [ Text.unpack . fromMaybe "" . maintenanceDefinition_group_id
               , Text.unpack . intercalate "," . map (\x -> "N" <> pack (show x)) . nub . getAffectedNodesIndex
               , printMaintenanceProgress . maintenanceDefinition_progress
               , getShards
               , getSequencers
               , printPriority . fromMaybe MaintenancePriority_MEDIUM . maintenanceDefinition_priority
               , Text.unpack . maintenanceDefinition_user
               , Text.unpack . maintenanceDefinition_reason
               , maybe "-" (unpack . fmtTimeStamp) . maintenanceDefinition_created_on
               , maybe "-" (unpack . fmtTimeStamp) . maintenanceDefinition_expires_on
               ]
  let mntListFilter = mntFilter MaintenanceFilter
                      { mntFilterIndexes = mntListNodeIndexes
                      , mntFilterNames = mntListNodeNames
                      , mntFilterBlocked = mntListBlocked
                      , mntFilterCompleted = mntListCompleted
                      , mntFilterInProgress = mntListInProgress
                      , mntFilterPriority = mntListPriority
                      }
  let stats = (\s -> map ($ s) lenses) <$> mntListFilter (maintenanceDefinitionResponse_maintenances resp)
  if null stats
    then putStrLn "No maintenances matching given criteria"
    else putStrLn $ simpleShowTable (map (, 20, Table.left) titles) stats

printPriority :: AA.MaintenancePriority -> String
printPriority MaintenancePriority_IMMINENT     = "IMMINENT"
printPriority MaintenancePriority_HIGH         = "HIGH"
printPriority MaintenancePriority_MEDIUM       = "MEDIUM"
printPriority MaintenancePriority_LOW          = "LOW"
printPriority (MaintenancePriority__UNKNOWN n) = "UNKNOWN" <> show n

printSequencingState :: SequencingState -> String
printSequencingState SequencingState_ENABLED      = "ENABLED"
printSequencingState SequencingState_BOYCOTTED    = "BOYCOTTED"
printSequencingState SequencingState_DISABLED     = "DISABLED"
printSequencingState SequencingState_UNKNOWN      = "UNKNOWN"
printSequencingState (SequencingState__UNKNOWN n) = "UNKNOWN" <> show n

printShardOperationalState :: ShardOperationalState -> String
printShardOperationalState ShardOperationalState_UNKNOWN = "UNKNOWN"
printShardOperationalState ShardOperationalState_ENABLED = "ENABLED"
printShardOperationalState ShardOperationalState_MAY_DISAPPEAR = "MAY_DISAPPEAR"
printShardOperationalState ShardOperationalState_DRAINED = "DRAINED"
printShardOperationalState ShardOperationalState_MIGRATING_DATA = "MIGRATING_DATA"
printShardOperationalState ShardOperationalState_ENABLING = "ENABLING"
printShardOperationalState ShardOperationalState_PROVISIONING = "PROVISIONING"
printShardOperationalState ShardOperationalState_PASSIVE_DRAINING = "PASSIVE_DRAINING"
printShardOperationalState ShardOperationalState_INVALID = "INVALID"
printShardOperationalState (ShardOperationalState__UNKNOWN n) = "UNKNOWN" <> show n

printMaintenanceProgress :: MaintenanceProgress -> String
printMaintenanceProgress MaintenanceProgress_UNKNOWN = "UNKNOWN"
printMaintenanceProgress MaintenanceProgress_BLOCKED_UNTIL_SAFE = "BLOCKED_UNTIL_SAFE"
printMaintenanceProgress MaintenanceProgress_IN_PROGRESS = "IN_PROGRESS"
printMaintenanceProgress MaintenanceProgress_COMPLETED = "COMPLETED"
printMaintenanceProgress (MaintenanceProgress__UNKNOWN n) = "UNKNOWN" <> show n
