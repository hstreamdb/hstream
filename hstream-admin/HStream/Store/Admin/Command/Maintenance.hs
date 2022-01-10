module HStream.Store.Admin.Command.Maintenance
  ( runMaintenanceCmd
  ) where

import           Control.Monad             (forM_, guard, unless, when, (<=<))
import           Data.Char                 (toUpper)
import           Data.Int                  (Int64)
import           Data.List                 (group, intercalate, intersect, nub,
                                            sort, union)
import qualified Data.Map                  as Map
import           Data.Maybe                (catMaybes, fromMaybe, mapMaybe)
import           Data.Set                  (elems, member)
import           Data.Text                 (Text, pack)
import qualified Data.Text                 as Text
import           System.IO                 (hFlush, stdout)
import           System.IO.Unsafe          (unsafePerformIO)
import qualified Text.Layout.Table         as Table
import           Z.Data.CBytes             (CBytes, unpack)
import           Z.IO                      (getEnv)
import           Z.IO.Time                 (SystemTime (MkSystemTime),
                                            formatSystemTime, simpleDateFormat)

import           HStream.Store.Admin.API
import qualified HStream.Store.Admin.API   as AA
import           HStream.Store.Admin.Types
import           HStream.Utils             (simpleShowTable)


runMaintenanceCmd :: HeaderConfig AdminAPI -> MaintenanceOpts -> IO ()
runMaintenanceCmd conf (MaintenanceListCmd s)               = runMaintenanceList conf s
runMaintenanceCmd conf (MaintenanceShowCmd s)               = runMaintenanceShow conf s
runMaintenanceCmd conf (MaintenanceApplyCmd s)              = runMaintenanceApply conf s
runMaintenanceCmd conf (MaintenanceRemoveCmd s)             = runMaintenanceRemove conf s
runMaintenanceCmd conf (MaintenanceTakeSnapShot version)    = takeSnapshot conf version
runMaintenanceCmd conf (MaintenanceMarkDataUnrecoverable s) = markDataUnrecoverable conf s

-- the maintenance id and user options are passed to the API as a filter (see MaintenancesFilter)
-- so we only run the filter with the rest of options here
data MaintenanceFilter = MaintenanceFilter
  { mntFilterIndexes         :: [Int]
  , mntFilterNames           :: [Text]
  , mntFilterBlocked         :: Bool
  , mntFilterCompleted       :: Bool
  , mntFilterInProgress      :: Bool
  , mntFilterPriority        :: Maybe MaintenancePriority
  , mntFilterIncludeInternal :: Bool
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
        && (not mntFilterInProgress || progress == MaintenanceProgress_IN_PROGRESS
             || progress == MaintenanceProgress_BLOCKED_UNTIL_SAFE)
        && maybe True (== priority) mntFilterPriority
        && (mntFilterIncludeInternal || maintenanceDefinition_user mnt /= "_internal_")

-------------------------------------------------------------------------------
-- maintenance list

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

getShardNodes :: MaintenanceDefinition -> [NodeID]
getShardNodes = nub . fmap shardID_node . maintenanceDefinition_shards

runMaintenanceList :: HeaderConfig AdminAPI -> MaintenanceListOpts -> IO ()
runMaintenanceList conf MaintenanceListOpts{..} = do
  resp <- AA.sendAdminApiRequest conf $
    getMaintenances $ MaintenancesFilter mntListIds mntListUsers

  let titles = ["MNT. ID", "AFFECTED", "STATUS", "SHARDS", "SEQUENCERS",
                "PRIORITY", "CREATED BY", "REASON", "CREATED AT", "EXPIRES IN"]
  let shardProgress mnt = unsafePerformIO $ getStatesProgress getShardProgress . concat
        <$> traverse (getNodeState conf) (getShardNodes mnt)
  let sequencerProgress mnt = unsafePerformIO $ getStatesProgress getSequencerProgress . concat
        <$> traverse (getNodeState conf) (maintenanceDefinition_sequencer_nodes mnt)
  -- Affected nodes include the sequencer nodes and the nodes where affected shards reside
  let getAffectedNodesIndex mnt = mapMaybe nodeID_node_index $
        maintenanceDefinition_sequencer_nodes mnt `union` getShardNodes mnt
  let getShards mnt = printInfo (maintenanceDefinition_shard_target_state mnt)
        <> "(" <> (show . fst . shardProgress $ mnt)
        <> "/" <> (show . snd . shardProgress $ mnt) <> ")"
  let getSequencers mnt = printInfo (maintenanceDefinition_sequencer_target_state mnt)
        <> "(" <> (show . fst . sequencerProgress $ mnt)
        <> "/" <> (show . snd . sequencerProgress $ mnt) <> ")"
  let lenses = [ Text.unpack . fromMaybe "" . maintenanceDefinition_group_id
               , Text.unpack . Text.intercalate "," . map (\x -> "N" <> pack (show x)) . nub . getAffectedNodesIndex
               , printInfo . maintenanceDefinition_progress
               , getShards
               , getSequencers
               , printInfo . fromMaybe MaintenancePriority_MEDIUM . maintenanceDefinition_priority
               , Text.unpack . maintenanceDefinition_user
               , Text.unpack . maintenanceDefinition_reason
               , maybe "-" (unpack . printTimeStamp) . maintenanceDefinition_created_on
               , maybe "-" (unpack . printTimeStamp) . maintenanceDefinition_expires_on
               ]
  let mntListFilter = mntFilter MaintenanceFilter
                      { mntFilterIndexes         = mntListNodeIndexes
                      , mntFilterNames           = mntListNodeNames
                      , mntFilterBlocked         = mntListBlocked
                      , mntFilterCompleted       = mntListCompleted
                      , mntFilterInProgress      = mntListInProgress
                      , mntFilterPriority        = mntListPriority
                      , mntFilterIncludeInternal = True
                      }
  let stats = (\s -> map ($ s) lenses) <$> mntListFilter (maintenanceDefinitionResponse_maintenances resp)
  if null stats
    then putStrLn "No maintenances matching given criteria"
    else putStrLn $ simpleShowTable (map (, 20, Table.left) titles) stats

-------------------------------------------------------------------------------
-- maintenance show

runMaintenanceShow :: HeaderConfig AdminAPI -> MaintenanceShowOpts -> IO ()
runMaintenanceShow conf MaintenanceShowOpts{..} = do
  resp <- AA.sendAdminApiRequest conf $
    getMaintenances $ MaintenancesFilter mntShowIds mntShowUsers
  let mntListFilter = mntFilter MaintenanceFilter
                      { mntFilterIndexes         = mntShowNodeIndexes
                      , mntFilterNames           = mntShowNodeNames
                      , mntFilterBlocked         = mntShowBlocked
                      , mntFilterCompleted       = mntShowCompleted
                      , mntFilterInProgress      = mntShowInProgress
                      , mntFilterPriority        = Nothing
                      , mntFilterIncludeInternal = True
                      }
  maintenanceShowMaintenanceDefs conf mntShowExpandShards mntShowSafetyCheckResults
    . mntListFilter . maintenanceDefinitionResponse_maintenances $ resp

maintenanceShowMaintenanceDefs :: HeaderConfig AdminAPI -> Bool -> Bool -> [MaintenanceDefinition] -> IO ()
maintenanceShowMaintenanceDefs conf expand safetyCheck defs = do
  forM_ defs $ \mnt -> do
    putStrLn $ maintenanceShowOverview mnt
    putStrLn "\nShard Maintenances:"
    shardNodesStates <- concat <$> traverse (getNodeState conf) (getShardNodes mnt)
    if expand
      then do
      forM_ shardNodesStates $ \node -> do
        putStrLn $ "N" <> show (nodeState_node_index node)
          <> "(" <> Text.unpack (nodeConfig_name (nodeState_config node)) <> ")"
        case nodeState_shard_states node of
          Just shards -> putStrLn $ maintenanceShowExpandShards shards
          Nothing     -> putStrLn ""
      else putStrLn $ maintenanceShowShards shardNodesStates
    putStrLn "\nSequencer Maintenances:"
    sequencerNodesStates <- concat <$> traverse (getNodeState conf) (maintenanceDefinition_sequencer_nodes mnt)
    putStrLn $ maintenanceShowSequencer sequencerNodesStates
    when (safetyCheck
          && maintenanceDefinition_progress mnt == MaintenanceProgress_BLOCKED_UNTIL_SAFE) $ do
      putStrLn "\nSafety Check Impact:"
      putStrLn "please run the command check-impact manually"
    putStrLn "\n---"

maintenanceShowExpandShards :: [ShardState] -> String
maintenanceShowExpandShards shards =
  let titles = ["SHARD INDEX", "CURRENT STATE", "TARGET STATE", "MAINTENANCE STATUS", "LAST UPDATED"]
      lenses = [ printInfo . shardState_current_operational_state
               , maybe "-" (intercalate "," . fmap printInfo . elems . shardMaintenanceProgress_target_states)
                 . shardState_maintenance
               , maybe "-" (printInfo . shardMaintenanceProgress_status) . shardState_maintenance
               , maybe "-" (unpack . printTimeStamp . shardMaintenanceProgress_last_updated_at) . shardState_maintenance
               ]
      stats = (\s -> ($ s) <$> lenses) <$> shards
      statsWithIndex = zipWith (\i s -> show (i :: Int) : s) [1..] stats
  in simpleShowTable (map (, 30, Table.left) titles) statsWithIndex


maintenanceShowShards :: [NodeState] -> String
maintenanceShowShards nodes =
  let titles = ["NODE INDEX", "NODE NAME", "LOCATION", "TARGET STATE", "CURRENT STATE", "MAINTENANCE STATUS", "LAST UPDATED"]
      lenses = [ show . nodeState_node_index
               , Text.unpack . nodeConfig_name . nodeState_config
               , printLoc . nodeConfig_location_per_scope . nodeState_config
               , printCounter printInfo . shardsTargetState
               , printCounter printInfo . shardsCurrentState
               , printCounter printInfo . shardsMaintenanceState
               , maybe "-" (unpack . printTimeStamp) . shardsLastUpdatedAt
               ]
      stats  = (\s -> map ($ s) lenses) <$> nodes
  in simpleShowTable (map (, 30, Table.left) titles) stats

shardsCurrentState :: NodeState -> Map.Map ShardOperationalState Int
shardsCurrentState = maybe Map.empty (counter . fmap shardState_current_operational_state) . nodeState_shard_states

shardsMaintenanceState :: NodeState -> Map.Map MaintenanceStatus Int
shardsMaintenanceState nodeState =
  case nodeState_shard_states nodeState of
    Nothing -> Map.empty
    Just shardStates ->
      case sequenceA $ shardState_maintenance <$> shardStates of
        Nothing       -> Map.empty
        Just progress -> counter $ shardMaintenanceProgress_status <$> progress

shardsTargetState :: NodeState -> Map.Map ShardOperationalState Int
shardsTargetState nodeState =
  case nodeState_shard_states nodeState of
    Nothing -> Map.empty
    Just shardStates ->
      case sequenceA $ shardState_maintenance <$> shardStates of
        Nothing -> Map.empty
        Just progress -> counter (concat (elems . shardMaintenanceProgress_target_states <$> progress))

shardsLastUpdatedAt :: NodeState -> Maybe Timestamp
shardsLastUpdatedAt nodeState =
  case nodeState_shard_states nodeState of
    Nothing -> Nothing
    Just shardStates ->
      if null shardStates then Nothing
      else shardMaintenanceProgress_last_updated_at <$> shardState_maintenance (head shardStates)

maintenanceShowSequencer :: [NodeState] -> String
maintenanceShowSequencer nodes =
  let readFromMaintenance f = f <.> (sequencerState_maintenance <=< nodeState_sequencer_state)
      titles = ["NODE INDEX", "NODE NAME", "LOCATION", "TARGET STATE", "CURRENT STATE", "MAINTENANCE STATUS", "LAST UPDATED"]
      lenses = [ show . nodeState_node_index
               , Text.unpack . nodeConfig_name . nodeState_config
               , printLoc . nodeConfig_location_per_scope . nodeState_config
               , maybe "-" printInfo . readFromMaintenance sequencerMaintenanceProgress_target_state
               , maybe "-" (printInfo . sequencerState_state) . nodeState_sequencer_state
               , maybe "-" printInfo . readFromMaintenance sequencerMaintenanceProgress_status
               , maybe "-" (unpack . printTimeStamp) . readFromMaintenance sequencerMaintenanceProgress_last_updated_at
               ]
      stats  = (\s -> map ($ s) lenses) <$> nodes
  in simpleShowTable (map (, 200, Table.left) titles) stats

maintenanceShowOverview :: MaintenanceDefinition -> String
maintenanceShowOverview mnt =
  let table = [ ["Maintenance ID:", maybe "-" Text.unpack $ maintenanceDefinition_group_id mnt]
              , ["Priority:", printInfo . fromMaybe MaintenancePriority_MEDIUM . maintenanceDefinition_priority $ mnt]
              , ["Affected:", affected]
              , ["Status:", printInfo . maintenanceDefinition_progress $ mnt]
              , ["Impact Result:", result]
              , ["Created By:", Text.unpack . maintenanceDefinition_user $ mnt]
              , ["Reason:", Text.unpack . maintenanceDefinition_reason $ mnt]
              , ["Extras:", extras]
              , ["Created On:", maybe "-" (unpack . printTimeStamp) $ maintenanceDefinition_created_on mnt]
              , ["Expires On:", maybe "-" (unpack . printTimeStamp) $ maintenanceDefinition_expires_on mnt]
              , ["Skip Safety Checks:", show $ maintenanceDefinition_skip_safety_checks mnt]
              , ["Skip Capacity Checks:", show $ maintenanceDefinition_skip_capacity_checks mnt]
              , ["Allow Passive Drains:", show $ maintenanceDefinition_allow_passive_drains mnt]
              , ["RESTORE rebuilding enforced:", show $ maintenanceDefinition_force_restore_rebuilding mnt]
              ]
      result = if maintenanceDefinition_progress mnt `elem`
                  [MaintenanceProgress_UNKNOWN, MaintenanceProgress_BLOCKED_UNTIL_SAFE]
               then maybe "-" (Data.List.intercalate "," . fmap printInfo . checkImpactResponse_impact)
                    $ maintenanceDefinition_last_check_impact_result mnt
               else "-"
      affected = (show . length . maintenanceDefinition_shards $ mnt)
                 <> " shards on "
                 <> (show . length . getShardNodes $ mnt)
                 <> " nodes, "
                 <> (show . length . maintenanceDefinition_sequencer_nodes $ mnt)
                 <> " sequencers"
      extras = if Map.null (maintenanceDefinition_extras mnt)
               then "-"
               else concatMap (\(k, v) -> Text.unpack k <> ":" <> Text.unpack v)
                    $ Map.toList (maintenanceDefinition_extras mnt)
      colSpec = Table.column Table.expand Table.left Table.def Table.def
  in Table.gridString [colSpec, colSpec] table

-------------------------------------------------------------------------------
-- maintenance apply

nodeIDFromIndex :: (Integral a) => a -> NodeID
nodeIDFromIndex index =  NodeID (Just $ fromIntegral index) Nothing Nothing

nodeIDFromName :: Text -> NodeID
nodeIDFromName name = NodeID Nothing Nothing (Just name)

isStorage :: NodeState -> Bool
isStorage ns = Role_STORAGE `member` nodeConfig_roles (nodeState_config ns)

isSequencer :: NodeState -> Bool
isSequencer ns = Role_SEQUENCER `member` nodeConfig_roles (nodeState_config ns)

runMaintenanceApply :: HeaderConfig AdminAPI -> MaintenanceApplyOpts -> IO ()
runMaintenanceApply conf MaintenanceApplyOpts{..} = do
  let nodes = (nodeIDFromIndex <$> mntApplyNodeIndexes) `union` (nodeIDFromName <$> mntApplyNodeNames)
  let sequencers = (nodeIDFromIndex <$> mntApplySequencerNodeIndexes)
                   `union` (nodeIDFromName <$> mntApplySequencerNodeNames)
  nodeStates <- concat <$> traverse (getNodeState conf) nodes
  let storageNodes = filter isStorage nodeStates
  let sequencerNodes = filter isSequencer nodeStates
  -- use $USER from the environment variables if --user is left unspecified
  -- hardcode the user to "unknown" if the env var is not set
  user <- maybe (maybe "unknown-user" (pack . unpack) <$> getEnv "USER") pure mntApplyUser
  let maintenance = MaintenanceDefinition
        { -- the affected shards include shards specified by user with the option --shards
          -- and all the shards on the nodes specified by the --node-indexes and --node-names options
          maintenanceDefinition_shards = mntApplyShards
            `union` ((`ShardID` aLL_SHARDS) . nodeIDFromIndex . nodeState_node_index <$> storageNodes)
        , maintenanceDefinition_shard_target_state = mntApplyShardTargetState
          -- likewise for sequencers
        , maintenanceDefinition_sequencer_nodes = sequencers
            `union` (nodeIDFromIndex . nodeState_node_index <$> sequencerNodes)
        , maintenanceDefinition_sequencer_target_state = SequencingState_DISABLED
        , maintenanceDefinition_user = user
        , maintenanceDefinition_reason = mntApplyReason
        , maintenanceDefinition_extras = Map.empty
        , maintenanceDefinition_skip_safety_checks = mntApplySkipSafetyChecks
        , maintenanceDefinition_force_restore_rebuilding = mntApplyForceRestoreRebuilding
        , maintenanceDefinition_group = mntApplyGroup
        , maintenanceDefinition_ttl_seconds = fromIntegral mntApplyTtl
        , maintenanceDefinition_allow_passive_drains = mntApplyAllowPassiveDrains
        , maintenanceDefinition_group_id = Nothing
        , maintenanceDefinition_last_check_impact_result = Nothing
        , maintenanceDefinition_expires_on = Nothing
        , maintenanceDefinition_created_on = Nothing
        , maintenanceDefinition_progress = MaintenanceProgress_UNKNOWN
        , maintenanceDefinition_priority = Just mntApplyPriority
        , maintenanceDefinition_skip_capacity_checks = mntApplySkipCapacityChecks
        }
  resp <- AA.sendAdminApiRequest conf $ applyMaintenance maintenance
  maintenanceShowMaintenanceDefs conf False False $ maintenanceDefinitionResponse_maintenances resp

-------------------------------------------------------------------------------
-- MaintenanceRemove

runMaintenanceRemove :: HeaderConfig AdminAPI -> MaintenanceRemoveOpts -> IO ()
runMaintenanceRemove conf MaintenanceRemoveOpts{..} = do
  resp <- AA.sendAdminApiRequest conf $
    getMaintenances $ MaintenancesFilter mntRemoveIds mntRemoveUsers
  let mntDefsFilter = mntFilter MaintenanceFilter
                      { mntFilterIndexes         = mntRemoveNodeIndexes
                      , mntFilterNames           = mntRemoveNodeNames
                      , mntFilterBlocked         = mntRemoveBlocked
                      , mntFilterCompleted       = mntRemoveCompleted
                      , mntFilterInProgress      = mntRemoveInProgress
                      , mntFilterPriority        = mntRemovePriority
                      , mntFilterIncludeInternal = mntRemoveIncludeInternal
                      }
  let mntDefs = mntDefsFilter $ maintenanceDefinitionResponse_maintenances resp
  let msgNotFound = if mntRemoveIncludeInternal
                    then "No maintenances matching given criteria"
                    else "No maintenances matching given criteria, did you "
                         <> "mean to target internal maintenances?\nUse "
                         <> "`include-internal-maintenances` for this."
  let msgFound = if mntRemoveIncludeInternal
                 then "\n\nWARNING: You might be deleting internal maintenances.\n "
                      <> "This is a DANGEROUS operation. Only proceed if you are "
                      <> "absolutely sure."
                 else "NOTE: Your query might have matched internal maintenances.\n"
                      <> "We have excluded them from your remove request for "
                      <> "safety. If you really need to remove internal maintenances,"
                      <> " You need to to set `include-internal-maintenances to "
                      <> "True`"
  let msgWarn = if mntRemoveIncludeInternal
                then "Take the RISK? [Y/n]"
                else "Continue? [Y/n]"
  if null mntDefs
    then putStrLn msgNotFound
    else do
    putStrLn "You are going to remove following maintenances:"
    maintenanceShowMaintenanceDefs conf False False mntDefs

    putStrLn msgFound
    c <- putStrLn msgWarn >> hFlush stdout >> getChar
    guard (toUpper c == 'Y')
    let groupIds = catMaybes $ maintenanceDefinition_group_id <$> mntDefs
    putStrLn $ "removing" <> show groupIds
    user <- maybe (maybe "unknown-user" (pack . unpack) <$> getEnv "USER") pure mntRemoveLogUser
    let removeFilter = MaintenancesFilter groupIds mntRemoveUsers
    removeResp <- AA.sendAdminApiRequest conf $
      removeMaintenances $ RemoveMaintenancesRequest removeFilter user mntRemoveReason
    let removeIds = catMaybes $ maintenanceDefinition_group_id
          <$> removeMaintenancesResponse_maintenances removeResp
    putStrLn $ "removed" <> show removeIds

-------------------------------------------------------------------------------
-- maintenance mark-data-unrecoverable

markDataUnrecoverable :: HeaderConfig AdminAPI -> MaintenanceMarkDataUnrecoverableOpts -> IO ()
markDataUnrecoverable conf MaintenanceMarkDataUnrecoverableOpts{..} = do
  user <- maybe (maybe "unknown-user" (pack . unpack) <$> getEnv "USER") pure mntMarkDataUnrecoverableUser
  resp <- AA.sendAdminApiRequest conf $ markAllShardsUnrecoverable
          $ MarkAllShardsUnrecoverableRequest user mntMarkDataUnrecoverableReason
  let success = markAllShardsUnrecoverableResponse_shards_succeeded resp
  let failure = markAllShardsUnrecoverableResponse_shards_failed resp
  unless (null success) $
    putStrLn $ "succeeded: " <> show success
  unless (null failure) $
    putStrLn $ "failed: " <> show failure
  when (null success && null failure) $
    putStrLn "No UNAVAILABLE shards to mark unrecoverable!"

-------------------------------------------------------------------------------
-- maintenance take-snapshot

takeSnapshot :: HeaderConfig AdminAPI -> Int64 -> IO ()
takeSnapshot conf version = AA.sendAdminApiRequest conf $ takeMaintenanceLogSnapshot version

-------------------------------------------------------------------------------
-- Utils

(<.>) :: Functor m => (b -> c) -> (a -> m b) -> a -> m c
(f <.> g) a = f <$> g a

type MapCounter a = Map.Map a Int

counter :: Ord a => [a] -> MapCounter a
counter = Map.fromList . map (\x -> (head x, length x)) . group . sort

printCounter :: (a -> String) -> MapCounter a -> String
printCounter f m =
  if Map.null m then "-"
  else intercalate "," $ (\(x,i) -> f x <> "(" <> show i <> ")") <$> Map.toList m

printTimeStamp :: Int64 -> CBytes
printTimeStamp ts = unsafePerformIO $ formatSystemTime simpleDateFormat (MkSystemTime (ts `div` 1000) 0)

printLoc :: Location -> String
printLoc m =
  let loc = ($ m) <$> [ Map.lookup LocationScope_ROOT
                      , Map.lookup LocationScope_RACK
                      , Map.lookup LocationScope_ROW
                      , Map.lookup LocationScope_CLUSTER
                      , Map.lookup LocationScope_DATA_CENTER
                      , Map.lookup LocationScope_REGION
                      , Map.lookup LocationScope_ROOT
                      ]
  in
    Text.unpack . Text.intercalate "." . catMaybes $ loc

printInfo :: Show a => a -> String
printInfo = Text.unpack . Text.intercalate "_" . tail . Text.splitOn "_" . pack . show
