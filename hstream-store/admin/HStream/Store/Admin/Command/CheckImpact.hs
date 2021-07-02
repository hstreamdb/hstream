module HStream.Store.Admin.Command.CheckImpact
  ( checkImpact
  ) where

import qualified Data.Int                  as Int
import           Data.List                 (intercalate, union)
import qualified Data.Map.Strict           as Map
import           Data.Maybe                (fromJust, mapMaybe)
import           Data.Set                  (Set)
import           Data.Text                 (Text)

import qualified HStream.Store.Admin.API   as AA
import           HStream.Store.Admin.Types

type NodeIdxMap = Map.Map AA.NodeIndex NodeProperties
type NodeNameToIdxMap = Map.Map Text AA.NodeIndex

data NodeProperties = NodeProperties
  { nodeIndex :: AA.NodeIndex
  , nodeRoles :: Set AA.Role
  , nodeName  :: Text
  , numShards :: Int.Int32
  } deriving (Show)

getNodeProperties :: AA.HeaderConfig AA.AdminAPI -> IO [NodeProperties]
getNodeProperties conf = do
  res <- AA.sendAdminApiRequest conf (AA.getNodesConfig (AA.NodesFilter Nothing Nothing Nothing))
  return $ map build (AA.nodesConfigResponse_nodes res)
    where
      build :: AA.NodeConfig -> NodeProperties
      build c = NodeProperties
        { nodeIndex = AA.nodeConfig_node_index c
        , nodeRoles = AA.nodeConfig_roles c
        , nodeName = AA.nodeConfig_name c
        , numShards = fromJust $ AA.storageConfig_num_shards <$> AA.nodeConfig_storage c
        }

getNodeIdxMap :: [NodeProperties] -> NodeIdxMap
getNodeIdxMap = Map.fromList . map (\node -> (nodeIndex node, node))

getNodeNameToIdxMap :: [NodeProperties] -> NodeNameToIdxMap
getNodeNameToIdxMap = Map.fromList . map (\node -> (nodeName node, nodeIndex node))

getNodesShardNum :: [AA.NodeIndex] -> NodeIdxMap -> [Int.Int32]
getNodesShardNum idxs dict = map (numShards . fromJust . flip Map.lookup dict) idxs

-- TODO: code refactoring
combine :: CheckImpactOpts -> [NodeProperties] -> AA.ShardSet
combine CheckImpactOpts{..} prop =
  let shardSets = shards
      nameToIdxMap = getNodeNameToIdxMap prop
      nodeIdxMap = getNodeIdxMap prop
      nodesIdsSelectByName = mapMaybe (`Map.lookup` nameToIdxMap) nodeNames
      nodesList = nodesIdsSelectByName `union` nodeIndexes
      shards' = getNodesShardNum nodesList nodeIdxMap
      nodesSelectById =
        [ AA.ShardID (AA.NodeID (Just n) Nothing Nothing) s
        | n <- nodesList , k <- shards' , s <- take (fromIntegral k) [0..]
        ]
   in shardSets `union` nodesSelectById

buildCheckImpactRequest :: CheckImpactOpts -> AA.ShardSet -> AA.CheckImpactRequest
buildCheckImpactRequest opts shardSet = AA.CheckImpactRequest
  { checkImpactRequest_shards = shardSet
  , checkImpactRequest_target_storage_state = Just (targetState opts)
  , checkImpactRequest_disable_sequencers = Just (disableSequencers opts)
  , checkImpactRequest_safety_margin = replicationProperty
  , checkImpactRequest_log_ids_to_check = logIdsToCheck
  , checkImpactRequest_abort_on_negative_impact = Just True
  , checkImpactRequest_return_sample_size = Just 20
  , checkImpactRequest_check_metadata_logs = Just (not $ skipMetaDataLogs opts)
  , checkImpactRequest_check_internal_logs = Just (not $ skipInternalLogs opts)
  , checkImpactRequest_check_capacity = Just (not skipCapacity)
  , checkImpactRequest_max_unavailable_storage_capacity_pct = maxUnavailableStorageCapacity
  , checkImpactRequest_max_unavailable_sequencing_capacity_pct = maxUnavailableSequencingCapacity
  }
  where
    replicationProperty =
      let margin = safetyMargin opts
       in if null margin
             then Nothing
             else Just (Map.fromList $ map unReplicationPropertyPair margin)
    logIdsToCheck =
      let logIds = logs opts
       in if null logIds
             then Nothing
             else Just logIds
    skipCapacity = skipCapacityChecks opts
    maxUnavailableStorageCapacity = if skipCapacity then 100 else maxUnavailableStorageCapacityPct opts
    maxUnavailableSequencingCapacity = if skipCapacity then 100 else maxUnavailableSequencingCapacityPct opts

checkImpact :: AA.HeaderConfig AA.AdminAPI -> CheckImpactOpts -> IO ()
checkImpact conf checkImpactOpts = do
  nodeProperties <- getNodeProperties conf
  let shardSets = combine checkImpactOpts nodeProperties
  let checkImpactRequest = buildCheckImpactRequest checkImpactOpts shardSets
  response <- AA.sendAdminApiRequest conf $ AA.checkImpact checkImpactRequest

  let delta = fromJust $ AA.checkImpactResponse_total_duration response
  let totalLogsChecked = fromJust $ AA.checkImpactResponse_total_logs_checked response
  let responseImpact = AA.checkImpactResponse_impact response
  case responseImpact of
    [] -> putStrLn $ "ALL GOOD.\n  Total logs checked (" <> show totalLogsChecked <> ") in " <> show delta <> "s"
    impact -> putStrLn $ "UNSAFE. Impact: " <> intercalate ", " (map show impact)
                      <> "\n Total logs checked (" <> show totalLogsChecked <> ") in " <> show delta <> "s"
