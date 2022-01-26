{-# OPTIONS_GHC -Wno-orphans #-}

module HStream.Admin.Store.Command.CheckImpact
  ( checkImpact
  ) where

import           Control.Monad                    (unless)
import           Data.Default                     (def)
import           Data.Foldable                    (foldlM)
import qualified Data.Int                         as Int
import qualified Data.List                        as List
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust, fromMaybe, isJust,
                                                   mapMaybe)
import           Data.Set                         (Set)
import           Data.Text                        (Text)
import qualified Data.Text                        as Text
import           GHC.Stack                        (HasCallStack)
import qualified Options.Applicative.Help.Pretty  as P
import qualified Text.Layout.Table                as Table
import qualified Text.Layout.Table.Cell           as TableCell
import qualified Text.Layout.Table.StringBuilder  as Table
import qualified Z.Data.CBytes                    as CBytes

import qualified HStream.Admin.Store.API          as AA
import           HStream.Admin.Store.Types
import qualified HStream.Logger                   as Log
import           HStream.Store.Internal.LogDevice (getInternalLogName,
                                                   isInternalLog)

type NodeIdxMap = Map.Map AA.NodeIndex NodeProperties
type NodeNameToIdxMap = Map.Map Text AA.NodeIndex
-- { LocationScope : { LocationString : ShardSet } }
type ReadUnavailableMapping = Map.Map AA.LocationScope (Map.Map String AA.ShardSet)
type LocationMapping = Map.Map String [P.Doc]

-- TODO: this should equal (or at least smaller) to terminal size
maxTableWidth :: Int
maxTableWidth = 80

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
      nodesList = nodesIdsSelectByName `List.union` nodeIndexes
      shards' = getNodesShardNum nodesList nodeIdxMap
      nodesSelectById =
        [ AA.ShardID (AA.NodeID (Just n) Nothing Nothing) s
        | n <- nodesList , k <- shards' , s <- take (fromIntegral k) [0..]
        ]
   in shardSets `List.union` nodesSelectById

buildCheckImpactRequest :: CheckImpactOpts -> AA.ShardSet -> AA.CheckImpactRequest
buildCheckImpactRequest opts shardSet = AA.CheckImpactRequest
  { checkImpactRequest_shards = shardSet
  , checkImpactRequest_target_storage_state = Just (targetState opts)
    -- Do we want to validate if sequencers will be disabled on these nodes as well?
  , checkImpactRequest_disable_sequencers = Nothing
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
  let shards = combine checkImpactOpts nodeProperties
  let checkImpactRequest = buildCheckImpactRequest checkImpactOpts shards
  Log.debug $ "CheckImpact request: " <> Log.buildString (show checkImpactRequest)
  response <- AA.sendAdminApiRequest conf $ AA.checkImpact checkImpactRequest

  let delta = fromJust $ AA.checkImpactResponse_total_duration response
  let totalLogsChecked = fromJust $ AA.checkImpactResponse_total_logs_checked response
  let responseImpact = AA.checkImpactResponse_impact response
  case responseImpact of
    [] -> print $ P.green "ALL GOOD."
    impact -> print . P.red $ "UNSAFE. Impact: " <> P.text (impacts2string impact)
  putStrLn $ "Total logs checked (" <> show totalLogsChecked <> ") in " <> show delta <> "s"
  unless (ciShort checkImpactOpts) $
    print =<< checkImpactDoc response shards (targetState checkImpactOpts)

checkImpactDoc
  :: HasCallStack
  => AA.CheckImpactResponse
  -> AA.ShardSet
  -> AA.ShardStorageState
  -> IO P.Doc
checkImpactDoc response shards targetState = do
  let initEle = if fromMaybe False $ AA.checkImpactResponse_internal_logs_affected response
                then P.red (P.text "CRITICAL: Internal Logs are affected negatively!")
                else mempty
  foldlM (\acc a -> impactOnLogDoc a shards targetState >>= \b -> return $! acc P.<$$> b)
         initEle
         (fromMaybe [] (AA.checkImpactResponse_logs_affected response))

impactOnLogDoc
  :: HasCallStack
  => AA.ImpactOnEpoch
  -> AA.ShardSet
  -> AA.ShardStorageState
  -> IO P.Doc
impactOnLogDoc AA.ImpactOnEpoch{..} shards targetState = do
  let logIdDoc = if impactOnEpoch_log_id /= 0
                    then P.integer (fromIntegral impactOnEpoch_log_id)
                    else P.cyan . P.text $ "METADATA-LOGS"
      epoch = P.integer (fromIntegral impactOnEpoch_epoch)
      impactDoc = P.text $ impacts2string impactOnEpoch_impact

  let shardMetaPairs = zip impactOnEpoch_storage_set (fromJust impactOnEpoch_storage_set_metadata)
      replication = normalizeReplication impactOnEpoch_replication
  let (nWriteable, nWriteableLoss, locationMap, readUnavailable) =
        analyzeShards replication shards targetState shardMetaPairs (0, 0, Map.empty, Map.empty)

  internalVisual <-
    if isInternalLog (fromIntegral impactOnEpoch_log_id)
       then (\name -> "(" <> name <> ")")
          . P.cyan . P.text
          . CBytes.unpack
        <$> getInternalLogName (fromIntegral impactOnEpoch_log_id)
       else return ""
  let replicationStr =
        List.intercalate ", "
          . map (\(k, v) -> "{" <> prettyLocationScope k <> ": " <> show v <> "}")
          . Map.toList
          $ impactOnEpoch_replication

  return $ P.vcat
    [ "Log: " <> logIdDoc <> " " <> internalVisual
    , "Epoch: " <> epoch
    , "Storage-set Size: " <> P.integer (fromIntegral $ length impactOnEpoch_storage_set)
    , "Replication: " <> P.text replicationStr
      -- Write/Rebuilding Availability
    , analyzeWriteAvailability nWriteable nWriteableLoss replication
      -- Read/F-Majority Availability
    , analyzeReadAvailability readUnavailable replication
    , P.red ("Impact: " <> impactDoc)
    , ""
    , P.text (showLocationMap locationMap)
    , ""
    ]

analyzeShards
  :: AA.ReplicationProperty
  -> AA.ShardSet
  -> AA.ShardStorageState
  -> [(AA.ShardID, AA.ShardMetadata)]
  -> (Int, Int, LocationMapping, ReadUnavailableMapping)
  -> (Int, Int, LocationMapping, ReadUnavailableMapping)
analyzeShards _ _ _ [] (n, nloss, l, ru) = (n, nloss, l, ru)
analyzeShards replication shards targetState ((shard, meta):tl) (n, nloss, l, ru) =
  analyzeShards replication shards targetState tl f
  where
    f = uncurry (,,,) analyzeNWriteable analyzeLocationMap analyzeReadUnavailable
    biggestReplicationScope = getBiggestScope replication
    locPerScope = AA.shardMetadata_location_per_scope meta
    location = locationUpToScope shard locPerScope biggestReplicationScope
    isInTargetShards = matchShards shard shards
    metaStorageState = AA.shardMetadata_storage_state meta
    metaIsAlive = AA.shardMetadata_is_alive meta
    metaDataHealth = AA.shardMetadata_data_health meta
    analyzeNWriteable =
      -- A writable shard is a fully authoritative ALIVE + HEALTHY + READ_WRITE
      if   metaStorageState == AA.ShardStorageState_READ_WRITE
        && metaIsAlive
        && metaDataHealth == AA.ShardDataHealth_HEALTHY
           then (n + 1, if isInTargetShards && (targetState /= AA.ShardStorageState_READ_WRITE)
                           then nloss + 1 else nloss)
           else (n, nloss)
    analyzeReadUnavailable =
      -- A readable shard is a fully authoritative ALIVE + HEALTHY + READ_ONLY/WRITE
      if  elem metaStorageState [AA.ShardStorageState_READ_WRITE, AA.ShardStorageState_READ_ONLY]
       -- The part of the storage set that should be read available is all
       -- READ_{ONLY, WRITE} shards that are non-EMPTY
       && metaDataHealth /= AA.ShardDataHealth_EMPTY
       -- This is a shard is/may/will be read unavailable.
       && ( not metaIsAlive
         || metaDataHealth /= AA.ShardDataHealth_HEALTHY
         || (isInTargetShards && targetState == AA.ShardStorageState_DISABLED)
          )
          then -- For each domain in the replication property, add the location
               -- string as a read unavailable target
               let locTag scope = locationUpToScope shard locPerScope scope
                   newElem scope = Map.singleton scope (Map.singleton (locTag scope) [shard])
                   foldFun = Map.unionWith (Map.unionWith List.union) . newElem
                in Map.foldrWithKey' (\scope _ acc -> foldFun scope acc) ru replication
          else ru
    analyzeLocationMap =
      let colorit = case metaDataHealth of
                      AA.ShardDataHealth_EMPTY -> P.white
                      x -> if x /= AA.ShardDataHealth_HEALTHY
                              then P.red
                              else if metaIsAlive then P.green else P.red
          boldit = if isInTargetShards then P.bold else id
          visual = -- e.g. N0:S0
                   (P.fill 8 . colorit . boldit . P.text) (prettyShardID shard)
                <> " "
                   -- e.g. READ_WRITE
                <> (colorit . P.text) (prettyShardStorageState metaStorageState)
                <> " "
                   -- e.g. → DISABLED
                <> if isInTargetShards
                      then "→ " <> (P.cyan . P.text) (prettyShardStorageState targetState)
                      else ""
                <> " "
                <> if metaIsAlive then "" else colorit "(DEAD)"
                <> " "
                <> if metaDataHealth /= AA.ShardDataHealth_HEALTHY
                      then "(" <> (colorit . P.text . prettySahrdDataHealth) metaDataHealth <> ")"
                      else ""
       in Map.insertWith (++) location [visual] l

docVisibleLength :: P.Doc -> Int
docVisibleLength d = length $ P.displayS (P.renderCompact d) ""

instance TableCell.Cell P.Doc where
  visibleLength = docVisibleLength
  measureAlignment = error "TODO: measureAlignment for Doc"
  buildCell = Table.stringB . show
  dropBoth = error "TODO: dropBoth for Doc"

-- (Title string, List of row)
type Column a = (String, Table.Col a)
type Columns a = [Column a]

consTable :: Columns P.Doc -> [String]
consTable cell =
  let (titles, cols) = unzip cell
      rowGroup = Table.colsAllG Table.top cols
   in Table.tableLines (def <$ cols) Table.asciiS (Table.titlesH titles) [rowGroup]

isRowFill :: Columns P.Doc -> Bool
isRowFill = (>= maxTableWidth) . length . head . consTable

splitTable' :: [Columns P.Doc] -> [Column P.Doc] -> [Columns P.Doc]
splitTable' [] [] = []
splitTable' acc [] = reverse $ map reverse acc
splitTable' [] (x:rs)
  | isRowFill [x] = splitTable' [[x]] rs
  | otherwise = splitTable' [[x]] rs
splitTable' acc (x:xs)
  | isRowFill [x] = splitTable' ([x] : acc) xs
  | otherwise = if null acc
                   then splitTable' [[x]] xs
                   else if isRowFill (x : head acc) then splitTable' ([x] : acc) xs
                                                    else splitTable' ((x : head acc) : tail acc) xs

splitTable :: [Column P.Doc] -> [Columns P.Doc]
splitTable = splitTable' []

showLocationMap :: LocationMapping -> String
showLocationMap locationMap =
  let tableCell = Map.toAscList locationMap
   in Table.concatLines (map (Table.concatLines . consTable) (splitTable tableCell))

analyzeWriteAvailability :: Int -> Int -> AA.ReplicationProperty -> P.Doc
analyzeWriteAvailability nWriteable nWriteableLoss replication =
  let nWriteable' = nWriteable - nWriteableLoss
      formattedReplication = List.intercalate " in "
                           . map (\(scope, v) -> show v <> " " <> prettyLocationScope scope <> "s")
                           $ Map.toAscList replication
      nWriteableVisual = if nWriteable' /= nWriteable
                            then " → " <> show nWriteable'
                            else ""
   in P.text
      ( "Write/Rebuilding availability: "
     <> show nWriteable <> nWriteableVisual <> " "
     <> "(we need at least " <> formattedReplication
     <> " that are healthy, writable, and alive.)"
      )

analyzeReadAvailability :: ReadUnavailableMapping -> AA.ReplicationProperty -> P.Doc
analyzeReadAvailability readUnavailable replication =
  let sortedReadUnavailable = Map.toAscList readUnavailable
      biggestFailingDomain = maybeLast sortedReadUnavailable
      readUnavailableFormatter (scope, v) =
        let maxLossAtScope = (fromIntegral . fromJust $ Map.lookup scope replication) - 1
            colorit = if length v > maxLossAtScope then P.red else P.green
            actual = if isJust biggestFailingDomain && scope == fst (fromJust biggestFailingDomain)
                        then colorit . P.text $ " (actual " <> (show . length) v <> ") "
                        else " "
         in P.int maxLossAtScope <> actual <> (P.text . prettyLocationScope) scope <> "s"
      formattedReadUnavailable = map readUnavailableFormatter sortedReadUnavailable
      expectation = P.hcat $ List.intersperse " across more than " formattedReadUnavailable
   in ( "Read availability: We can't afford losing more than "
     <> expectation <> ". "
     <> "Nodes must be healthy, readable, and alive."
      )

-- If we got replication {cluster: 3}, it actually means {cluster: 3, node: 3}
--
-- type ReplicationProperty = Map.Map LocationScope Int.Int32
normalizeReplication :: AA.ReplicationProperty -> AA.ReplicationProperty
normalizeReplication replication =
  let replication_factor = maximum $ Map.elems replication
   in Map.insert AA.LocationScope_NODE replication_factor replication

-- | Returns the biggest scope at given replication property
getBiggestScope :: AA.ReplicationProperty -> AA.LocationScope
getBiggestScope = Map.foldrWithKey (\k _ r -> max k r) AA.LocationScope_NODE

-- | Generates a string of the location string up to a given scope. The input
-- scope is inclusive.
locationUpToScope :: AA.ShardID -> AA.Location -> AA.LocationScope -> String
locationUpToScope shard locPerScope scope
  | Map.null locPerScope = "UNKNOWN"
  | otherwise =
      -- Sort scopes from bigger to smaller (ROOT > REGION > CLUSTER > ...)
      let sortedLocPerScope = Map.toDescList locPerScope
          nodeID = maybe "UNKNOWN_NODE" show (AA.nodeID_node_index . AA.shardID_node $ shard)
          extraLocs = [nodeID | scope == AA.LocationScope_NODE]
          locs = map (Text.unpack . snd) $ takeWhile (\(locScope, _) -> locScope >= scope) sortedLocPerScope
       in List.intercalate "." $ locs ++ extraLocs

isSameScope :: AA.ShardID -> AA.ShardID -> Bool
isSameScope scope1 scope2
  | n1 == n2 = s1 == (-1) || s2 == (-1) || s1 == s2
  | otherwise = False
  where
    n1 = getNodeID scope1
    n2 = getNodeID scope2
    s1 = AA.shardID_shard_index scope1
    s2 = AA.shardID_shard_index scope2
    getNodeID = AA.nodeID_node_index . AA.shardID_node

matchShards :: AA.ShardID -> AA.ShardSet -> Bool
matchShards shard = any (isSameScope shard)

maybeLast :: [a] -> Maybe a
maybeLast [x]    = Just x
maybeLast (_:xs) = maybeLast xs
maybeLast []     = Nothing
