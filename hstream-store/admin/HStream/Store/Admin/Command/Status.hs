module HStream.Store.Admin.Command.Status
  ( runStatus
  ) where

import           Control.Monad
import           Data.Int                  (Int64)
import           Data.List                 (elemIndex, group, intercalate, sort,
                                            sortBy, sortOn)
import           Data.Maybe                (fromJust, fromMaybe)
import qualified Data.Text                 as Text
import qualified Data.Text.Encoding        as Text
import           Data.Time.Clock.POSIX     (POSIXTime, getPOSIXTime)

import qualified HStream.Store.Admin.API   as AA
import           HStream.Store.Admin.Types
import           HStream.Utils             (approxNaturalTime,
                                            simpleShowTableIO')

data NodeState' = NodeState'
  { stateState      :: AA.NodeState
  , stateVersion    :: Text.Text
  , stateAliveSince :: Int64
  }

showID :: NodeState' -> String
showID = show . AA.nodeConfig_node_index . AA.nodeState_config . stateState

showName :: NodeState' -> String
showName = Text.unpack . AA.nodeConfig_name . AA.nodeState_config . stateState

showLocation :: NodeState' -> String
showLocation = Text.unpack
             . fromMaybe "?"
             . AA.nodeConfig_location
             . AA.nodeState_config
             . stateState

showDaemonState :: NodeState' -> String
showDaemonState = takeTail' "_" . show . AA.nodeState_daemon_state . stateState

showDataHealth :: NodeState' -> String
showDataHealth = maybe " " f . AA.nodeState_shard_states . stateState
  where
    f = interpretByFrequency . map (takeTail' "_" . show . AA.shardState_data_health)

showStorageState :: NodeState' -> String
showStorageState = maybe " " f . AA.nodeState_shard_states . stateState
  where
    f = interpretByFrequency . map (takeTail' "_" . show . AA.shardState_storage_state)

showShardOp :: NodeState' -> String
showShardOp = maybe " " f . AA.nodeState_shard_states . stateState
  where
    f = interpretByFrequency . map (takeTail' "_" . show . AA.shardState_current_operational_state)

showHealthState :: NodeState' -> String
showHealthState = takeTail' "_" . show . AA.nodeState_daemon_health_status . stateState

showVersion :: NodeState' -> String
showVersion = Text.unpack . stateVersion

showUptime :: POSIXTime -> NodeState' -> String
showUptime time state =
  approxNaturalTime (time - fromIntegral (stateAliveSince state)) ++ " ago"

showSeqState :: NodeState' -> String
showSeqState = takeTail' "_" . maybe " " (show . AA.sequencerState_state) . AA.nodeState_sequencer_state . stateState

-- | Gets the state object for all nodes that matches the supplied NodesFilter.
--
-- If NodesFilter is empty we will return all nodes. If the filter does not
-- match any nodes, an empty list of nodes is returned in the
-- NodesStateResponse object. `force` will force this method to return all the
-- available state even if the node is not fully ready. In this case we will
-- not throw NodeNotReady exception but we will return partial data.
runStatus :: AA.HeaderConfig AA.AdminAPI -> StatusOpts -> IO String
runStatus conf StatusOpts{..} = do
  states <- AA.sendAdminApiRequest conf $ do
    case fromSimpleNodesFilter statusFilter of
      [] -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest Nothing (Just statusForce))
      xs -> do
        rs <- forM xs $ \x -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest (Just x) (Just statusForce))
        return $ concat rs

  let getNodeHeaderConfig sa =
        AA.HeaderConfig
          { AA.headerHost = Text.encodeUtf8 . fromJust $ AA.socketAddress_address sa
          , AA.headerPort = fromIntegral . fromJust $ AA.socketAddress_port sa
          , AA.headerProtocolId  = AA.binaryProtocolId
          , AA.headerConnTimeout = 5000
          , AA.headerSendTimeout = 5000
          , AA.headerRecvTimeout = 5000
          }
  additionStates <- forM states $ \state -> do
    let hc = getNodeHeaderConfig . AA.getNodeAdminAddr $ AA.nodeState_config state
    AA.sendAdminApiRequest hc $ do
      version <- AA.getVersion
      aliveSince <- AA.aliveSince
      return (version, aliveSince)
  let allStates = zipWith (\state (version, alive) -> NodeState' state version alive) states additionStates
  currentTime <- getPOSIXTime

  let cons = [ ("ID", showID)
             , ("NAME", showName)
             , ("PACKAGE", showVersion)
             , ("STATE", showDaemonState)
             , ("UPTIME", showUptime currentTime)
             , ("LOCATION", showLocation)
             , ("SEQ.", showSeqState)
             , ("DATA HEALTH", showDataHealth)
             , ("STORAGE STATE", showStorageState)
             , ("SHARD OP.", showShardOp)
             , ("HEALTH STATUS", showHealthState)
             ]
  let titles = map fst cons
      collectedState = map (\s -> map (($ s) . snd) cons) allStates

  let m_sortIdx = elemIndex (Text.unpack . Text.toUpper $ statusSortField) titles
  case m_sortIdx of
    Just sortIdx -> do
      let stats = sortBy (\xs ys -> compare (xs!!sortIdx) (ys!!sortIdx)) collectedState
      case statusFormat of
        TabularFormat -> simpleShowTableIO' titles stats
        JSONFormat    -> errorWithoutStackTrace "NotImplemented"
    Nothing -> errorWithoutStackTrace $ "No such sort key: " <> Text.unpack statusSortField

-------------------------------------------------------------------------------

interpretByFrequency :: [String] -> String
interpretByFrequency = intercalate ","
                     . map (\(x, l) -> x ++ "(" ++ show l ++ ")")
                     . sortOn snd
                     . map (\xs -> (head xs, length xs))
                     . group . sort
