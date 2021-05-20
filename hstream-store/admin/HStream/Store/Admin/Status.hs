module HStream.Store.Admin.Status
  ( runStatus
  ) where

import           Control.Monad
import           Data.Int
import           Data.List                  (elemIndex, sortBy)
import           Data.Text                  (Text)
import qualified Data.Text                  as Text
import qualified Text.Layout.Table          as Table

import qualified HStream.Store.Admin.API    as AA
import           HStream.Store.Admin.Format (simpleShowTable)
import           HStream.Store.Admin.Types

-- | Gets the state object for all nodes that matches the supplied NodesFilter.
--
-- If NodesFilter is empty we will return all nodes. If the filter does not
-- match any nodes, an empty list of nodes is returned in the
-- NodesStateResponse object. `force` will force this method to return all the
-- available state even if the node is not fully ready. In this case we will
-- not throw NodeNotReady exception but we will return partial data.
runStatus :: AA.SocketConfig AA.AdminAPI -> StatusOpts -> IO String
runStatus socketConfig StatusOpts{..} = do
  states <- AA.withSocketChannel @AA.AdminAPI socketConfig $ do
    case fromSimpleNodesFilter statusFilter of
      [] -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest Nothing (Just statusForce))
      xs -> do
        rs <- forM xs $ \x -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest (Just x) (Just statusForce))
        return $ concat rs

  let titles = ["ID", "NAME", "STATE", "HEALTH STATUS"]
  let getID = show . AA.nodeConfig_node_index . AA.nodeState_config
  let getName = Text.unpack . AA.nodeConfig_name . AA.nodeState_config
  let getState = Text.unpack . last . Text.splitOn "_" . Text.pack . show . AA.nodeState_daemon_state
  let getHealthState = Text.unpack . last . Text.splitOn "_" . Text.pack . show . AA.nodeState_daemon_health_status
  let collectState s = map ($ s) [getID, getName, getState, getHealthState]
  let allStates = map collectState states

  let m_sortIdx = elemIndex (Text.unpack . Text.toUpper $ statusSortField) titles
  case m_sortIdx of
    Just sortIdx -> do
      let stats = sortBy (\xs ys -> compare (xs!!sortIdx) (ys!!sortIdx)) allStates
      case statusFormat of
        TabularFormat -> return $ simpleShowTable (map (, 10, Table.left) titles) stats
        JSONFormat    -> errorWithoutStackTrace "NotImplemented"
    Nothing -> errorWithoutStackTrace $ "No such sort key: " <> Text.unpack statusSortField
