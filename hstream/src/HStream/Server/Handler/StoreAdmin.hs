{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.StoreAdmin
  ( listStoreNodesHandler
  , getStoreNodeHandler
  ) where

import           Control.Monad                    (forM)
import           Data.Aeson                       (Value (..))
import           Data.Int                         (Int32)
import           Data.List                        (find)
import           Data.Maybe                       (fromMaybe)
import           Data.Scientific                  (floatingOrInteger)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Lens.Micro
import           Lens.Micro.Aeson
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Admin.Store.API          as AA
import qualified HStream.Admin.Store.Command      as AC
import           HStream.Admin.Store.Types        (SimpleNodesFilter (..),
                                                   StatusFormat (..),
                                                   StatusOpts (..),
                                                   fromSimpleNodesFilter)
import qualified HStream.Logger                   as Log
import           HStream.Server.Handler.Common    (responseWithErrorMsgIfNothing)
import           HStream.Server.HStreamApi
import           HStream.Server.Types

toInt :: Value -> Int32
toInt (Number sci) = case floatingOrInteger sci of
    Left _  -> 0
    Right i -> (i :: Int32)
toInt _ = 0

toArrInt :: Value -> V.Vector Int32
toArrInt (Array v) = fmap toInt v
toArrInt _         = V.empty

toString :: Value -> T.Text
toString (String s) = s
toString _          = ""

statusOpts :: StatusOpts
statusOpts = StatusOpts TabularFormat True (StatusNodeIdx []) "ID"

getNodes :: AA.HeaderConfig AA.AdminAPI -> StatusOpts -> IO (Maybe (V.Vector Node))
getNodes headerConfig StatusOpts{..} = do
  states <- AA.sendAdminApiRequest headerConfig $ do
    case fromSimpleNodesFilter statusFilter of
      [] -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest Nothing (Just statusForce))
      xs -> do
        rs <- forM xs $ \x -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest (Just x) (Just statusForce))
        return $ concat rs

  let getID = show . AA.nodeConfig_node_index . AA.nodeState_config
  let getName = T.unpack . AA.nodeConfig_name . AA.nodeState_config
  let getState = T.unpack . last . T.splitOn "_" . T.pack . show . AA.nodeState_daemon_state
  let getHealthState = T.unpack . last . T.splitOn "_" . T.pack . show . AA.nodeState_daemon_health_status
  let collectState s = map ($ s) [getID, getName, getState, getHealthState]
  let allStatus = map collectState states

  res <- AC.showConfig headerConfig (StatusNodeIdx [])
  case res ^? key "nodes" of
    Just (Array arr) -> do
      return $ sequenceA (readNode allStatus <$> arr)
    _                -> return Nothing
  where
    readNode statusList node = Node
      <$> (toInt <$> node ^? key "node_index")
      <*> (toArrInt <$> node ^? key "roles")
      <*> (toString <$> node ^? key "data_address" . key "address")
      <*> getStatus statusList node

    getStatus statusList node = do
      nodeId <- toInt <$> node ^? key "node_index"
      info <- find (\(id':_) -> show nodeId == id') statusList
      case info of
        _:_:_:[status] -> Just $ T.pack status
        _              -> Nothing

listStoreNodesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListNodesRequest ListNodesResponse
  -> IO (ServerResponse 'Normal ListNodesResponse)
listStoreNodesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  Log.debug "Receive List Store Nodes Request"
  nodes <- getNodes headerConfig statusOpts
  responseWithErrorMsgIfNothing (ListNodesResponse <$> nodes) StatusInternal "Get Nodes Info failed"

getStoreNodeHandler
  :: ServerContext
  -> ServerRequest 'Normal GetNodeRequest Node
  -> IO (ServerResponse 'Normal Node)
getStoreNodeHandler ServerContext{..} (ServerNormalRequest _metadata GetNodeRequest{..}) = do
  Log.debug $ "Receive Get Store Node Request. "
    <> "Node ID: " <> Log.buildInt getNodeRequestId
  nodes <- getNodes headerConfig statusOpts
  let node = V.find (\(Node id' _ _ _) -> id' == getNodeRequestId) <$> nodes
  responseWithErrorMsgIfNothing (fromMaybe Nothing node) StatusNotFound "Node not exists"
