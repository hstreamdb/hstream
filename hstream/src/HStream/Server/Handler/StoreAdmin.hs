{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.StoreAdmin where

import           Control.Lens
import           Control.Monad                    (forM)
import           Data.Aeson                       (Value (..))
import           Data.Aeson.Lens
import           Data.Int                         (Int32)
import           Data.List                        (find)
import           Data.Maybe                       (fromMaybe)
import           Data.Scientific                  (floatingOrInteger)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated

import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   responseWithErrorMsgIfNothing)
import qualified HStream.Store.Admin.API          as AA
import qualified HStream.Store.Admin.Command      as AC
import           HStream.Store.Admin.Types        (SimpleNodesFilter (..),
                                                   StatusFormat (..),
                                                   StatusOpts (..),
                                                   fromSimpleNodesFilter)

toInt :: Value -> Int32
toInt (Number sci) = case floatingOrInteger sci of
    Left _  -> 0
    Right i -> (i :: Int32)
toInt _ = 0

toArrInt :: Value -> V.Vector Int32
toArrInt (Array v) = fmap toInt v
toArrInt _         = V.empty

toString :: Value -> TL.Text
toString (String s) = TL.pack $ T.unpack s
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
  let nodes = res ^? key "nodes"
  case nodes of
    Just (Array arr) -> do
      let nodes' = fmap (\node -> do
                            let Just id' = toInt <$> node ^? key "node_index"
                                Just roles = toArrInt <$> node ^? key "roles"
                                Just address = toString <$> node ^? key "data_address" . key "address"
                                Just status = (\(_:name:_:[status']) -> status') <$> find (\(id'':_) -> (show id') == id'') allStatus
                            Node id' roles address (TL.pack status)
                        ) arr
      return $ Just nodes'
    _ -> return Nothing

listStoreNodesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListNodesRequest ListNodesResponse
  -> IO (ServerResponse 'Normal ListNodesResponse)
listStoreNodesHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  nodes <- getNodes headerConfig statusOpts
  responseWithErrorMsgIfNothing (ListNodesResponse <$> nodes) StatusInternal "Get Nodes Info failed"

getStoreNodeHandler
  :: ServerContext
  -> ServerRequest 'Normal GetNodeRequest Node
  -> IO (ServerResponse 'Normal Node)
getStoreNodeHandler ServerContext{..} (ServerNormalRequest _metadata GetNodeRequest{..}) = do
  nodes <- getNodes headerConfig statusOpts
  let node = (V.find (\(Node id' _ _ _) -> id' == getNodeRequestId)) <$> nodes
  responseWithErrorMsgIfNothing (fromMaybe Nothing node) StatusInternal "Node not exists"
