{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.StoreAdmin
  ( -- * For grpc-haskell
    listStoreNodesHandler
  , getStoreNodeHandler
    -- * For hs-grpc-server
  , handleListStoreNodes
  , handleGetStoreNode
  ) where

import           Control.Exception                (throwIO)
import           Control.Monad                    (forM)
import           Data.Aeson                       (Value (..))
import           Data.Int                         (Int32)
import           Data.List                        (find)
import           Data.Scientific                  (floatingOrInteger)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import           Lens.Micro
import           Lens.Micro.Aeson
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Admin.Store.API          as AA
import qualified HStream.Admin.Store.Command      as AC
import           HStream.Admin.Store.Types        (SimpleNodesFilter (..),
                                                   StatusFormat (..),
                                                   StatusOpts (..),
                                                   fromSimpleNodesFilter)
import qualified HStream.Exception                as HE
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.Utils                    (returnResp)

-------------------------------------------------------------------------------

listStoreNodesHandler
  :: ServerContext
  -> ServerRequest 'Normal ListNodesRequest ListNodesResponse
  -> IO (ServerResponse 'Normal ListNodesResponse)
listStoreNodesHandler ServerContext{..} (ServerNormalRequest _metadata _) =
  defaultExceptionHandle $ do
    Log.debug "Receive List Store Nodes Request"
    nodes <- listStoreNodes headerConfig statusOpts
    returnResp $ ListNodesResponse nodes

handleListStoreNodes
  :: ServerContext -> G.UnaryHandler ListNodesRequest ListNodesResponse
handleListStoreNodes ServerContext{..} _ _ = catchDefaultEx $ do
  Log.debug "Receive List Store Nodes Request"
  ListNodesResponse <$> listStoreNodes headerConfig statusOpts

getStoreNodeHandler
  :: ServerContext
  -> ServerRequest 'Normal GetNodeRequest Node
  -> IO (ServerResponse 'Normal Node)
getStoreNodeHandler ServerContext{..} (ServerNormalRequest _metadata GetNodeRequest{..}) =
  defaultExceptionHandle $ do
    Log.debug $ "Receive Get Store Node Request. "
             <> "Node ID: " <> Log.buildInt getNodeRequestId
    node <- getStoreNode headerConfig getNodeRequestId
    returnResp node

handleGetStoreNode :: ServerContext -> G.UnaryHandler GetNodeRequest Node
handleGetStoreNode ServerContext{..} _ GetNodeRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Get Store Node Request. "
           <> "Node ID: " <> Log.buildInt getNodeRequestId
  getStoreNode headerConfig getNodeRequestId

-------------------------------------------------------------------------------

getStoreNode :: AA.HeaderConfig AA.AdminAPI -> Int32 -> IO Node
getStoreNode headerConfig nodeId = do
  nodes <- listStoreNodes headerConfig statusOpts
  let m_node = V.find (\(Node id' _ _ _) -> id' == nodeId) nodes
  case m_node of
    Just node -> pure node
    Nothing   -> throwIO $ HE.NodesNotFound "Node not exists"

listStoreNodes :: AA.HeaderConfig AA.AdminAPI -> StatusOpts -> IO (V.Vector Node)
listStoreNodes headerConfig StatusOpts{..} = do
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
    Just (Array arr) ->
      case sequenceA (readNode allStatus <$> arr) of
        Just nodes -> pure nodes
        Nothing    -> throwIO $ HE.UnexpectedError "Unexpected store nodes info"
    _ -> throwIO $ HE.UnexpectedError "Get store nodes failed"
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
statusOpts = StatusOpts TabularFormat True False (StatusNodeIdx []) "ID"
