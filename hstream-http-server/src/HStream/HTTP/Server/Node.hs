{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Node (
  NodesAPI, nodeServer, getNodes
) where

import           Control.Lens
import           Control.Monad               (forM, join)
import           Control.Monad.IO.Class      (liftIO)
import           Data.Aeson                  (FromJSON, ToJSON, Value (..),
                                              decode, object, toJSON, (.=))
import           Data.Aeson.Lens
import           Data.ByteString             (ByteString)
import qualified Data.ByteString.Lazy.Char8  as DBCL
import qualified Data.HashMap.Strict         as HM
import           Data.List                   (find)
import qualified Data.Map.Strict             as Map
import           Data.Maybe                  (fromMaybe)
import           Data.Scientific             (floatingOrInteger)
import           Data.Swagger                (ToSchema)
import           Data.Text                   (Text)
import qualified Data.Text                   as T
import           Data.Text.Encoding          (encodeUtf8)
import           Data.Vector                 (toList)
import           GHC.Generics                (Generic)
import           Servant                     (Capture, Delete, Get, JSON,
                                              PlainText, Post, ReqBody,
                                              type (:>), (:<|>) (..))
import           Servant.Server              (Handler, Server)
import qualified Z.Data.CBytes               as ZDC
import qualified Z.IO.Logger                 as Log


import           HStream.Connector.HStore    as HCH
import           HStream.Store               as HS
import qualified HStream.Store.Admin.API     as AA
import qualified HStream.Store.Admin.Command as AC
import           HStream.Store.Admin.Types   (SimpleNodesFilter (..),
                                              StatusFormat (..),
                                              StatusOpts (..),
                                              fromSimpleNodesFilter)

-- BO is short for Business Object
data NodeBO = NodeBO
  { id      :: Maybe Int
  , roles   :: Maybe [Int]
  , address :: Maybe String
  , status  :: Maybe String
  } deriving (Eq, Show, Generic)

instance ToJSON NodeBO
instance ToSchema NodeBO

extractProperty :: [Text] -> Value -> Maybe Value
extractProperty []     v          = Just v
extractProperty (k:ks) (Object o) = HM.lookup k o >>= extractProperty ks
extractProperty _      _          = Nothing

toInt :: Value -> Int
toInt (Number sci) = case floatingOrInteger sci of
    Left r  -> 0
    Right i -> (i :: Int)

toArrInt :: Value -> [Int]
toArrInt (Array v) = toList $ fmap toInt v

toString :: Value -> String
toString (String s) = T.unpack s

type NodesAPI =
  "nodes" :> Get '[JSON] (Maybe [NodeBO])
  :<|> "nodes" :> Capture "id" Int :> Get '[JSON] (Maybe NodeBO)

getNodes :: AA.HeaderConfig AA.AdminAPI -> StatusOpts -> IO (Maybe [NodeBO])
getNodes headerConfig StatusOpts{..} = do
  states <- AA.sendAdminApiRequest headerConfig $ do
    case fromSimpleNodesFilter statusFilter of
      [] -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest Nothing (Just statusForce))
      xs -> do
        rs <- forM xs $ \x -> AA.nodesStateResponse_states <$> AA.getNodesState (AA.NodesStateRequest (Just x) (Just statusForce))
        return $ concat rs

  let titles = ["ID", "NAME", "STATE", "HEALTH STATUS"]
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
                            let id = toInt <$> node ^? key "node_index"
                                roles = toArrInt <$> node ^? key "roles"
                                address = toString <$> node ^? key "data_address" . key "address"
                            let status = (\(_:name:_:[status']) -> status') <$> find (\(id':_) -> (show <$> id) == Just id') allStatus
                            NodeBO id roles address status
                        ) arr
      return $ Just $ toList nodes'
    _ -> return Nothing

getNodeHandler :: AA.HeaderConfig AA.AdminAPI -> StatusOpts -> Int -> Handler (Maybe NodeBO)
getNodeHandler headerConfig statusOpts target = do
  nodes <- liftIO (getNodes headerConfig statusOpts)
  let node = (find (\(NodeBO id _ _ _) -> id == Just target)) <$> nodes
  return $ fromMaybe Nothing node

fetchNodeHandler :: AA.HeaderConfig AA.AdminAPI -> StatusOpts -> Handler (Maybe [NodeBO])
fetchNodeHandler headerConfig statusOpts = liftIO (getNodes headerConfig statusOpts)

nodeServer :: ByteString -> Int -> Server NodesAPI
nodeServer ldAdminHost ldAdminPort = do
  let headerConfig = AA.HeaderConfig ldAdminHost ldAdminPort AA.binaryProtocolId 5000 5000 5000
  let statusOpts = StatusOpts TabularFormat True (StatusNodeIdx []) "ID"
  fetchNodeHandler headerConfig statusOpts :<|> (getNodeHandler headerConfig statusOpts)
