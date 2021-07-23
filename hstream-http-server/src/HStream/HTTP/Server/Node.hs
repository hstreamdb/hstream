{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Node (
  NodesAPI, nodeServer, listStoreNodesHandler
) where

import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.Int                         (Int32)
import qualified Data.Map.Strict                  as Map
import           Data.Swagger                     (ToSchema)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Client     (Client)
import           Servant                          (Capture, Get, JSON,
                                                   type (:>), (:<|>) (..))
import           Servant.Server                   (Handler, Server)

import           HStream.Server.HStreamApi

-- BO is short for Business Object
data NodeBO = NodeBO
  { id      :: Int32
  , roles   :: [Int32]
  , address :: T.Text
  , status  :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON NodeBO
instance ToSchema NodeBO

type NodesAPI =
  "nodes" :> Get '[JSON] [NodeBO]
  :<|> "nodes" :> Capture "id" Int32 :> Get '[JSON] (Maybe NodeBO)

nodeToNodeBO :: Node -> NodeBO
nodeToNodeBO (Node id' roles address status) =
  NodeBO (id') (V.toList roles) (TL.toStrict address) (TL.toStrict status)

listNodes :: Client -> IO [NodeBO]
listNodes hClient = do
  HStreamApi{..} <- hstreamApiClient hClient
  let listNodeRequest = ListNodesRequest {}
  resp <- hstreamApiListNodes (ClientNormalRequest listNodeRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 _status _details -> do
      case x of
        ListNodesResponse {listNodesResponseNodes = nodes} -> do
          return $ V.toList $ V.map nodeToNodeBO nodes
        _ -> return []
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

getStoreNodeHandler :: Client -> Int32 -> Handler (Maybe NodeBO)
getStoreNodeHandler hClient target = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let getNodeRequest = GetNodeRequest { getNodeRequestId = target}
  resp <- hstreamApiGetNode (ClientNormalRequest getNodeRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return $ Just $ nodeToNodeBO x
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

listStoreNodesHandler :: Client -> Handler [NodeBO]
listStoreNodesHandler hClient = liftIO $ listNodes hClient

nodeServer :: Client -> Server NodesAPI
nodeServer hClient = do
  listStoreNodesHandler hClient :<|> (getStoreNodeHandler hClient)
