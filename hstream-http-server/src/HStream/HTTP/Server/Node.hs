{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Node (
  NodesAPI, nodeServer, listStoreNodesHandler, NodeBO
) where

import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int32)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Text.Lazy               as TL
import qualified Data.Vector                  as V
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Get, JSON, type (:>),
                                               (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils    (getServerResp,
                                               mkClientNormalRequest)
import           HStream.Server.HStreamApi

-- BO is short for Business Object
data NodeBO = NodeBO
  { id      :: Int32
  , roles   :: [Int32]
  , address :: T.Text
  , status  :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON NodeBO
instance FromJSON NodeBO
instance ToSchema NodeBO

type NodesAPI =
  "nodes" :> Get '[JSON] [NodeBO]
  :<|> "nodes" :> Capture "id" Int32 :> Get '[JSON] (Maybe NodeBO)

nodeToNodeBO :: Node -> NodeBO
nodeToNodeBO (Node id' roles address status) =
  NodeBO id' (V.toList roles) (TL.toStrict address) (TL.toStrict status)

listNodes :: Client -> IO [NodeBO]
listNodes hClient = do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiListNodes (mkClientNormalRequest ListNodesRequest)
  maybe [] (V.toList . V.map nodeToNodeBO . listNodesResponseNodes) <$> getServerResp resp

getStoreNodeHandler :: Client -> Int32 -> Handler (Maybe NodeBO)
getStoreNodeHandler hClient target = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiGetNode
    (mkClientNormalRequest def { getNodeRequestId = target})
  (nodeToNodeBO <$>) <$> getServerResp resp

listStoreNodesHandler :: Client -> Handler [NodeBO]
listStoreNodesHandler hClient = liftIO $ listNodes hClient

nodeServer :: Client -> Server NodesAPI
nodeServer hClient = listStoreNodesHandler hClient
                :<|> getStoreNodeHandler hClient
