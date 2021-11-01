{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Node
  ( NodesAPI, nodeServer
  , listStoreNodesHandler
  , NodeBO
  ) where

import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int32)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Vector                  as V
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Get, JSON, type (:>),
                                               (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils
import qualified HStream.Logger               as Log
import           HStream.Server.HStreamApi

-- BO is short for Business Object
data NodeBO = NodeBO
  { id      :: Int32
  , roles   :: [Int32]
  , address :: T.Text
  , status  :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON   NodeBO
instance FromJSON NodeBO
instance ToSchema NodeBO

type NodesAPI
  =    "nodes" :> Get '[JSON] [NodeBO]
  :<|> "nodes" :> Capture "id" Int32 :> Get '[JSON] NodeBO

nodeToNodeBO :: Node -> NodeBO
nodeToNodeBO (Node id' roles address status) =
  NodeBO id' (V.toList roles) address status

listStoreNodesHandler :: Client -> Handler [NodeBO]
listStoreNodesHandler hClient = do
  resp <- liftIO $ do
    Log.debug "Send list nodes request to HStream server. "
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiListNodes (mkClientNormalRequest ListNodesRequest)
  V.toList . V.map nodeToNodeBO . listNodesResponseNodes <$> getServerResp' resp

getStoreNodeHandler :: Client -> Int32 -> Handler NodeBO
getStoreNodeHandler hClient target = do
  resp <- liftIO $ do
    Log.debug $ "Send get store node request to HStream server. "
             <> "Node ID: " <> Log.buildString (show target)
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiGetNode . mkClientNormalRequest $ def
      { getNodeRequestId = target}
  nodeToNodeBO <$> getServerResp' resp

nodeServer :: Client -> Server NodesAPI
nodeServer hClient
  =    listStoreNodesHandler hClient
  :<|> getStoreNodeHandler   hClient
