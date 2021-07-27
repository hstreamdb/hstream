{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Overview (
  OverviewAPI, overviewServer
) where

import           Data.Aeson                    (FromJSON, ToJSON)
import           Data.Maybe                    (fromMaybe)
import           Data.Swagger                  (ToSchema)
import           GHC.Generics                  (Generic)
import           Network.GRPC.LowLevel.Client  (Client)
import           Servant                       (Get, JSON, type (:>))
import           Servant.Server                (Handler, Server)

import           HStream.HTTP.Server.Connector (listConnectorsHandler)
import           HStream.HTTP.Server.Node      (listStoreNodesHandler)
import           HStream.HTTP.Server.Query     (listQueriesHandler)
import           HStream.HTTP.Server.Stream    (listStreamsHandler)
import           HStream.HTTP.Server.View      (listViewsHandler)
import           HStream.Server.HStreamApi
import qualified HStream.Store                 as HS

-- BO is short for Business Object
data OverviewBO = OverviewBO
  { streams    :: Int
  , queries    :: Int
  , views      :: Int
  , connectors :: Int
  , nodes      :: Int
  } deriving (Eq, Show, Generic)

instance ToJSON OverviewBO
instance FromJSON OverviewBO
instance ToSchema OverviewBO

type OverviewAPI =
  "overview" :> Get '[JSON] OverviewBO

getOverviewHandler :: Client -> HS.LDClient -> Handler OverviewBO
getOverviewHandler hClient ldClient = do
  overview <- do
    streamCnt <- length <$> listStreamsHandler ldClient
    queryCnt <- length <$> listQueriesHandler hClient
    viewCnt <- length <$> listViewsHandler hClient
    connectorCnt <- length <$> listConnectorsHandler hClient
    nodeCnt <- length <$> listStoreNodesHandler hClient
    return $ OverviewBO streamCnt queryCnt viewCnt connectorCnt nodeCnt
  return overview

overviewServer :: Client -> HS.LDClient -> Server OverviewAPI
overviewServer hClient ldClient = do
  getOverviewHandler hClient ldClient
