{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Overview
  ( OverviewAPI , overviewServer
  ) where

import           Data.Aeson                    (FromJSON, ToJSON)
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

-- BO is short for Business Object
data OverviewBO = OverviewBO
  { streams    :: Int
  , queries    :: Int
  , views      :: Int
  , connectors :: Int
  , nodes      :: Int
  } deriving (Eq, Show, Generic)

instance ToJSON   OverviewBO
instance FromJSON OverviewBO
instance ToSchema OverviewBO

type OverviewAPI
  = "overview" :> Get '[JSON] OverviewBO

getOverviewHandler :: Client -> Handler OverviewBO
getOverviewHandler hClient = do
    streamCnt    <- length <$> listStreamsHandler    hClient
    queryCnt     <- length <$> listQueriesHandler    hClient
    viewCnt      <- length <$> listViewsHandler      hClient
    connectorCnt <- length <$> listConnectorsHandler hClient
    nodeCnt      <- length <$> listStoreNodesHandler hClient
    return $ OverviewBO streamCnt queryCnt viewCnt connectorCnt nodeCnt

overviewServer :: Client -> Server OverviewAPI
overviewServer = getOverviewHandler
