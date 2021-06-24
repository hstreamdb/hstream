{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.HTTP.Server.Overview (
  OverviewAPI, overviewServer
) where

import           Control.Monad.IO.Class     (liftIO)
import           Data.Aeson                 (FromJSON, ToJSON)
import           Data.ByteString            (ByteString)
import qualified Data.Map.Strict            as Map
import           Data.Maybe                 (fromMaybe)
import           Data.Swagger               (ToSchema)
import           Data.Text                  (Text)
import           GHC.Generics               (Generic)
import           Servant                    (Capture, Delete, Get, JSON,
                                             PlainText, Post, ReqBody,
                                             type (:>), (:<|>) (..))
import           Servant.Server             (Handler, Server)
import qualified Z.Data.CBytes              as ZDC
import qualified ZooKeeper                  as ZK
import qualified ZooKeeper.Exception        as ZK
import qualified ZooKeeper.Types            as ZK

import           HStream.Connector.HStore   as HCH
import           HStream.HTTP.Server.Node   (getNodes)
import qualified HStream.Server.Persistence as HSP
import           HStream.Store              as HS
import qualified HStream.Store.Admin.API    as AA
import           HStream.Store.Admin.Types  (SimpleNodesFilter (..),
                                             StatusFormat (..), StatusOpts (..))
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

fetchOverviewHandler :: HS.LDClient -> Maybe ZK.ZHandle -> AA.HeaderConfig AA.AdminAPI -> StatusOpts -> Handler OverviewBO
fetchOverviewHandler ldClient zkHandle headerConfig statusOpts = do
  overview <- liftIO $ do
    streamCnt <- length <$> (findStreams ldClient True)
    queryCnt <- length <$> (HSP.withMaybeZHandle zkHandle HSP.getQueries)
    let views = 0
    connectorCnt <- length <$> (HSP.withMaybeZHandle zkHandle HSP.getConnectors)
    nodes <- (getNodes headerConfig statusOpts)
    let nodeCnt = length $ fromMaybe [] nodes
    return $ OverviewBO streamCnt queryCnt views connectorCnt nodeCnt
  return overview

overviewServer :: HS.LDClient -> Maybe ZK.ZHandle -> ByteString -> Int -> Server OverviewAPI
overviewServer ldClient zkHandle ldAdminHost ldAdminPort = do
  let headerConfig = AA.HeaderConfig ldAdminHost ldAdminPort AA.binaryProtocolId 5000 5000 5000
  let statusOpts = StatusOpts TabularFormat True (StatusNodeIdx []) "ID"
  fetchOverviewHandler ldClient zkHandle headerConfig statusOpts
