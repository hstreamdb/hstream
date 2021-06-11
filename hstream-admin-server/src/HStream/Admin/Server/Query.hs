{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators     #-}

module HStream.Admin.Server.Query (
  QueriesAPI, queryServer
) where

import           Control.Monad.IO.Class   (liftIO)
import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Map.Strict          as Map
import           Data.Text                (Text)
import           GHC.Generics             (Generic)
import           Servant                  (Capture, Delete, Get, JSON,
                                           PlainText, Post, ReqBody, type (:>),
                                           (:<|>) (..))
import           Servant.Server           (Handler, Server)
import qualified Z.Data.CBytes            as ZDC
import qualified ZooKeeper.Types             as ZK

import           HStream.Connector.HStore as HCH
import           HStream.Store            as HS
import           HStream.Server.Persistence  as HSP

data QueryBO = QueryBO
  {
    id          :: Text,
    status      :: Int,
    createdTime :: Int,
    queryText   :: Text
  } deriving (Eq, Show, Generic)
instance ToJSON QueryBO
instance FromJSON QueryBO

type QueriesAPI =
  "queries" :> Get '[JSON] [QueryBO]
  -- :<|> "queries" :> ReqBody '[JSON] QueryBO :> Post '[JSON] QueryBO
  -- :<|> "queries" :> Capture "name" String :> Delete '[JSON] Bool
  -- :<|> "queries" :> Capture "name" String :> Get '[JSON] Bool

mockQueries :: [QueryBO]
mockQueries = [QueryBO "test" 0 0 "text", QueryBO "test2" 1 0 "text2"]

-- getQueryHandler :: HS.LDClient -> String -> Handler Bool
-- getQueryHandler ldClient name = do
--   res <- liftIO $ doesQueryExists ldClient (mkQueryName $ ZDC.pack name)
--   return True

-- removeQueryHandler :: HS.LDClient -> String -> Handler Bool
-- removeQueryHandler ldClient name = do
--   liftIO $ removeQuery ldClient (mkQueryName $ ZDC.pack name)
--   return True

-- createQueryHandler :: HS.LDClient -> QueryBO -> Handler QueryBO
-- createQueryHandler ldClient query = do
--   liftIO $ createQuery ldClient (HCH.transToQueryName $ name query)
--           (LogAttrs $ HsLogAttrs (replicationFactor query) Map.empty)
--   return query

fetchQueryHandler :: HS.LDClient -> Maybe ZK.ZHandle -> Handler [QueryBO]
fetchQueryHandler ldClient zkHandle = do
  -- queryNames <- liftIO $ findQueries ldClient True
  queries <- liftIO $ withMaybeZHandle zkHandle HSP.getQueries
  liftIO $ print (show queries)
  return mockQueries

queryServer :: HS.LDClient -> Maybe ZK.ZHandle -> Server QueriesAPI
queryServer ldClient zkHandle =
  (fetchQueryHandler ldClient zkHandle)
  -- :<|> (createQueryHandler ldClient)
  -- :<|> (removeQueryHandler ldClient)
  -- :<|> (getQueryHandler ldClient)
