{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.Connector (
  ConnectorsAPI, connectorServer, listConnectorsHandler, ConnectorBO(..)
) where

import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int64)
import           Data.Maybe                   (isJust)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Text.Lazy               as TL
import qualified Data.Vector                  as V
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Delete, Get, JSON, Post,
                                               ReqBody, type (:>), (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils    (getServerResp,
                                               mkClientNormalRequest)
import qualified HStream.Logger               as Log
import           HStream.Server.HStreamApi
import           HStream.Utils                (TaskStatus (..))

-- BO is short for Business Object
data ConnectorBO = ConnectorBO
  { id          :: Maybe T.Text
  , status      :: Maybe TaskStatus
  , createdTime :: Maybe Int64
  , sql         :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON ConnectorBO
instance FromJSON ConnectorBO
instance ToSchema ConnectorBO

type ConnectorsAPI =
  "connectors" :> Get '[JSON] [ConnectorBO]
  :<|> "connectors" :> "restart" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "connectors" :> "terminate" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "connectors" :> ReqBody '[JSON] ConnectorBO :> Post '[JSON] ConnectorBO
  :<|> "connectors" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "connectors" :> Capture "name" String :> Get '[JSON] (Maybe ConnectorBO)

connectorToConnectorBO :: Connector -> ConnectorBO
connectorToConnectorBO Connector{..} = ConnectorBO
  { id = Just $ TL.toStrict connectorId
  , status = Just (TaskStatus connectorStatus)
  , createdTime = Just connectorCreatedTime
  , sql = TL.toStrict connectorSql }

createConnectorHandler :: Client -> ConnectorBO -> Handler ConnectorBO
createConnectorHandler hClient (ConnectorBO _ _ _ sql) = liftIO $ do
  Log.debug $ "Send create connector request to HStream server. "
    <> "SQL statement: " <> Log.buildText sql
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiCreateSinkConnector
    (mkClientNormalRequest def { createSinkConnectorRequestSql = TL.fromStrict sql } )
  maybe (ConnectorBO Nothing Nothing Nothing sql) connectorToConnectorBO <$> getServerResp resp

listConnectorsHandler :: Client -> Handler [ConnectorBO]
listConnectorsHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  Log.debug "Send list connector request to HStream server. "
  resp <- hstreamApiListConnectors
    (mkClientNormalRequest ListConnectorsRequest)
  maybe []
    (V.toList . V.map connectorToConnectorBO . listConnectorsResponseConnectors)
    <$> getServerResp resp

deleteConnectorHandler :: Client -> String -> Handler Bool
deleteConnectorHandler hClient cid = liftIO $ do
  Log.debug $ "Send create connector request to HStream server. "
    <> "SQL statement: " <> Log.buildString cid
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiDeleteConnector
    (mkClientNormalRequest def { deleteConnectorRequestId = TL.pack cid } )
  isJust <$> getServerResp resp

getConnectorHandler :: Client -> String -> Handler (Maybe ConnectorBO)
getConnectorHandler hClient cid = liftIO $ do
  Log.debug $ "Send create connector request to HStream server. "
    <> "Connector ID: " <> Log.buildString cid
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiGetConnector
    (mkClientNormalRequest def { getConnectorRequestId = TL.pack cid })
  (connectorToConnectorBO <$>) <$> getServerResp resp

restartConnectorHandler :: Client -> String -> Handler Bool
restartConnectorHandler hClient cid = liftIO $ do
  Log.debug $ "Send restart connector request to HStream server. "
    <> "Connector ID: " <> Log.buildString cid
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiRestartConnector
    (mkClientNormalRequest def { restartConnectorRequestId = TL.pack cid })
  isJust <$> getServerResp resp

terminateConnectorHandler :: Client -> String -> Handler Bool
terminateConnectorHandler hClient cid = liftIO $ do
  Log.debug $ "Send termiante connector request to HStream server. "
    <> "Connector ID: " <> Log.buildString cid
  HStreamApi{..} <- hstreamApiClient hClient
  resp <- hstreamApiTerminateConnector
    (mkClientNormalRequest def { terminateConnectorRequestConnectorId = TL.pack cid })
  isJust <$> getServerResp resp

connectorServer :: Client -> Server ConnectorsAPI
connectorServer hClient =
  listConnectorsHandler hClient
  :<|> restartConnectorHandler hClient
  :<|> terminateConnectorHandler hClient
  :<|> createConnectorHandler hClient
  :<|> deleteConnectorHandler hClient
  :<|> getConnectorHandler hClient
