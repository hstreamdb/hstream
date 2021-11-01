{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.Connector
  ( ConnectorsAPI, connectorServer
  , listConnectorsHandler
  , ConnectorBO(..)
  ) where

import           Control.Monad                (void)
import           Control.Monad.IO.Class       (liftIO)
import           Data.Aeson                   (FromJSON, ToJSON)
import           Data.Int                     (Int64)
import           Data.Swagger                 (ToSchema)
import qualified Data.Text                    as T
import qualified Data.Vector                  as V
import           GHC.Generics                 (Generic)
import           Network.GRPC.LowLevel.Client (Client)
import           Proto3.Suite                 (def)
import           Servant                      (Capture, Delete, Get, JSON, Post,
                                               ReqBody, type (:>), (:<|>) (..))
import           Servant.Server               (Handler, Server)

import           HStream.HTTP.Server.Utils
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

instance ToJSON   ConnectorBO
instance FromJSON ConnectorBO
instance ToSchema ConnectorBO

type ConnectorsAPI
  =    "connectors" :> Get '[JSON] [ConnectorBO]
  :<|> "connectors" :> "restart"   :> Capture "name" String :> Post '[JSON] ()
  :<|> "connectors" :> "terminate" :> Capture "name" String :> Post '[JSON] ()
  :<|> "connectors" :> ReqBody '[JSON] SQLCmd :> Post '[JSON] ConnectorBO
  :<|> "connectors" :> Capture "name" String :> Delete '[JSON] ()
  :<|> "connectors" :> Capture "name" String :> Get '[JSON] ConnectorBO

connectorToConnectorBO :: Connector -> ConnectorBO
connectorToConnectorBO Connector{..} = ConnectorBO
  { id          = Just connectorId
  , status      = Just $ TaskStatus connectorStatus
  , createdTime = Just connectorCreatedTime
  , sql         = connectorSql
  }

createConnectorHandler :: Client -> SQLCmd -> Handler ConnectorBO
createConnectorHandler hClient SQLCmd{..} = do
  resp <- liftIO $ do
    Log.debug $ "Send create connector request to HStream server. "
             <> "SQL statement: " <> Log.buildText sqlCmd
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiCreateSinkConnector . mkClientNormalRequest $ def
      { createSinkConnectorRequestSql = sqlCmd }
  connectorToConnectorBO <$> getServerResp' resp

listConnectorsHandler :: Client -> Handler [ConnectorBO]
listConnectorsHandler hClient = do
  resp <- liftIO $ do
    HStreamApi{..} <- hstreamApiClient hClient
    Log.debug "Send list connector request to HStream server. "
    hstreamApiListConnectors (mkClientNormalRequest ListConnectorsRequest)
  V.toList . V.map connectorToConnectorBO . listConnectorsResponseConnectors
    <$> getServerResp' resp

deleteConnectorHandler :: Client -> String -> Handler ()
deleteConnectorHandler hClient cid = do
  resp <- liftIO $ do
    Log.debug $ "Send create connector request to HStream server. "
             <> "SQL statement: " <> Log.buildString cid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiDeleteConnector . mkClientNormalRequest $ def
      { deleteConnectorRequestId = T.pack cid }
  void $ getServerResp' resp

getConnectorHandler :: Client -> String -> Handler ConnectorBO
getConnectorHandler hClient cid = do
  resp <- liftIO $ do
    Log.debug $ "Send create connector request to HStream server. "
            <> "Connector ID: " <> Log.buildString cid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiGetConnector . mkClientNormalRequest $ def
      { getConnectorRequestId = T.pack cid }
  connectorToConnectorBO <$> getServerResp' resp

restartConnectorHandler :: Client -> String -> Handler ()
restartConnectorHandler hClient cid = do
  resp <- liftIO $ do
    Log.debug $ "Send restart connector request to HStream server. "
            <> "Connector ID: " <> Log.buildString cid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiRestartConnector . mkClientNormalRequest $ def
      { restartConnectorRequestId = T.pack cid }
  void $ getServerResp' resp

terminateConnectorHandler :: Client -> String -> Handler ()
terminateConnectorHandler hClient cid = do
  resp <- liftIO $ do
    Log.debug $ "Send termiante connector request to HStream server. "
            <> "Connector ID: " <> Log.buildString cid
    HStreamApi{..} <- hstreamApiClient hClient
    hstreamApiTerminateConnector . mkClientNormalRequest $ def
      { terminateConnectorRequestConnectorId = T.pack cid }
  void $ getServerResp' resp

connectorServer :: Client -> Server ConnectorsAPI
connectorServer hClient
  =    listConnectorsHandler     hClient
  :<|> restartConnectorHandler   hClient
  :<|> terminateConnectorHandler hClient
  :<|> createConnectorHandler    hClient
  :<|> deleteConnectorHandler    hClient
  :<|> getConnectorHandler       hClient
