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
  ConnectorsAPI, connectorServer, listConnectorsHandler
) where

import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.Int                         (Int64)
import qualified Data.Map.Strict                  as Map
import           Data.Swagger                     (ToSchema)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Client     (Client)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   Post, ReqBody, type (:>),
                                                   (:<|>) (..))
import           Servant.Server                   (Handler, Server)
import qualified Z.IO.Logger                      as Log

import           HStream.Server.HStreamApi

-- BO is short for Business Object
data ConnectorBO = ConnectorBO
  { id          :: Maybe T.Text
  , status      :: Maybe Int
  , createdTime :: Maybe Int64
  , sql         :: T.Text
  } deriving (Eq, Show, Generic)

instance ToJSON ConnectorBO
instance FromJSON ConnectorBO
instance ToSchema ConnectorBO

type ConnectorsAPI =
  "connectors" :> Get '[JSON] [ConnectorBO]
  :<|> "connectors" :> "restart" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "connectors" :> "cancel" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "connectors" :> ReqBody '[JSON] ConnectorBO :> Post '[JSON] ConnectorBO
  :<|> "connectors" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "connectors" :> Capture "name" String :> Get '[JSON] (Maybe ConnectorBO)

connectorToConnectorBO :: Connector -> ConnectorBO
connectorToConnectorBO (Connector id' status createdTime queryText) =
  ConnectorBO (Just $ TL.toStrict id') (Just $ fromIntegral status) (Just createdTime) (TL.toStrict queryText)

createConnectorHandler :: Client -> ConnectorBO -> Handler ConnectorBO
createConnectorHandler hClient (ConnectorBO _ _ _ sql) = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let createSinkConnectorRequest = CreateSinkConnectorRequest { createSinkConnectorRequestSql = TL.pack $ T.unpack sql }
  resp <- hstreamApiCreateSinkConnector (ClientNormalRequest createSinkConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    -- TODO: should return connectorBO; but we need to update hstream api first
    ClientNormalResponse _ _meta1 _meta2 _status _details -> return $ ConnectorBO Nothing Nothing Nothing sql
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return $ ConnectorBO Nothing Nothing Nothing sql

listConnectorsHandler :: Client -> Handler [ConnectorBO]
listConnectorsHandler hClient = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let listConnectorRequest = ListConnectorsRequest {}
  resp <- hstreamApiListConnectors (ClientNormalRequest listConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x@ListConnectorsResponse{} _meta1 _meta2 _status _details -> do
      case x of
        ListConnectorsResponse {listConnectorsResponseConnectors = connectors} -> do
          return $ V.toList $ V.map connectorToConnectorBO connectors
        _ -> return []
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return []

deleteConnectorHandler :: Client -> String -> Handler Bool
deleteConnectorHandler hClient cid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let deleteConnectorRequest = DeleteConnectorRequest { deleteConnectorRequestId = TL.pack cid }
  resp <- hstreamApiDeleteConnector (ClientNormalRequest deleteConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse x _meta1 _meta2 StatusInternal _details -> return True
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False
    _ -> return False

getConnectorHandler :: Client -> String -> Handler (Maybe ConnectorBO)
getConnectorHandler hClient cid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let getConnectorRequest = GetConnectorRequest { getConnectorRequestId = TL.pack cid }
  resp <- hstreamApiGetConnector (ClientNormalRequest getConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return $ Just $ connectorToConnectorBO x
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> return Nothing
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return Nothing

restartConnectorHandler :: Client -> String -> Handler Bool
restartConnectorHandler hClient cid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let restartConnectorRequest = RestartConnectorRequest { restartConnectorRequestId = TL.pack cid }
  resp <- hstreamApiRestartConnector (ClientNormalRequest restartConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse _ _meta1 _meta2 StatusInternal _details -> return False
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False

cancelConnectorHandler :: Client -> String -> Handler Bool
cancelConnectorHandler hClient cid = liftIO $ do
  HStreamApi{..} <- hstreamApiClient hClient
  let cancelConnectorRequest = CancelConnectorRequest { cancelConnectorRequestId = TL.pack cid }
  resp <- hstreamApiCancelConnector (ClientNormalRequest cancelConnectorRequest 100 (MetadataMap $ Map.empty))
  case resp of
    ClientNormalResponse x _meta1 _meta2 StatusOk _details -> return True
    ClientNormalResponse x _meta1 _meta2 StatusInternal _details -> return False
    ClientErrorResponse clientError -> do
      putStrLn $ "Client Error: " <> show clientError
      return False

connectorServer :: Client -> Server ConnectorsAPI
connectorServer hClient =
  listConnectorsHandler hClient
  :<|> restartConnectorHandler hClient
  :<|> cancelConnectorHandler hClient
  :<|> createConnectorHandler hClient
  :<|> deleteConnectorHandler hClient
  :<|> getConnectorHandler hClient
