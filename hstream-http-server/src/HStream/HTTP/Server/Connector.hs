{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.HTTP.Server.Connector (
  ConnectorsAPI, connectorServer
) where

import           Control.Concurrent               (forkIO)
import           Control.Exception                (SomeException, catch, try)
import           Control.Monad                    (void)
import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import           Data.Int                         (Int64)
import           Data.List                        (find)
import           Data.Swagger                     (ToSchema)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import           GHC.Generics                     (Generic)
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Client     (Client)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   Post, ReqBody, type (:>),
                                                   (:<|>) (..))
import           Servant.Server                   (Handler, Server)
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log
import qualified ZooKeeper.Types                  as ZK

import qualified HStream.Connector.HStore         as HCH
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import qualified HStream.Server.Handler           as Handler
import qualified HStream.Server.Handler.Common    as Handler
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils.Converter          (cbytesToText)

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

hstreamConnectorToConnectorBO :: HSP.Connector -> ConnectorBO
hstreamConnectorToConnectorBO (HSP.Connector connectorId (HSP.Info sqlStatement createdTime) (HSP.Status status _)) =
  ConnectorBO (Just $ cbytesToText connectorId) (Just $ fromEnum status) (Just createdTime) (T.pack $ ZT.unpack sqlStatement)

hstreamConnectorNameIs :: T.Text -> HSP.Connector -> Bool
hstreamConnectorNameIs name (HSP.Connector connectorId _ _) = cbytesToText connectorId == name

removeConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
removeConnectorHandler _ldClient zkHandle name = liftIO $ catch
  (HSP.withMaybeZHandle zkHandle (HSP.removeConnector (ZDC.pack name)) >> return True)
  (\(_ :: SomeException) -> return False)

-- TODO: we should remove the duplicate code in HStream/Admin/Server/Connector.hs and HStream/Server/Handler.hs
createConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> ConnectorBO -> Handler ConnectorBO
createConnectorHandler ldClient zkHandle connector = liftIO $ do
  plan' <- try $ HSC.streamCodegen $ sql connector
  case plan' of
    Left  (_ :: SomeSQLException) -> returnErr "exception on parsing or codegen"
    Right (HSC.CreateSinkConnectorPlan cName ifNotExist sName cConfig _) -> do
      streamExists <- HS.doesStreamExists ldClient (HCH.transToStreamName sName)
      connectorIds <- HSP.withMaybeZHandle zkHandle HSP.getConnectorIds
      let connectorExists = elem (T.unpack cName) $ map HSP.getSuffix connectorIds
      if streamExists then
        if connectorExists then if ifNotExist then return () else returnErr "connector exists"
        else void $ Handler.handleCreateSinkConnector
          Handler.ServerContext {Handler.scLDClient = ldClient, Handler.zkHandle = zkHandle}
          (TL.fromStrict $ sql connector) cName sName cConfig
      else returnErr "stream does not exist"
    _ -> returnErr "wrong methods called"
    -- TODO: return error code
  return connector
  where
    returnErr = Log.fatal . string8

fetchConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> Handler [ConnectorBO]
fetchConnectorHandler ldClient zkHandle = do
  connectors <- liftIO $ HSP.withMaybeZHandle zkHandle HSP.getConnectors
  return $ map hstreamConnectorToConnectorBO connectors

getConnectorHandler :: Maybe ZK.ZHandle -> String -> Handler (Maybe ConnectorBO)
getConnectorHandler zkHandle name = do
  connector <- liftIO $ do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    return $ find (hstreamConnectorNameIs (T.pack name)) connectors
  return $ hstreamConnectorToConnectorBO <$> connector

-- Question: What else should I do to really restart the connector? Unsubscribe and Stop CkpReader?
restartConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
restartConnectorHandler ldClient zkHandle name = liftIO $ do
  connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
  case find (hstreamConnectorNameIs (T.pack name)) connectors of
    Just connector -> do
      _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus (HSP.connectorId connector) HSP.Running)
      return True
    Nothing -> return False

cancelConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
cancelConnectorHandler ldClient zkHandle name = liftIO $ do
  connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
  case find (hstreamConnectorNameIs (T.pack name)) connectors of
    Just connector -> do
      _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus (HSP.connectorId connector) HSP.Terminated)
      return True
    Nothing -> return False

connectorServer :: HS.LDClient -> Maybe ZK.ZHandle -> Client -> Server ConnectorsAPI
connectorServer ldClient zkHandle hClient =
  fetchConnectorHandler ldClient zkHandle
  :<|> restartConnectorHandler ldClient zkHandle
  :<|> cancelConnectorHandler ldClient zkHandle
  :<|> createConnectorHandler ldClient zkHandle
  :<|> removeConnectorHandler ldClient zkHandle
  :<|> getConnectorHandler zkHandle
