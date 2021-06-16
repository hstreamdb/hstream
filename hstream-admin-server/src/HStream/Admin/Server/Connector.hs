{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators       #-}

module HStream.Admin.Server.Connector (
  ConnectorsAPI, connectorServer
) where

import           Control.Concurrent               (forkIO)
import           Control.Exception                (SomeException, try)
import           Control.Monad                    (void)
import           Control.Monad.IO.Class           (liftIO)
import           Data.Aeson                       (FromJSON, ToJSON)
import qualified Data.ByteString.Char8            as C
import           Data.Int                         (Int64)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromMaybe)
import qualified Data.Text                        as T
import qualified Database.ClickHouseDriver.Client as ClickHouse
import qualified Database.ClickHouseDriver.Types  as ClickHouse
import qualified Database.MySQL.Base              as MySQL
import           GHC.Generics                     (Generic)
import           RIO                              (async, forM_, forever)
import           Servant                          (Capture, Delete, Get, JSON,
                                                   PlainText, Post, ReqBody,
                                                   type (:>), (:<|>) (..))
import           Servant.Server                   (Handler, Server)
import qualified Z.Data.CBytes                    as CB
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import           Z.IO.Time                        (SystemTime (..),
                                                   getSystemTime')
import qualified ZooKeeper.Types                  as ZK

import           HStream.Connector.ClickHouse     (clickHouseSinkConnector)
import qualified HStream.Connector.HStore         as HCH
import           HStream.Connector.MySQL          (mysqlSinkConnector)
import           HStream.Processing.Connector     (SinkConnector (..),
                                                   SourceConnectorWithoutCkp (..),
                                                   subscribeToStream)
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Type          (Offset (..), SinkRecord (..),
                                                   SourceRecord (..))
import qualified HStream.SQL.AST                  as AST
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import qualified HStream.Server.Exception         as HSE
import           HStream.Server.Handler           (catchZkException,
                                                   runTaskWrapper)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils.Converter          (cbytesToText, textToCBytes)

data ConnectorBO = ConnectorBO
  {
    id          :: Maybe T.Text,
    status      :: Maybe Int,
    createdTime :: Maybe Int64,
    sql         :: T.Text
  } deriving (Eq, Show, Generic)
instance ToJSON ConnectorBO
instance FromJSON ConnectorBO

type ConnectorsAPI =
  "connectors" :> Get '[JSON] [ConnectorBO]
  :<|> "connectors" :> "restart" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "connectors" :> "cancel" :> Capture "name" String :> Post '[JSON] Bool
  :<|> "connectors" :> ReqBody '[JSON] ConnectorBO :> Post '[JSON] ConnectorBO
  -- :<|> "connectors" :> Capture "name" String :> Delete '[JSON] Bool
  :<|> "connectors" :> Capture "name" String :> Get '[JSON] (Maybe ConnectorBO)

hstreamConnectorToConnectorBO :: HSP.Connector -> ConnectorBO
hstreamConnectorToConnectorBO (HSP.Connector connectorId (HSP.Info sqlStatement createdTime) (HSP.Status status _)) =
  ConnectorBO (Just $ cbytesToText connectorId) (Just $ fromEnum status) (Just createdTime) (T.pack $ ZT.unpack sqlStatement)

hstreamConnectorNameIs :: T.Text -> HSP.Connector -> Bool
hstreamConnectorNameIs name (HSP.Connector connectorId _ _) = (cbytesToText connectorId) == name

-- removeConnectorHandler :: HS.LDClient -> String -> Handler Bool
-- removeConnectorHandler ldClient name = do
--   liftIO $ removeConnector ldClient (mkConnectorName $ ZDC.pack name)
--   return True

-- TODO: we should remove the duplicate code in HStream/Admin/Server/Connector.hs and HStream/Server/Handler.hs
createConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> ConnectorBO -> Handler ConnectorBO
createConnectorHandler ldClient zkHandle connector = do
  err <- liftIO $ do
    plan' <- try $ HSC.streamCodegen $ sql connector
    case plan' of
      Left  (_ :: SomeSQLException) -> return $ Just "exception on parsing or codegen"
      Right (HSC.CreateConnectorPlan cName (AST.RConnectorOptions cOptions)) -> do
        let streamM = lookup "streamname" cOptions
            typeM   = lookup "type" cOptions
            fromCOptionString          = \case Just (AST.ConstantString s) -> Just $ C.pack s;    _ -> Nothing
            fromCOptionStringToString  = \case Just (AST.ConstantString s) -> Just s;             _ -> Nothing
            fromCOptionIntToPortNumber = \case Just (AST.ConstantInt s) -> Just $ fromIntegral s; _ -> Nothing
        sk <- case typeM of
          Just (AST.ConstantString cType) -> do
            case cType of
              "clickhouse" -> do
                cli <- ClickHouse.createClient $ ClickHouse.ConnParams
                  (fromMaybe "default"   $ fromCOptionString (lookup "username" cOptions))
                  (fromMaybe "127.0.0.1" $ fromCOptionString (lookup "host"     cOptions))
                  (fromMaybe "9000"      $ fromCOptionString (lookup "port"     cOptions))
                  (fromMaybe ""          $ fromCOptionString (lookup "password" cOptions))
                  False
                  (fromMaybe "default"   $ fromCOptionString (lookup "database" cOptions))
                return $ Right $ clickHouseSinkConnector cli
              "mysql" -> do
                conn <- MySQL.connect $ MySQL.ConnectInfo
                  (fromMaybe "127.0.0.1" $ fromCOptionStringToString   (lookup "host" cOptions))
                  (fromMaybe 3306        $ fromCOptionIntToPortNumber  (lookup "port" cOptions))
                  (fromMaybe "mysql"     $ fromCOptionString       (lookup "database" cOptions))
                  (fromMaybe "root"      $ fromCOptionString       (lookup "username" cOptions))
                  (fromMaybe "password"  $ fromCOptionString       (lookup "password" cOptions))
                  33
                return $ Right $ mysqlSinkConnector conn
              _ -> return $ Left "unsupported sink connector type"
          _ -> return $ Left "invalid type in connector options"
        MkSystemTime timestamp _ <- getSystemTime'
        let cid = CB.pack $ T.unpack $ cName
            cinfo = HSP.Info (ZT.pack $ T.unpack $ sql connector) timestamp
        case sk of
          Left err -> return $ Just err
          Right connector -> case streamM of
            Just (AST.ConstantString stream) -> do
              streamExists <- HS.doesStreamExists ldClient (HS.mkStreamName $ ZDC.pack stream)
              case streamExists of
                False -> do
                  return $ Just $ "Stream " <> stream <> " doesn't exist"
                True -> do
                  ldreader <- HS.newLDReader ldClient 1000 Nothing
                  let sc = HCH.hstoreSourceConnectorWithoutCkp ldClient ldreader
                  catchZkException (HSP.withMaybeZHandle zkHandle $ HSP.insertConnector cid cinfo) HSE.FailedToRecordInfo
                  subscribeToStreamWithoutCkp sc (T.pack stream) Latest
                  _ <- async $ do
                    catchZkException (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus cid HSP.Running) HSE.FailedToSetStatus
                    forever $ do
                      records <- readRecordsWithoutCkp sc
                      forM_ records $ \SourceRecord {..} ->
                        writeRecord connector $ SinkRecord (T.pack stream) srcKey srcValue srcTimestamp
                  return Nothing
            _ -> return $ Just "stream name missed in connector options"
      Right _ -> return $ Just "inconsistent method called"
      -- TODO: return error code
  liftIO $ print err
  return connector

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
restartConnectorHandler ldClient zkHandle name = do
  res <- liftIO $ do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    case find (hstreamConnectorNameIs (T.pack name)) connectors of
      Just connector -> do
        _ <- forkIO $ catchZkException (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus (HSP.connectorId connector) HSP.Running) HSE.FailedToSetStatus
        return True
      Nothing -> return False
  return res

cancelConnectorHandler :: HS.LDClient -> Maybe ZK.ZHandle -> String -> Handler Bool
cancelConnectorHandler ldClient zkHandle name = do
  res <- liftIO $ do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    case find (hstreamConnectorNameIs (T.pack name)) connectors of
      Just connector -> do
        _ <- forkIO $ catchZkException (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus (HSP.connectorId connector) HSP.Terminated) HSE.FailedToSetStatus
        return True
      Nothing -> return False
  return res

connectorServer :: HS.LDClient -> Maybe ZK.ZHandle -> Server ConnectorsAPI
connectorServer ldClient zkHandle =
  (fetchConnectorHandler ldClient zkHandle)
  :<|> (restartConnectorHandler ldClient zkHandle)
  :<|> (cancelConnectorHandler ldClient zkHandle)
  :<|> (createConnectorHandler ldClient zkHandle)
  -- :<|> (removeConnectorHandler ldClient)
  :<|> (getConnectorHandler zkHandle)
