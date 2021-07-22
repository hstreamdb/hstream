{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Connector where

import           Control.Exception                (SomeException, catch, try)
import           Data.List                        (find)
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log

import qualified HStream.Connector.HStore         as HCH
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   handleCreateSinkConnector,
                                                   handleTerminateConnector)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (cbytesToText, returnErrResp,
                                                   returnResp)

hstreamConnectorToConnector :: HSP.Connector -> Connector
hstreamConnectorToConnector (HSP.Connector connectorId (HSP.Info sqlStatement createdTime) (HSP.Status status _)) =
  Connector (TL.pack $ ZDC.unpack connectorId) (fromIntegral $ fromEnum status) createdTime (TL.pack $ ZT.unpack sqlStatement)

hstreamConnectorNameIs :: T.Text -> HSP.Connector -> Bool
hstreamConnectorNameIs name (HSP.Connector connectorId _ _) = cbytesToText connectorId == name

createConnector :: ServerContext -> T.Text -> Bool -> IO (Either String Connector)
createConnector sc@ServerContext{..} sql isCreate = do
  plan' <- try $ HSC.streamCodegen sql
  case plan' of
    Left  (_ :: SomeSQLException) -> return $ Left "exception on parsing or codegen"
    Right (HSC.CreateSinkConnectorPlan cName ifNotExist sName cConfig _) -> do
      streamExists <- HS.doesStreamExists scLDClient (HCH.transToStreamName sName)
      connectorIds <- HSP.withMaybeZHandle zkHandle HSP.getConnectorIds
      let connectorExists = elem (T.unpack cName) $ map HSP.getSuffix connectorIds
      if streamExists then
        if connectorExists && isCreate then
          if ifNotExist then
            return $ Right $ Connector "" (fromIntegral $ fromEnum HSP.Running) 0 ""
          else return $ Left "connector exists"
        else do
          (cid, timestamp) <- handleCreateSinkConnector sc sql cName sName cConfig
          return $ Right $ Connector (TL.pack $ ZDC.unpack cid) (fromIntegral $ fromEnum HSP.Running) timestamp (TL.pack $ T.unpack sql)
      else return $ Left "stream does not exist"
    _ -> return $ Left "inconsistent method called"

createSinkConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createSinkConnectorHandler sc (ServerNormalRequest _ CreateSinkConnectorRequest{..}) = do
  err <- createConnector sc (TL.toStrict createSinkConnectorRequestSql) True
  case err of
    Left err'        -> do
      Log.fatal . string8 $ err'
      returnErrResp StatusInternal "Failed"
    Right connector  -> returnResp connector

listConnectorsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorsRequest ListConnectorsResponse
  -> IO (ServerResponse 'Normal ListConnectorsResponse)
listConnectorsHandler ServerContext{..} (ServerNormalRequest _metadata _) = do
  connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
  let records = map hstreamConnectorToConnector connectors
  let resp = ListConnectorsResponse . V.fromList $ records
  returnResp resp

getConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
getConnectorHandler ServerContext{..} (ServerNormalRequest _metadata GetConnectorRequest{..}) = do
  connector <- do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    return $ find (hstreamConnectorNameIs (T.pack $ TL.unpack getConnectorRequestId)) connectors
  case connector of
    Just q -> returnResp $ hstreamConnectorToConnector q
    _      -> returnErrResp StatusInternal "Not Exist"

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler ServerContext{..} (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = do
  catch
    (HSP.withMaybeZHandle zkHandle (HSP.removeConnector $ ZDC.pack $ TL.unpack deleteConnectorRequestId) >> returnResp Empty)
    (\(_ :: SomeException) -> returnErrResp StatusInternal "Failed")

restartConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata RestartConnectorRequest{..}) = do
  connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
  case find (hstreamConnectorNameIs (T.pack $ TL.unpack restartConnectorRequestId)) connectors of
    Just (HSP.Connector cId HSP.Info{..} HSP.Status{..})  -> do
      if status == HSP.Terminated
        then do
          err <- createConnector sc (T.pack $ ZT.unpack sqlStatement) False
          case err of
            Left err' -> do
              Log.fatal . string8 $ err'
              returnErrResp StatusInternal "Failed"
            Right _ -> do
              HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus cId HSP.Running
              returnResp Empty
        -- If the connector is already started, nothing should be done.
        else returnResp Empty
    Nothing -> returnErrResp StatusInternal "Not exist"

cancelConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CancelConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
cancelConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata CancelConnectorRequest{..}) = do
  connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
  case find (hstreamConnectorNameIs (T.pack $ TL.unpack cancelConnectorRequestId)) connectors of
    Just connector -> do
      handleTerminateConnector sc (HSP.connectorId connector)
      returnResp Empty
    Nothing -> returnErrResp StatusInternal "failed"
