{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedLists     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Connector where

import           Control.Concurrent               (forkIO, killThread, putMVar,
                                                   takeMVar)
import           Control.Exception                (SomeException, catch, try)
import qualified Data.HashMap.Strict              as HM
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import           Proto3.Suite                     (Enumerated (..))
import           Z.Data.Builder.Base              (string8)
import qualified Z.Data.CBytes                    as ZDC
import qualified Z.Data.Text                      as ZT
import qualified Z.IO.Logger                      as Log
import           Z.IO.Time                        (SystemTime (..),
                                                   getSystemTime')

import qualified HStream.Connector.HStore         as HCH
import           HStream.Processing.Connector     (subscribeToStream)
import           HStream.Processing.Processor     (getTaskName,
                                                   taskBuilderWithName)
import           HStream.Processing.Type          (Offset (..))
import qualified HStream.SQL.Codegen              as HSC
import           HStream.SQL.Exception            (SomeSQLException)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   handleCreateSinkConnector)
import qualified HStream.Server.Persistence       as HSP
import qualified HStream.Store                    as HS
import           HStream.Utils.Converter          (cbytesToText, textToCBytes)

hstreamConnectorToGetConnectorResponse :: HSP.Connector -> GetConnectorResponse
hstreamConnectorToGetConnectorResponse (HSP.Connector connectorId (HSP.Info sqlStatement createdTime) (HSP.Status status _)) =
  GetConnectorResponse (TL.pack $ ZDC.unpack connectorId) (fromIntegral $ fromEnum status) createdTime (TL.pack $ ZT.unpack sqlStatement) (Enumerated $ Right HStreamServerErrorNoError)

emptyGetConnectorResponse :: GetConnectorResponse
emptyGetConnectorResponse = GetConnectorResponse "" 0 0 "" (Enumerated $ Right HStreamServerErrorNotExistError)

hstreamConnectorNameIs :: T.Text -> HSP.Connector -> Bool
hstreamConnectorNameIs name (HSP.Connector connectorId _ _) = (cbytesToText connectorId) == name

createSinkConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest CreateSinkConnectorResponse
  -> IO (ServerResponse 'Normal CreateSinkConnectorResponse)
createSinkConnectorHandler sc@ServerContext{..} (ServerNormalRequest _ CreateSinkConnectorRequest{..}) = do
  plan' <- try $ HSC.streamCodegen $ (TL.toStrict createSinkConnectorRequestSql)
  err <- case plan' of
    Left  (_ :: SomeSQLException) -> return $ Just "exception on parsing or codegen"
    Right (HSC.CreateSinkConnectorPlan cName ifNotExist sName cConfig _) -> do
      streamExists <- HS.doesStreamExists scLDClient (HCH.transToStreamName sName)
      connectorIds <- HSP.withMaybeZHandle zkHandle HSP.getConnectorIds
      let connectorExists = elem (T.unpack cName) $ map HSP.getSuffix connectorIds
      if streamExists then
        if connectorExists then if ifNotExist then return Nothing else return $ Just "connector exists"
        else handleCreateSinkConnector sc createSinkConnectorRequestSql cName sName cConfig >> return Nothing
      else return $ Just "stream does not exist"
  case err of
    Just err -> do
      Log.fatal . string8 $ err
      return (ServerNormalResponse (CreateSinkConnectorResponse False) [] StatusOk  "")
    Nothing  -> return (ServerNormalResponse (CreateSinkConnectorResponse True) [] StatusOk  "")

listConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorRequest ListConnectorResponse
  -> IO (ServerResponse 'Normal ListConnectorResponse)
listConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata _) = do
  connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
  let records = map hstreamConnectorToGetConnectorResponse connectors
  let resp = ListConnectorResponse . V.fromList $ records
  return (ServerNormalResponse resp [] StatusOk "")

getConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorRequest GetConnectorResponse
  -> IO (ServerResponse 'Normal GetConnectorResponse)
getConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata GetConnectorRequest{..}) = do
  connector <- do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    return $ find (hstreamConnectorNameIs (T.pack $ TL.unpack getConnectorRequestId)) connectors
  let resp = case connector of
        Just q -> hstreamConnectorToGetConnectorResponse q
        _      ->  emptyGetConnectorResponse
  return (ServerNormalResponse resp [] StatusOk "")

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest DeleteConnectorResponse
  -> IO (ServerResponse 'Normal DeleteConnectorResponse)
deleteConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = do
  res <- catch
    ((HSP.withMaybeZHandle zkHandle $ HSP.removeConnector (ZDC.pack $ TL.unpack deleteConnectorRequestId)) >> return True)
    (\(e :: SomeException) -> return False)
  return (ServerNormalResponse (DeleteConnectorResponse res) [] StatusOk "")

restartConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartConnectorRequest RestartConnectorResponse
  -> IO (ServerResponse 'Normal RestartConnectorResponse)
restartConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata RestartConnectorRequest{..}) = do
  res <- do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    case find (hstreamConnectorNameIs (T.pack $ TL.unpack restartConnectorRequestId)) connectors of
      Just connector -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus (HSP.connectorId connector) HSP.Running)
        return True
      Nothing -> return False
  return (ServerNormalResponse (RestartConnectorResponse res) [] StatusOk "")

cancelConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CancelConnectorRequest CancelConnectorResponse
  -> IO (ServerResponse 'Normal CancelConnectorResponse)
cancelConnectorHandler sc@ServerContext{..} (ServerNormalRequest _metadata CancelConnectorRequest{..}) = do
  res <- do
    connectors <- HSP.withMaybeZHandle zkHandle HSP.getConnectors
    case find (hstreamConnectorNameIs (T.pack $ TL.unpack cancelConnectorRequestId)) connectors of
      Just connector -> do
        _ <- forkIO (HSP.withMaybeZHandle zkHandle $ HSP.setConnectorStatus (HSP.connectorId connector) HSP.Terminated)
        return True
      Nothing -> return False
  return (ServerNormalResponse (CancelConnectorResponse res) [] StatusOk "")
