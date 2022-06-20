{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Connector where

import           Control.Concurrent               (killThread, readMVar)
import           Control.Exception                (Exception, Handler (..),
                                                   throwIO)
import           Control.Monad                    (unless, void, when)
import           Data.Functor                     ((<&>))
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Database.MySQL.Base              (ERRException)
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB
import           Z.IO.Time                        (SystemTime (MkSystemTime),
                                                   getSystemTime')

import qualified Data.HashMap.Strict              as HM
import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import qualified HStream.SQL.Codegen              as CodeGen
import           HStream.Server.Exception         (ExceptionHandle, Handlers,
                                                   StreamNotExist (..),
                                                   defaultHandlers,
                                                   mkExceptionHandle,
                                                   mkStatusDetails, setRespType)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (handleCreateSinkConnector)
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (TaskStatus (..),
                                                   cBytesToText,
                                                   mkServerErrResp, returnResp,
                                                   textToCBytes)

createSinkConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createSinkConnectorHandler sc
  (ServerNormalRequest _ CreateSinkConnectorRequest{..}) = connectorExceptionHandle $ do
    Log.debug "Receive Create Sink Connector Request"
    connector <- createConnector sc createSinkConnectorRequestSql
    returnResp connector

listConnectorsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorsRequest ListConnectorsResponse
  -> IO (ServerResponse 'Normal ListConnectorsResponse)
listConnectorsHandler ServerContext{..}
  (ServerNormalRequest _metadata _) = connectorExceptionHandle $ do
  Log.debug "Receive List Connector Request"
  connectors <- P.getConnectors zkHandle
  returnResp $ ListConnectorsResponse .
    V.fromList . map hstreamConnectorToConnector $ connectors

getConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
getConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata GetConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Get Connector Request. "
    <> "Connector ID: " <> Log.buildString (T.unpack getConnectorRequestId)
  connector <- P.getConnector (textToCBytes getConnectorRequestId) zkHandle
  returnResp $ hstreamConnectorToConnector connector

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Delete Connector Request. "
    <> "Connector ID: " <> Log.buildText deleteConnectorRequestId
  let cName = textToCBytes deleteConnectorRequestId
  P.removeConnector cName zkHandle
  returnResp Empty

restartConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata RestartConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Restart Connector Request. "
    <> "Connector ID: " <> Log.buildText restartConnectorRequestId
  let cid = textToCBytes restartConnectorRequestId
  cStatus <- P.getConnectorStatus cid zkHandle
  when (cStatus `elem` [Created, Creating, Running]) $ do
    Log.warning . Log.buildString $ "The connector " <> show cid
      <> "cannot be restarted because it has state " <> show cStatus
    throwIO (ConnectorRestartErr cStatus)
  restartConnector sc cid >> returnResp Empty

terminateConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal TerminateConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
terminateConnectorHandler sc
  (ServerNormalRequest _metadata TerminateConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Terminate Connector Request. "
    <> "Connector ID: " <> Log.buildText terminateConnectorRequestConnectorId
  let cid = textToCBytes terminateConnectorRequestConnectorId
  terminateConnector sc cid
  returnResp Empty

--------------------------------------------------------------------------------

hstreamConnectorToConnector :: P.PersistentConnector -> Connector
hstreamConnectorToConnector P.PersistentConnector{..} =
  Connector (cBytesToText connectorId)
    (getPBStatus connectorStatus) connectorCreatedTime
    connectorBindedSql

createConnector :: ServerContext -> T.Text -> IO Connector
createConnector sc@ServerContext{..} sql = do
  (CodeGen.CreateSinkConnectorPlan cName ifNotExist sName cConfig _) <- CodeGen.streamCodegen sql
  Log.debug $ "CreateConnector CodeGen"
           <> ", connector name: " <> Log.buildText cName
           <> ", stream name: "    <> Log.buildText sName
           <> ", config: "         <> Log.buildString (show cConfig)
  streamExists <- S.doesStreamExist scLDClient (transToStreamName sName)
  connectorIds <- P.getConnectorIds zkHandle
  let cid = CB.pack $ T.unpack cName
      connectorExists = cid `elem` connectorIds
  unless streamExists $ throwIO StreamNotExist
  when (connectorExists && not ifNotExist) $ do
    cStatus <- P.getConnectorStatus cid zkHandle
    throwIO (ConnectorAlreadyExists cStatus)
  if connectorExists then do
    connector <- P.getConnector cid zkHandle
    return $ hstreamConnectorToConnector connector
  else do
    MkSystemTime timestamp _ <- getSystemTime'
    P.insertConnector cid sql timestamp serverID zkHandle
    handleCreateSinkConnector sc cid sName cConfig <&> hstreamConnectorToConnector

restartConnector :: ServerContext -> CB.CBytes -> IO ()
restartConnector sc@ServerContext{..} cid = do
  P.PersistentConnector _ sql _ _ _ _ <- P.getConnector cid zkHandle
  (CodeGen.CreateSinkConnectorPlan _ _ sName cConfig _)
    <- CodeGen.streamCodegen sql
  streamExists <- S.doesStreamExist scLDClient (transToStreamName sName)
  unless streamExists $ throwIO StreamNotExist
  void $ handleCreateSinkConnector sc cid sName cConfig

terminateConnector :: ServerContext -> CB.CBytes -> IO ()
terminateConnector ServerContext{..} cid = do
  hmapC <- readMVar runningConnectors
  case HM.lookup cid hmapC of
    Just tid -> do
      void $ killThread tid
      Log.debug . Log.buildString $ "TERMINATE: terminated connector: " <> show cid
    _        -> throwIO ConnectorTerminatedOrNotExist
--------------------------------------------------------------------------------
-- Exception and Exception Handlers

newtype ConnectorAlreadyExists = ConnectorAlreadyExists TaskStatus
  deriving (Show)
instance Exception ConnectorAlreadyExists

newtype ConnectorRestartErr = ConnectorRestartErr TaskStatus
  deriving (Show)
instance Exception ConnectorRestartErr

data ConnectorTerminatedOrNotExist = ConnectorTerminatedOrNotExist
  deriving (Show)
instance Exception ConnectorTerminatedOrNotExist

connectorExceptionHandlers :: Handlers (StatusCode, StatusDetails)
connectorExceptionHandlers =[
  Handler (\(err :: ConnectorAlreadyExists) -> do
    Log.fatal $ Log.buildString' err
    return (StatusAlreadyExists, mkStatusDetails err)),
  Handler (\(err :: ConnectorRestartErr) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, mkStatusDetails err)),
  Handler (\(err :: ConnectorTerminatedOrNotExist) -> do
    Log.fatal $ Log.buildString' err
    return (StatusNotFound, "Connector can not be terminated: No running connector with the same id")),
  Handler (\(err :: ERRException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, "Mysql error " <> mkStatusDetails err))]

connectorExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
connectorExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  connectorExceptionHandlers ++ defaultHandlers
