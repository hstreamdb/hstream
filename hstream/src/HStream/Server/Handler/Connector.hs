{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Connector where

import           Control.Exception                (Exception, Handler (..),
                                                   throwIO)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (ExceptionHandle, Handlers,
                                                   defaultHandlers,
                                                   mkExceptionHandle,
                                                   mkStatusDetails, setRespType)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (TaskStatus (..),
                                                   mkServerErrResp, returnResp)
import           Network.GRPC.HighLevel.Generated

createConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createConnectorHandler sc
  (ServerNormalRequest _ CreateConnectorRequest{..}) = connectorExceptionHandle $ do
    Log.debug "Receive Create Sink Connector Request"
    IO.createIOTaskFromSql (scIOWorker sc) createConnectorRequestSql >>= returnResp

listConnectorsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorsRequest ListConnectorsResponse
  -> IO (ServerResponse 'Normal ListConnectorsResponse)
listConnectorsHandler ServerContext{..}
  (ServerNormalRequest _metadata _) = connectorExceptionHandle $ do
  Log.debug "Receive List Connector Request"
  cs <- IO.listIOTasks scIOWorker
  returnResp . ListConnectorsResponse . V.fromList $ cs

getConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
getConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata GetConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Get Connector Request. "
    <> "Connector Name: " <> Log.buildString (T.unpack getConnectorRequestName)
  IO.showIOTask scIOWorker getConnectorRequestName >>= \case
    Nothing -> throwIO ConnectorNotFound
    Just c  -> returnResp c

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Delete Connector Request. "
    <> "Connector Name: " <> Log.buildText deleteConnectorRequestName
  IO.deleteIOTask scIOWorker deleteConnectorRequestName
  returnResp Empty

resumeConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal ResumeConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
resumeConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata ResumeConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive ResumeConnectorRequest. "
    <> "Connector Name: " <> Log.buildText resumeConnectorRequestName
  IO.startIOTask scIOWorker resumeConnectorRequestName
  returnResp Empty

pauseConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal PauseConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
pauseConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata PauseConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Terminate Connector Request. "
    <> "Connector ID: " <> Log.buildText pauseConnectorRequestName
  IO.stopIOTask scIOWorker pauseConnectorRequestName False False
  returnResp Empty

--------------------------------------------------------------------------------
-- Exception and Exception Handlers

newtype ConnectorAlreadyExists = ConnectorAlreadyExists TaskStatus
  deriving (Show)
instance Exception ConnectorAlreadyExists

newtype ConnectorRestartErr = ConnectorRestartErr TaskStatus
  deriving (Show)
instance Exception ConnectorRestartErr

data ConnectorNotFound = ConnectorNotFound
  deriving (Show)
instance Exception ConnectorNotFound

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
    return (StatusNotFound, "Connector can not be terminated: No running connector with the same id"))
  ]

connectorExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
connectorExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  connectorExceptionHandlers ++ defaultHandlers
