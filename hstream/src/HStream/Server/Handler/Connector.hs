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
import qualified HStream.IO.Types                 as IO
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (ExceptionHandle, Handlers,
                                                   StreamNotExist (..),
                                                   defaultHandlers,
                                                   mkExceptionHandle,
                                                   mkStatusDetails, setRespType)
import           HStream.Server.Handler.Common    (handleCreateSinkConnector)
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import qualified HStream.SQL.Codegen              as CodeGen
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (TaskStatus (..),
                                                   cBytesToText,
                                                   mkServerErrResp, returnResp,
                                                   textToCBytes)

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

startConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal StartConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
startConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata StartConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Start Connector Request. "
    <> "Connector ID: " <> Log.buildText startConnectorRequestName
  IO.startIOTask scIOWorker startConnectorRequestName
  returnResp Empty

stopConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal StopConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
stopConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata StopConnectorRequest{..}) = connectorExceptionHandle $ do
  Log.debug $ "Receive Terminate Connector Request. "
    <> "Connector ID: " <> Log.buildText stopConnectorRequestName
  IO.stopIOTask scIOWorker stopConnectorRequestName False
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
    return (StatusNotFound, "Connector can not be terminated: No running connector with the same id")),
  Handler (\(err :: ERRException) -> do
    Log.fatal $ Log.buildString' err
    return (StatusInternal, "Mysql error " <> mkStatusDetails err))]

connectorExceptionHandle :: ExceptionHandle (ServerResponse 'Normal a)
connectorExceptionHandle = mkExceptionHandle . setRespType mkServerErrResp $
  connectorExceptionHandlers ++ defaultHandlers
