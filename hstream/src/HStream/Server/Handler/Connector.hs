{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Connector
  ( -- * For grpc-haskell
    createConnectorHandler
  , listConnectorsHandler
  , getConnectorHandler
  , deleteConnectorHandler
  , resumeConnectorHandler
  , pauseConnectorHandler
    -- * For hs-grpc-server
  , handleCreateConnector
  , handleListConnectors
  , handleGetConnector
  , handleDeleteConnector
  , handleResumeConnector
  , handlePauseConnector
  ) where

import           Control.Exception                (throwIO)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (returnResp)

createConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createConnectorHandler sc
  (ServerNormalRequest _ CreateConnectorRequest{..}) = defaultExceptionHandle $ do
    Log.debug "Receive Create Sink Connector Request"
    IO.createIOTaskFromSql (scIOWorker sc) createConnectorRequestSql >>= returnResp

handleCreateConnector :: ServerContext -> G.UnaryHandler CreateConnectorRequest Connector
handleCreateConnector sc _ CreateConnectorRequest{..} = catchDefaultEx $
  IO.createIOTaskFromSql (scIOWorker sc) createConnectorRequestSql

listConnectorsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorsRequest ListConnectorsResponse
  -> IO (ServerResponse 'Normal ListConnectorsResponse)
listConnectorsHandler ServerContext{..}
  (ServerNormalRequest _metadata _) = defaultExceptionHandle $ do
  Log.debug "Receive List Connector Request"
  cs <- IO.listIOTasks scIOWorker
  returnResp . ListConnectorsResponse . V.fromList $ cs

handleListConnectors :: ServerContext -> G.UnaryHandler ListConnectorsRequest ListConnectorsResponse
handleListConnectors ServerContext{..} _ _ = catchDefaultEx $
  ListConnectorsResponse . V.fromList <$> IO.listIOTasks scIOWorker

getConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
getConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata GetConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Get Connector Request. "
    <> "Connector Name: " <> Log.buildString (T.unpack getConnectorRequestName)
  IO.showIOTask scIOWorker getConnectorRequestName >>= \case
    Nothing -> throwIO $ HE.ConnectorNotFound "ConnectorNotFound"
    Just c  -> returnResp c

handleGetConnector :: ServerContext -> G.UnaryHandler GetConnectorRequest Connector
handleGetConnector ServerContext{..} _ GetConnectorRequest{..} = catchDefaultEx $ do
  IO.showIOTask scIOWorker getConnectorRequestName >>= \case
    Nothing -> throwIO $ HE.ConnectorNotFound "ConnectorNotFound"
    Just c  -> pure c

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete Connector Request. "
    <> "Connector Name: " <> Log.buildText deleteConnectorRequestName
  IO.deleteIOTask scIOWorker deleteConnectorRequestName
  returnResp Empty

handleDeleteConnector :: ServerContext -> G.UnaryHandler DeleteConnectorRequest Empty
handleDeleteConnector ServerContext{..} _ DeleteConnectorRequest{..} = catchDefaultEx $
    IO.deleteIOTask scIOWorker deleteConnectorRequestName >> pure Empty

resumeConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal ResumeConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
resumeConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata ResumeConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive ResumeConnectorRequest. "
    <> "Connector Name: " <> Log.buildText resumeConnectorRequestName
  IO.startIOTask scIOWorker resumeConnectorRequestName
  returnResp Empty

handleResumeConnector :: ServerContext -> G.UnaryHandler ResumeConnectorRequest Empty
handleResumeConnector ServerContext{..} _ ResumeConnectorRequest{..} = catchDefaultEx $
  IO.startIOTask scIOWorker resumeConnectorRequestName >> pure Empty

pauseConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal PauseConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
pauseConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata PauseConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Terminate Connector Request. "
    <> "Connector ID: " <> Log.buildText pauseConnectorRequestName
  IO.stopIOTask scIOWorker pauseConnectorRequestName False False
  returnResp Empty

handlePauseConnector :: ServerContext -> G.UnaryHandler PauseConnectorRequest Empty
handlePauseConnector ServerContext{..} _ PauseConnectorRequest{..} = catchDefaultEx $
  IO.stopIOTask scIOWorker pauseConnectorRequestName False False >> pure Empty
