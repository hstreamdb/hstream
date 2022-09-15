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

import qualified HStream.Exception                as HE
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Exception         (defaultExceptionHandle,
                                                   defaultHandlers)
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
  (ServerNormalRequest _ CreateConnectorRequest{..}) = defaultExceptionHandle $ do
    Log.debug "Receive Create Sink Connector Request"
    IO.createIOTaskFromSql (scIOWorker sc) createConnectorRequestSql >>= returnResp

listConnectorsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorsRequest ListConnectorsResponse
  -> IO (ServerResponse 'Normal ListConnectorsResponse)
listConnectorsHandler ServerContext{..}
  (ServerNormalRequest _metadata _) = defaultExceptionHandle $ do
  Log.debug "Receive List Connector Request"
  cs <- IO.listIOTasks scIOWorker
  returnResp . ListConnectorsResponse . V.fromList $ cs

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

resumeConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal ResumeConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
resumeConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata ResumeConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive ResumeConnectorRequest. "
    <> "Connector Name: " <> Log.buildText resumeConnectorRequestName
  IO.startIOTask scIOWorker resumeConnectorRequestName
  returnResp Empty

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
