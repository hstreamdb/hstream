{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE LambdaCase          #-}
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

  , createIOTaskFromSql
  ) where

import           Control.Exception                (throwIO)
import           Control.Monad                    (unless)
import qualified Data.Aeson                       as A
import qualified Data.Text                        as T
import qualified Data.UUID                        as UUID
import qualified Data.UUID.V4                     as UUID
import qualified Data.Vector                      as V
import qualified HsGrpc.Server                    as G
import           Network.GRPC.HighLevel.Generated

import qualified HStream.Exception                as HE
import qualified HStream.IO.Types                 as IO
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Common       (lookupResource')
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
#ifdef HStreamUseV2Engine
import qualified HStream.SQL.Codegen              as CG
#else
import qualified HStream.SQL.Codegen.V1           as CG
#endif
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (ResourceType (..),
                                                   returnResp,
                                                   validateNameAndThrow)

createConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createConnectorHandler sc
  (ServerNormalRequest _ CreateConnectorRequest{..}) = defaultExceptionHandle $ do
    Log.debug "Receive Create Sink Connector Request"
    createIOTaskFromSql sc createConnectorRequestSql >>= returnResp

handleCreateConnector :: ServerContext -> G.UnaryHandler CreateConnectorRequest Connector
handleCreateConnector sc _ CreateConnectorRequest{..} = catchDefaultEx $
  createIOTaskFromSql sc createConnectorRequestSql

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
    Nothing -> throwIO $ HE.ConnectorNotFound getConnectorRequestName
    Just c  -> returnResp c

handleGetConnector :: ServerContext -> G.UnaryHandler GetConnectorRequest Connector
handleGetConnector ServerContext{..} _ GetConnectorRequest{..} = catchDefaultEx $ do
  IO.showIOTask scIOWorker getConnectorRequestName >>= \case
    Nothing -> throwIO $ HE.ConnectorNotFound getConnectorRequestName
    Just c  -> pure c

handleGetConnectorSpec :: ServerContext -> G.UnaryHandler GetConnectorRequest GetConnectorSpecResponse
handleGetConnectorSpec ServerContext{..} _ GetConnectorSpecRequest{..} = catchDefaultEx $ do
  IO.getSpec GetConnectorSpecRequestType GetConnectorSpecRequestTarget

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete Connector Request. "
    <> "Connector Name: " <> Log.build deleteConnectorRequestName
  ServerNode{..} <- lookupResource' sc ResConnector deleteConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.deleteIOTask scIOWorker deleteConnectorRequestName
  returnResp Empty

handleDeleteConnector :: ServerContext -> G.UnaryHandler DeleteConnectorRequest Empty
handleDeleteConnector sc@ServerContext{..} _ DeleteConnectorRequest{..} = catchDefaultEx $ do
  ServerNode{..} <- lookupResource' sc ResConnector deleteConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.deleteIOTask scIOWorker deleteConnectorRequestName >> pure Empty

resumeConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal ResumeConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
resumeConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata ResumeConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive ResumeConnectorRequest. "
    <> "Connector Name: " <> Log.build resumeConnectorRequestName
  ServerNode{..} <- lookupResource' sc ResConnector resumeConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.startIOTask scIOWorker resumeConnectorRequestName
  returnResp Empty

handleResumeConnector :: ServerContext -> G.UnaryHandler ResumeConnectorRequest Empty
handleResumeConnector sc@ServerContext{..} _ ResumeConnectorRequest{..} = catchDefaultEx $ do
  ServerNode{..} <- lookupResource' sc ResConnector resumeConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.startIOTask scIOWorker resumeConnectorRequestName >> pure Empty

pauseConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal PauseConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
pauseConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata PauseConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Terminate Connector Request. "
    <> "Connector ID: " <> Log.build pauseConnectorRequestName
  ServerNode{..} <- lookupResource' sc ResConnector pauseConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.stopIOTask scIOWorker pauseConnectorRequestName False False
  returnResp Empty

handlePauseConnector :: ServerContext -> G.UnaryHandler PauseConnectorRequest Empty
handlePauseConnector sc@ServerContext{..} _ PauseConnectorRequest{..} = catchDefaultEx $ do
  ServerNode{..} <- lookupResource' sc ResConnector pauseConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.stopIOTask scIOWorker pauseConnectorRequestName False False >> pure Empty

createIOTaskFromSql :: ServerContext -> T.Text -> IO Connector
createIOTaskFromSql sc@ServerContext{scIOWorker = worker@IO.Worker{..}, ..} sql = do
  (CG.CreateConnectorPlan cType cName cTarget ifNotExist cfg) <- CG.streamCodegen sql
  validateNameAndThrow cName
  ServerNode{..} <- lookupResource' sc ResConnector cName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  Log.info $ "CreateConnector CodeGen"
           <> ", connector type: " <> Log.build cType
           <> ", connector name: " <> Log.build cName
           <> ", config: "         <> Log.buildString (show cfg)
  taskId <- UUID.toText <$> UUID.nextRandom
  let IO.IOOptions {..} = options
      taskType = if cType == "SOURCE" then IO.SOURCE else IO.SINK
      image = IO.makeImage taskType cTarget options
      connectorConfig =
        A.object
          [ "hstream" A..= IO.toTaskJson hsConfig taskId
          , "connector" A..= cfg
          ]
      taskInfo = IO.TaskInfo
        { taskName = cName
        , taskType = if cType == "SOURCE" then IO.SOURCE else IO.SINK
        , taskConfig = IO.TaskConfig image optTasksNetwork
        , connectorConfig = connectorConfig
        , originSql = sql
        }
  IO.createIOTask worker taskId taskInfo
  return $ IO.mkConnector cName (IO.ioTaskStatusToText IO.NEW)
