{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE CPP                 #-}
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
  , getConnectorSpecHandler
  , getConnectorLogsHandler
  , deleteConnectorHandler
  , resumeConnectorHandler
  , pauseConnectorHandler
    -- * For hs-grpc-server
  , handleCreateConnector
  , handleListConnectors
  , handleGetConnector
  , handleGetConnectorSpec
  , handleGetConnectorLogs
  , handleDeleteConnector
  , handleResumeConnector
  , handlePauseConnector
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

import qualified Data.Aeson.KeyMap                as A
import qualified Data.Text.Encoding               as T
import qualified HStream.Exception                as HE
import qualified HStream.IO.Types                 as IO
import qualified HStream.IO.Worker                as IO
import qualified HStream.Logger                   as Log
import           HStream.Server.Core.Common       (lookupResource')
import           HStream.Server.Exception         (catchDefaultEx,
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Types
import           HStream.Server.Validation        (validateCreateConnector)
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (ResourceType (..),
                                                   returnResp,
                                                   validateNameAndThrow)
import qualified HStream.Utils                    as Utils

createConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createConnectorHandler sc (ServerNormalRequest _ req) = defaultExceptionHandle $ do
    Log.debug "Receive Create Connector Request"
    validateCreateConnector req
    createIOTaskFromRequest sc req >>= returnResp

handleCreateConnector :: ServerContext -> G.UnaryHandler CreateConnectorRequest Connector
handleCreateConnector sc _ req = catchDefaultEx $ do
  validateCreateConnector req
  createIOTaskFromRequest sc req

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
  validateNameAndThrow ResConnector getConnectorRequestName
  IO.showIOTask_ scIOWorker getConnectorRequestName >>= returnResp

handleGetConnector :: ServerContext -> G.UnaryHandler GetConnectorRequest Connector
handleGetConnector ServerContext{..} _ GetConnectorRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Get Connector Request. "
    <> "Connector Name: " <> Log.buildString (T.unpack getConnectorRequestName)
  validateNameAndThrow ResConnector getConnectorRequestName
  IO.showIOTask_ scIOWorker getConnectorRequestName

getConnectorSpecHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorSpecRequest GetConnectorSpecResponse
  -> IO (ServerResponse 'Normal GetConnectorSpecResponse)
getConnectorSpecHandler ServerContext{..}
  (ServerNormalRequest _metadata GetConnectorSpecRequest{..}) = defaultExceptionHandle $ do
    spec <- IO.getSpec scIOWorker getConnectorSpecRequestType getConnectorSpecRequestTarget
    returnResp $ GetConnectorSpecResponse spec

handleGetConnectorSpec :: ServerContext -> G.UnaryHandler GetConnectorSpecRequest GetConnectorSpecResponse
handleGetConnectorSpec ServerContext{..} _ GetConnectorSpecRequest{..} = catchDefaultEx $ do
  GetConnectorSpecResponse <$> IO.getSpec scIOWorker getConnectorSpecRequestType getConnectorSpecRequestTarget

getConnectorLogsHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorLogsRequest GetConnectorLogsResponse
  -> IO (ServerResponse 'Normal GetConnectorLogsResponse)
getConnectorLogsHandler ServerContext{..}
  (ServerNormalRequest _metadata GetConnectorLogsRequest{..}) = defaultExceptionHandle $ do
    logs <-IO.getTaskLogs scIOWorker
      getConnectorLogsRequestName
      getConnectorLogsRequestBegin
      getConnectorLogsRequestCount
    returnResp $ GetConnectorLogsResponse logs

handleGetConnectorLogs :: ServerContext -> G.UnaryHandler GetConnectorLogsRequest GetConnectorLogsResponse
handleGetConnectorLogs ServerContext{..} _ GetConnectorLogsRequest{..} = catchDefaultEx $ do
  GetConnectorLogsResponse <$> IO.getTaskLogs scIOWorker
    getConnectorLogsRequestName
    getConnectorLogsRequestBegin
    getConnectorLogsRequestCount

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = defaultExceptionHandle $ do
  Log.debug $ "Receive Delete Connector Request. "
    <> "Connector Name: " <> Log.build deleteConnectorRequestName
  validateNameAndThrow ResConnector deleteConnectorRequestName
  ServerNode{..} <- lookupResource' sc ResConnector deleteConnectorRequestName
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  IO.deleteIOTask scIOWorker deleteConnectorRequestName
  returnResp Empty

handleDeleteConnector :: ServerContext -> G.UnaryHandler DeleteConnectorRequest Empty
handleDeleteConnector sc@ServerContext{..} _ DeleteConnectorRequest{..} = catchDefaultEx $ do
  Log.debug $ "Receive Delete Connector Request. "
    <> "Connector Name: " <> Log.build deleteConnectorRequestName
  validateNameAndThrow ResConnector deleteConnectorRequestName
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

-- uncurry
createIOTaskFromRequest :: ServerContext -> CreateConnectorRequest -> IO Connector
createIOTaskFromRequest sc CreateConnectorRequest{..} = do
  createIOTask sc createConnectorRequestName createConnectorRequestType createConnectorRequestTarget  createConnectorRequestConfig

createIOTask :: ServerContext -> T.Text -> T.Text -> T.Text -> T.Text -> IO Connector
createIOTask sc@ServerContext{scIOWorker = worker@IO.Worker{..}, ..} name typ target cfg = do
  -- FIXME: Can we remove this validation ?
  validateNameAndThrow ResConnector name
  ServerNode{..} <- lookupResource' sc ResConnector name
  unless (serverNodeId == serverID) $
    throwIO $ HE.WrongServer "Connector is bound to a different node"
  Log.info $ "CreateConnector CodeGen"
           <> ", connector type: " <> Log.build typ
           <> ", connector name: " <> Log.build name
           <> ", connector targe: " <> Log.build target
           <> ", config: "         <> Log.build cfg
  taskId <- UUID.toText <$> UUID.nextRandom
  createdTime <- Utils.getProtoTimestamp
  let IO.IOOptions {..} = options
      taskType = IO.ioTaskTypeFromText typ
      image = IO.makeImage taskType target options
      connectorConfig =
        A.fromList
          [ "hstream" A..= A.toJSON hsConfig
          , "connector" A..= (A.decodeStrict $ T.encodeUtf8 cfg :: Maybe A.Object)
          , "task" A..= taskId
          ]
      taskInfo = IO.TaskInfo
        { taskName = name
        , taskType = taskType
        , taskTarget = target
        , taskCreatedTime = createdTime
        , taskConfig = IO.TaskConfig image optTasksNetwork
        , connectorConfig = connectorConfig
        }
  IO.createIOTask worker taskId taskInfo False True
  IO.showIOTask_ worker name
