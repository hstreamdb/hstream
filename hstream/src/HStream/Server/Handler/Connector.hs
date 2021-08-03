{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Connector where

import           Control.Exception                (throwIO)
import           Control.Monad                    (unless, when)
import           Data.Functor                     ((<&>))
import qualified Data.Text                        as T
import qualified Data.Text.Lazy                   as TL
import qualified Data.Vector                      as V
import           Network.GRPC.HighLevel.Generated
import qualified Z.Data.CBytes                    as CB
import qualified Z.Data.Text                      as ZT

import           HStream.Connector.HStore         (transToStreamName)
import qualified HStream.Logger                   as Log
import qualified HStream.SQL.Codegen              as CodeGen
import           HStream.Server.Exception         (ConnectorAlreadyExists (..),
                                                   StreamNotExist (..),
                                                   defaultExceptionHandle)
import           HStream.Server.HStreamApi
import           HStream.Server.Handler.Common    (ServerContext (..),
                                                   handleCreateSinkConnector,
                                                   handleTerminateConnector,
                                                   runSinkConnector)
import qualified HStream.Server.Persistence       as P
import qualified HStream.Store                    as S
import           HStream.ThirdParty.Protobuf      (Empty (..))
import           HStream.Utils                    (cBytesToLazyText,
                                                   lazyTextToCBytes, returnResp)

createSinkConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CreateSinkConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
createSinkConnectorHandler sc
  (ServerNormalRequest _ CreateSinkConnectorRequest{..}) = defaultExceptionHandle $ do
    connector <- createConnector sc (TL.toStrict createSinkConnectorRequestSql)
    returnResp connector

listConnectorsHandler
  :: ServerContext
  -> ServerRequest 'Normal ListConnectorsRequest ListConnectorsResponse
  -> IO (ServerResponse 'Normal ListConnectorsResponse)
listConnectorsHandler ServerContext{..}
  (ServerNormalRequest _metadata _) = defaultExceptionHandle $ do
  connectors <- P.withMaybeZHandle zkHandle P.getConnectors
  returnResp $ ListConnectorsResponse .
    V.fromList . map hstreamConnectorToConnector $ connectors

getConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal GetConnectorRequest Connector
  -> IO (ServerResponse 'Normal Connector)
getConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata GetConnectorRequest{..}) = defaultExceptionHandle $ do
  connector <- P.withMaybeZHandle zkHandle $
    P.getConnector (lazyTextToCBytes getConnectorRequestId)
  returnResp $ hstreamConnectorToConnector connector

deleteConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal DeleteConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
deleteConnectorHandler ServerContext{..}
  (ServerNormalRequest _metadata DeleteConnectorRequest{..}) = defaultExceptionHandle $ do
    P.withMaybeZHandle zkHandle $
      P.removeConnector (lazyTextToCBytes deleteConnectorRequestId)
    returnResp Empty

restartConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal RestartConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
restartConnectorHandler sc@ServerContext{..}
  (ServerNormalRequest _metadata RestartConnectorRequest{..}) = defaultExceptionHandle $ do
  let cid = lazyTextToCBytes restartConnectorRequestId
  cStatus <- P.withMaybeZHandle zkHandle $ P.getConnectorStatus cid
  when (cStatus == P.Terminated) $ restartConnector sc cid
  returnResp Empty

cancelConnectorHandler
  :: ServerContext
  -> ServerRequest 'Normal CancelConnectorRequest Empty
  -> IO (ServerResponse 'Normal Empty)
cancelConnectorHandler sc
  (ServerNormalRequest _metadata CancelConnectorRequest{..}) = do
  let cid = lazyTextToCBytes cancelConnectorRequestId
  handleTerminateConnector sc cid
  returnResp Empty

--------------------------------------------------------------------------------

hstreamConnectorToConnector :: P.PersistentConnector -> Connector
hstreamConnectorToConnector P.PersistentConnector{..} =
  Connector (cBytesToLazyText connectorId)
    (fromIntegral . fromEnum  $ connectorStatus)
    connectorCreatedTime
    (TL.pack . ZT.unpack $ connectorBindedSql)

createConnector :: ServerContext -> T.Text -> IO Connector
createConnector sc@ServerContext{..} sql = do
  (CodeGen.CreateSinkConnectorPlan cName ifNotExist sName cConfig _) <- CodeGen.streamCodegen sql
  Log.debug $ "CreateConnector CodeGen"
           <> ", connector name: " <> Log.buildText cName
           <> ", stream name: " <> Log.buildText sName
           <> ", config: " <> Log.buildString (show cConfig)
  streamExists <- S.doesStreamExists scLDClient (transToStreamName sName)
  connectorIds <- P.withMaybeZHandle zkHandle P.getConnectorIds
  let cid = T.unpack cName
      connectorExists = cid `elem` map CB.unpack connectorIds
  unless streamExists $ throwIO StreamNotExist
  when (connectorExists && not ifNotExist) $ throwIO ConnectorAlreadyExists
  if connectorExists then do
    connector <- P.withMaybeZHandle zkHandle $ P.getConnector (CB.pack cid)
    return $ hstreamConnectorToConnector connector
  else handleCreateSinkConnector sc sql cName sName cConfig <&> hstreamConnectorToConnector

restartConnector :: ServerContext -> CB.CBytes -> IO ()
restartConnector sc@ServerContext{..} cid = do
  P.PersistentConnector _ sql _ _ _
    <- P.withMaybeZHandle zkHandle $ P.getConnector cid
  (CodeGen.CreateSinkConnectorPlan _ _ sName cConfig _)
    <- CodeGen.streamCodegen (T.pack . ZT.unpack $ sql)
  runSinkConnector sc cid sName cConfig
