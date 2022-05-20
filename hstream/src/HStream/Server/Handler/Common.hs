{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent
import           Data.Hashable           (Hashable)
import           Control.Exception                (Handler (Handler),
                                                   SomeException (..), catches,
                                                   onException)
import           Control.Exception.Base           (AsyncException (..))
import           Control.Monad                    (forever, void, when, mapM, forM)
import           Data.Foldable                    (foldrM)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import           Database.ClickHouseDriver.Client (createClient)
import           Database.MySQL.Base              (ERRException)
import qualified Database.MySQL.Base              as MySQL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)
import qualified Z.Data.CBytes                    as CB
import           Z.Data.CBytes                    (CBytes)

import           HStream.Connector.ClickHouse
import           HStream.Connector.MySQL
import qualified HStream.Logger                   as Log
import           HStream.Processing.Connector
import           HStream.Processing.Processor     (TaskBuilder, getTaskName,
                                                   runTask, runTask')
import           HStream.Processing.Type          (Offset (..), SinkRecord (..),
                                                   SourceRecord (..), Timestamp)
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import           HStream.Server.Config
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import           HStream.SQL.Codegen
import qualified HStream.Store                    as HS
import           HStream.Utils                    (TaskStatus (..),
                                                   cBytesToText,
                                                   clientDefaultKey,
                                                   newRandomText, runWithAddr,
                                                   textToCBytes)

import Types
import Shard
import Graph

--------------------------------------------------------------------------------

runTaskWrapper :: Text -> [(Node, Text)] -> (Node, Text) -> HS.StreamType -> HS.StreamType -> GraphBuilder -> Maybe (MVar (DataChangeBatch HStream.Processing.Type.Timestamp)) -> IO ()
runTaskWrapper taskName inNodesWithStreams outNodeWithStream sourceType sinkType graphBuilder accumulation = do
  runWithAddr (ZNet.ipv4 "127.0.0.1" (ZNet.PortNumber $ _serverPort serverOpts)) $ \api -> do
    let consumerName = textToCBytes (getTaskName taskBuilder)

    sourceConnectors <- forM inNodesWithStreams
      (\(inNode,_) -> do
        -- create a new sourceConnector
        return $ HCS.hstoreSourceConnectorWithoutCkp api (cBytesToText consumerName)
      )

    -- create a new sinkConnector
    let sinkConnector = HCS.hstoreSinkConnector api

    -- build graph then shard
    let graph = buildGraph graphBuilder
    shard <- buildShard graph

    -- RUN TASK
    runTask' inNodesWithStreams outNodeWithStream sourceConnectors sinkConnector accumulation shard

runSinkConnector
  :: ServerContext
  -> CB.CBytes -- ^ Connector Id
  -> SourceConnectorWithoutCkp
  -> Text -- ^ source stream name
  -> SinkConnector
  -> IO ()
runSinkConnector ServerContext{..} cid src streamName connector = do
    P.setConnectorStatus cid Running zkHandle
    catches (forever action) cleanup
  where
    writeToConnector c SourceRecord{..} =
      writeRecord c $ SinkRecord srcStream srcKey srcValue srcTimestamp
    action = withReadRecordsWithoutCkp src streamName $ \sourceRecords -> do
      mapM_ (writeToConnector connector) sourceRecords
    cleanup =
      [ Handler (\(_ :: ERRException) -> do
                    Log.warning "Sink connector thread died due to SQL errors"
                    P.setConnectorStatus cid ConnectionAbort zkHandle
                    void releasePid)
      , Handler (\(e :: AsyncException) -> do
                    Log.debug . Log.buildString $ "Sink connector thread killed because of " <> show e
                    P.setConnectorStatus cid Terminated zkHandle
                    void releasePid)
      ]
    releasePid = do
      hmapC <- readMVar runningConnectors
      swapMVar runningConnectors $ HM.delete cid hmapC

handlePushQueryCanceled :: ServerCall () -> IO () -> IO ()
handlePushQueryCanceled ServerCall{..} handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left err   -> print err
    Right []   -> putStrLn "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b]
      -> when b handle
    _ -> putStrLn "impossible happened"

responseWithErrorMsgIfNothing :: Maybe a -> StatusCode -> StatusDetails -> IO (ServerResponse 'Normal a)
responseWithErrorMsgIfNothing (Just resp) _ _ = return $ ServerNormalResponse (Just resp) mempty StatusOk ""
responseWithErrorMsgIfNothing Nothing errCode msg = return $ ServerNormalResponse Nothing mempty errCode msg

--------------------------------------------------------------------------------
-- GRPC Handler Helper

handleCreateSinkConnector
  :: ServerContext
  -> CB.CBytes -- ^ Connector Name
  -> Text -- ^ Source Stream Name
  -> ConnectorConfig
  -> IO P.PersistentConnector
handleCreateSinkConnector serverCtx@ServerContext{..} cid sName cConfig = do
  onException action cleanup
  where
    cleanup = do
      Log.debug "Create sink connector failed"
      P.setConnectorStatus cid CreationAbort zkHandle

    action = do
      P.setConnectorStatus cid Creating zkHandle
      Log.debug "Start creating sink connector"

      connector <- case cConfig of
        ClickhouseConnector config  -> do
          Log.debug $ "Connecting to clickhouse with " <> Log.buildString (show config)
          clickHouseSinkConnector  <$> createClient config
        MySqlConnector table config -> do
          Log.debug $ "Connecting to mysql with " <> Log.buildString (show config)
          mysqlSinkConnector table <$> MySQL.connect config
      P.setConnectorStatus cid Created zkHandle
      Log.debug . Log.buildString . CB.unpack $ cid <> "Connected"

      consumerName <- newRandomText 20
      let src = HStore.hstoreSourceConnectorWithoutCkp serverCtx consumerName
      subscribeToStreamWithoutCkp src sName SpecialOffsetLATEST
      tid <- forkIO $ runSinkConnector serverCtx cid src sName connector

      Log.debug . Log.buildString $ "Sink connector started running on thread#" <> show tid
      modifyMVar_ runningConnectors (return . HM.insert cid tid)
      P.getConnector cid zkHandle

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext
                     -> HStreamPlan
                     -> Text
                     -> P.QueryType
                     -> HS.StreamType
                     -> IO (CB.CBytes, Int64)
handleCreateAsSelect ServerContext{..} plan commandQueryStmtText queryType sinkType = do
  (qid, timestamp) <- P.createInsertPersistentQuery
    tName commandQueryStmtText queryType serverID zkHandle
  P.setQueryStatus qid Running zkHandle
  tid <- forkIO $ catches (action qid) (cleanup qid)
  takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
  return (qid, timestamp)
  where
    (tName,inNodesWithStreams,outNodeWithStream,builder,accumulation) = case plan of
        SelectPlan tName inNodesWithStreams outNodeWithStream builder ->
          (tName,inNodesWithStreams,outNodeWithStream,builder, Nothing)
        CreateBySelectPlan tName inNodesWithStreams outNodeWithStream builder _ ->
          (tName,inNodesWithStreams,outNodeWithStream,builder, Nothing)
        CreateViewPlan tName schema inNodesWithStreams outNodeWithStream builder accumulation ->
          (tName,inNodesWithStreams,outNodeWithStream,builder, Just accumulation)
        _ -> undefined
    action qid = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTaskWrapper tName inNodesWithStreams outNodeWithStream HS.StreamTypeStream sinkType builder accumulation
    cleanup qid =
      [ Handler (\(e :: AsyncException) -> do
                    Log.debug . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " is killed because of " <> show e
                    P.setQueryStatus qid Terminated zkHandle
                    void $ releasePid qid)
      , Handler (\(e :: SomeException) -> do
                    Log.warning . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " died because of " <> show e
                    P.setQueryStatus qid ConnectionAbort zkHandle
                    void $ releasePid qid)
      ]
    releasePid qid = do
      hmapC <- readMVar runningQueries
      swapMVar runningQueries $ HM.delete qid hmapC

--------------------------------------------------------------------------------

alignDefault :: Text -> Text
alignDefault x  = if T.null x then clientDefaultKey else x

orderingKeyToStoreKey :: Text -> Maybe CBytes
orderingKeyToStoreKey key
  | key == clientDefaultKey = Nothing
  | T.null key = Nothing
  | otherwise  = Just $ textToCBytes key
