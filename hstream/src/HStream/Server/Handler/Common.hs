{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent
import           Control.Exception                (Handler (Handler),
                                                   SomeException (..), catches,
                                                   onException, try)
import           Control.Exception.Base           (AsyncException (..))
import           Control.Monad                    (forever, void, when)
import qualified Data.ByteString                  as BS
import           Data.Foldable                    (foldrM)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Vector                      as V
import           Data.Word                        (Word32, Word64)
import           Database.ClickHouseDriver.Client (createClient)
import           Database.MySQL.Base              (ERRException)
import qualified Database.MySQL.Base              as MySQL
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op         (Op (OpRecvCloseOnServer),
                                                   OpRecvResult (OpRecvCloseOnServerResult),
                                                   runOps)
import qualified Z.Data.CBytes                    as CB
import           Z.Data.CBytes                    (CBytes)
import qualified Z.IO.Network                     as ZNet

import           HStream.Connector.ClickHouse
import qualified HStream.Connector.HStore         as HCS
import           HStream.Connector.MySQL
import qualified HStream.Logger                   as Log
import           HStream.Processing.Connector
import           HStream.Processing.Processor     (TaskBuilder, getTaskName,
                                                   runTask)
import           HStream.Processing.Type          (Offset (..), SinkRecord (..),
                                                   SourceRecord (..))
import           HStream.Server.Config
import           HStream.Server.HStreamApi
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import           HStream.SQL.Codegen
import qualified HStream.Store                    as HS
import           HStream.Utils                    (TaskStatus (..),
                                                   cBytesToText,
                                                   clientDefaultKey,
                                                   decodeByteStringBatch,
                                                   newRandomText, runWithAddr,
                                                   textToCBytes)

--------------------------------------------------------------------------------

runTaskWrapper :: ServerContext -> TaskBuilder -> HS.LDClient -> IO ()
runTaskWrapper ServerContext{..} taskBuilder ldclient = do
  let consumerName = textToCBytes (getTaskName taskBuilder)

  runWithAddr (ZNet.ipv4 "127.0.0.1" (ZNet.PortNumber $ _serverPort serverOpts)) $ \api -> do
    -- create a new sourceConnector
    let sourceConnector = HCS.hstoreSourceConnectorWithoutCkp api (cBytesToText consumerName)
    -- create a new sinkConnector
    let sinkConnector = HCS.hstoreSinkConnector api
    -- RUN TASK
    runTask sourceConnector sinkConnector taskBuilder

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
    action = do
      datas <- (readRecordsWithoutCkp src) streamName
      mapM_ (writeToConnector connector) datas
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

      tid <- forkIO $ runWithAddr (ZNet.ipv4 "127.0.0.1" (ZNet.PortNumber $ _serverPort serverOpts)) $ \api -> do
        consumerName <- newRandomText 20
        let src = HCS.hstoreSourceConnectorWithoutCkp api consumerName
        subscribeToStreamWithoutCkp src sName

        runSinkConnector serverCtx cid src sName connector
      Log.debug . Log.buildString $ "Sink connector started running on thread#" <> show tid
      modifyMVar_ runningConnectors (return . HM.insert cid tid)
      P.getConnector cid zkHandle

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext
                     -> TaskBuilder
                     -> Text
                     -> P.QueryType
                     -> HS.StreamType
                     -> IO (CB.CBytes, Int64)
handleCreateAsSelect ctx@ServerContext{..} taskBuilder commandQueryStmtText queryType sinkType = do
  (qid, timestamp) <- P.createInsertPersistentQuery
    (getTaskName taskBuilder) commandQueryStmtText queryType serverID zkHandle
  P.setQueryStatus qid Running zkHandle
  tid <- forkIO $ catches (action qid) (cleanup qid)
  takeMVar runningQueries >>= putMVar runningQueries . HM.insert qid tid
  return (qid, timestamp)
  where
    action qid = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTaskWrapper ctx taskBuilder scLDClient
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
-- Query

terminateQueryAndRemove :: ServerContext -> CB.CBytes -> IO ()
terminateQueryAndRemove sc@ServerContext{..} objectId = do
  queries <- P.getQueries zkHandle
  let queryExists = find (\query -> P.getQuerySink query == objectId) queries
  case queryExists of
    Just query -> do
      Log.debug . Log.buildString
         $ "TERMINATE: found query " <> show (P.queryType query)
        <> " with query id " <> show (P.queryId query)
        <> " writes to the stream being dropped " <> show objectId
      void $ handleQueryTerminate sc (OneQuery $ P.queryId query)
      P.removeQuery' (P.queryId query) zkHandle
      Log.debug . Log.buildString
         $ "TERMINATE: query " <> show (P.queryType query)
        <> " has been removed"
    Nothing    -> do
      Log.debug . Log.buildString
        $ "TERMINATE: found no query writes to the stream being dropped " <> show objectId

terminateRelatedQueries :: ServerContext -> CB.CBytes -> IO ()
terminateRelatedQueries sc@ServerContext{..} name = do
  queries <- P.getQueries zkHandle
  let getRelatedQueries = [P.queryId query | query <- queries, name `elem` P.getRelatedStreams query]
  Log.debug . Log.buildString
     $ "TERMINATE: the queries related to the terminating stream " <> show name
    <> ": " <> show getRelatedQueries
  mapM_ (handleQueryTerminate sc . OneQuery) getRelatedQueries

handleQueryTerminate :: ServerContext -> TerminationSelection -> IO [CB.CBytes]
handleQueryTerminate ServerContext{..} (OneQuery qid) = do
  hmapQ <- readMVar runningQueries
  case HM.lookup qid hmapQ of Just tid -> killThread tid; _ -> pure ()
  P.setQueryStatus qid Terminated zkHandle
  void $ swapMVar runningQueries (HM.delete qid hmapQ)
  Log.debug . Log.buildString $ "TERMINATE: terminated query: " <> show qid
  return [qid]
handleQueryTerminate sc@ServerContext{..} AllQueries = do
  hmapQ <- readMVar runningQueries
  handleQueryTerminate sc (ManyQueries $ HM.keys hmapQ)
handleQueryTerminate ServerContext{..} (ManyQueries qids) = do
  hmapQ <- readMVar runningQueries
  qids' <- foldrM (action hmapQ) [] qids
  Log.debug . Log.buildString $ "TERMINATE: terminated queries: " <> show qids'
  return qids'
  where
    action hm x terminatedQids = do
      result <- try $ do
        case HM.lookup x hm of
          Just tid -> do
            killThread tid
            P.setQueryStatus x Terminated zkHandle
            void $ swapMVar runningQueries (HM.delete x hm)
          _        ->
            Log.debug $ "query id " <> Log.buildString' x <> " not found"
      case result of
        Left (e ::SomeException) -> do
          Log.warning . Log.buildString
            $ "TERMINATE: unable to terminate query: " <> show x
           <> "because of " <> show e
          return terminatedQids
        Right _                  -> return (x:terminatedQids)

--------------------------------------------------------------------------------

alignDefault :: Text -> Text
alignDefault x  = if T.null x then clientDefaultKey else x

orderingKeyToStoreKey :: Text -> Maybe CBytes
orderingKeyToStoreKey key
  | key == clientDefaultKey = Nothing
  | T.null key = Nothing
  | otherwise  = Just $ textToCBytes key
