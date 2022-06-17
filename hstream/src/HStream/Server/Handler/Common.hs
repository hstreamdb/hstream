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
import           Control.Monad
import qualified Data.Aeson                       as Aeson
import           Data.Foldable                    (foldrM)
import           Data.Hashable                    (Hashable)
import qualified Data.HashMap.Strict              as HM
import           Data.Int                         (Int64)
import           Data.List                        (find)
import qualified Data.Map.Strict                  as Map
import           Data.Maybe                       (fromJust)
import           Data.Text                        (Text)
import qualified Data.Text                        as T
import qualified Data.Time                        as Time
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

import           HStream.Connector.ClickHouse
import           HStream.Connector.Common
import           HStream.Connector.MySQL
import           HStream.Connector.Type           (Offset (..), SinkRecord (..),
                                                   SourceRecord (..),
                                                   TemporalFilter (..),
                                                   Timestamp)
import qualified HStream.Connector.Type           as HCT
import           HStream.Connector.Util           (getCurrentTimestamp)
import qualified HStream.Logger                   as Log
import qualified HStream.Server.HStore            as HStore
import           HStream.Server.HStreamApi
import           HStream.Server.Config
import qualified HStream.Server.Persistence       as P
import           HStream.Server.Types
import           HStream.SQL.AST                  (RWindow (..))
import           HStream.SQL.Codegen
import qualified HStream.Store                    as HS
import           HStream.Utils                    (TaskStatus (..),
                                                   cBytesToText,
                                                   clientDefaultKey,
                                                   newRandomText, runWithAddr,
                                                   textToCBytes)

import           DiffFlow.Graph
import           DiffFlow.Shard
import           DiffFlow.Types
import           DiffFlow.Weird

runTaskWrapper :: Text -> [(Node, Text)] -> (Node, Text) -> HS.StreamType -> HS.StreamType -> Maybe RWindow -> GraphBuilder -> Maybe (MVar (DataChangeBatch HCT.Timestamp)) -> IO ()
runTaskWrapper taskName inNodesWithStreams outNodeWithStream sourceType sinkType window graphBuilder accumulation = do
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

    let temporalFilter = case window of
          Nothing -> NoFilter
          Just (RTumblingWindow interval) ->
            let interval_ms = (Time.diffTimeToPicoseconds interval) `div` (1000 * 1000 * 1000)
             in Tumbling (fromIntegral interval_ms)
          Just (RHoppingWIndow len hop) ->
            let len_ms = (Time.diffTimeToPicoseconds len) `div` (1000 * 1000 * 1000)
                hop_ms = (Time.diffTimeToPicoseconds hop) `div` (1000 * 1000 * 1000)
             in Hopping (fromIntegral len_ms) (fromIntegral hop_ms)
          _ -> error "not supported"

    -- RUN TASK
    runTask' inNodesWithStreams outNodeWithStream sourceConnectors sinkConnector temporalFilter accumulation shard

--------------------------------------------------------------------------------
runTask' :: [(Node, Text)] -> (Node, Text) -> [SourceConnector] -> SinkConnector -> TemporalFilter -> Maybe (MVar (DataChangeBatch HCT.Timestamp)) -> Shard HStream.Connector.Type.Timestamp -> IO ()
runTask' inNodesWithStreams outNodeWithStream sourceConnectors sinkConnector temporalFilter accumulation shard@Shard{..} = do

  -- the task itself
  forkIO $ run shard

  -- subscribe to all source streams
  forM_ (sourceConnectors `zip` inNodesWithStreams)
    (\(SourceConnector{..}, (_, sourceStreamName)) ->
        subscribeToStream sourceStreamName Latest
    )

  -- main loop: push input to INPUT nodes
  forkIO . forever $ do
    forM_ (sourceConnectors `zip` inNodesWithStreams)
      (\(SourceConnector{..}, (inNode, _)) -> do
          sourceRecords <- readRecords
          forM_ sourceRecords $ \SourceRecord{..} -> do
            let dataChange
                  = DataChange
                  { dcRow = (fromJust . Aeson.decode $ srcValue)
                  , dcTimestamp = Timestamp srcTimestamp []
                  , dcDiff = 1
                  }
            Prelude.print $ "### Get input: " <> show dataChange
            pushInput shard inNode dataChange -- original update

            -- insert new negated updates to limit the valid range of this update
            case temporalFilter of
              NoFilter -> return ()
              Tumbling interval_ms -> do
                let insert_ms = timestampTime (dcTimestamp dataChange)
                let start_ms = interval_ms * (insert_ms `div` interval_ms)
                    end_ms   = interval_ms * (1 + insert_ms `div` interval_ms)
                let negatedDataChange = dataChange { dcTimestamp = Timestamp end_ms [], dcDiff = -1 }
                pushInput shard inNode negatedDataChange -- negated update
              _ -> return ()
          flushInput shard inNode
      )

  -- second loop: advance input after an interval
  forkIO . forever $ do
    forM_ inNodesWithStreams $ \(inNode, _) -> do
      ts <- getCurrentTimestamp
      Prelude.print $ "### Advance time to " <> show ts
      advanceInput shard inNode (Timestamp ts [])
    threadDelay 5000000

  -- third loop: push output from OUTPUT node to output stream
  accumulatedOutput <- case accumulation of
                         Nothing -> newMVar emptyDataChangeBatch
                         Just m  -> return m
  forever $ do
    let (outNode, outStream) = outNodeWithStream
    popOutput shard outNode $ \dcb@DataChangeBatch{..} -> do
      Prelude.print $ "~~~~~~~ POPOUT: " <> show dcb
      forM_ dcbChanges $ \change -> do
        Prelude.print $ "<<< this change: " <> show change
        modifyMVar_ accumulatedOutput
          (\dcb -> return $ updateDataChangeBatch' dcb (\xs -> xs ++ [change]))
        when (dcDiff change > 0) $ do
          let sinkRecord = SinkRecord
                { snkStream = outStream
                , snkKey = Nothing
                , snkValue = Aeson.encode (dcRow change)
                , snkTimestamp = timestampTime (dcTimestamp change)
                }
          replicateM_ (dcDiff change) $ writeRecord sinkConnector sinkRecord


--------------------------------------------------------------------------------

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
  modifyMVar_ runningQueries (return . HM.insert qid tid)
  return (qid, timestamp)
  where
    (tName,inNodesWithStreams,outNodeWithStream,win,builder,accumulation) = case plan of
        SelectPlan tName inNodesWithStreams outNodeWithStream win builder ->
          (tName,inNodesWithStreams,outNodeWithStream,win,builder, Nothing)
        CreateBySelectPlan tName inNodesWithStreams outNodeWithStream win builder _ ->
          (tName,inNodesWithStreams,outNodeWithStream,win,builder, Nothing)
        CreateViewPlan tName schema inNodesWithStreams outNodeWithStream win builder accumulation ->
          (tName,inNodesWithStreams,outNodeWithStream,win,builder, Just accumulation)
        _ -> undefined
    action qid = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTaskWrapper tName inNodesWithStreams outNodeWithStream HS.StreamTypeStream sinkType win builder accumulation
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
    releasePid qid = modifyMVar_ runningQueries (return . HM.delete qid)

--------------------------------------------------------------------------------

alignDefault :: Text -> Text
alignDefault x  = if T.null x then clientDefaultKey else x

orderingKeyToStoreKey :: Text -> Maybe CBytes
orderingKeyToStoreKey key
  | key == clientDefaultKey = Nothing
  | T.null key = Nothing
  | otherwise  = Just $ textToCBytes key
