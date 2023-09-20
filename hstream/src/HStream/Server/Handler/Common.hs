{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent
import           Control.Concurrent.STM                (TVar, atomically,
                                                        newTVarIO, writeTVar)
import           Control.Exception                     (Handler (Handler),
                                                        SomeException (..),
                                                        catches, finally,
                                                        throwIO, try)
import           Control.Exception.Base                (AsyncException (..))
import           Control.Monad
import qualified Data.Aeson                            as Aeson
import qualified Data.ByteString.Lazy                  as BL
import           Data.Default                          (def)
import           Data.Foldable                         (foldlM)
import           Data.Function                         (fix)
import qualified Data.HashMap.Strict                   as HM
import           Data.IORef
import           Data.Maybe                            (fromJust)
import           Data.Text                             (Text)
import qualified Database.RocksDB                      as RocksDB
import           GHC.Stack                             (HasCallStack)

import qualified HStream.Exception                     as HE
import qualified HStream.Logger                        as Log
import qualified HStream.MetaStore.Types               as M
import qualified HStream.Server.HStore                 as HStore
import qualified HStream.Server.MetaData               as P
import           HStream.Server.Types
import           HStream.SQL.AST
import           HStream.SQL.Rts
import qualified HStream.Store                         as S
import           HStream.Utils                         (cBytesToText,
                                                        getPOSIXTime,
                                                        lazyByteStringToBytes,
                                                        msecSince, textToCBytes,
                                                        transToStreamName,
                                                        transToTempStreamName)

#ifdef HStreamUseV2Engine
import qualified DiffFlow.Graph                        as DiffFlow
import qualified DiffFlow.Shard                        as DiffFlow
import qualified DiffFlow.Types                        as DiffFlow
import qualified DiffFlow.Weird                        as DiffFlow
import           HStream.Server.ConnectorTypes         (SinkConnector (..),
                                                        SinkRecord (..),
                                                        SourceConnectorWithoutCkp (..),
                                                        SourceRecord (..))
import qualified HStream.Server.ConnectorTypes         as HCT
import           HStream.SQL.Codegen.V2
#else
import qualified Data.ByteString                       as BS
import           HStream.Processing.Connector
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Snapshot
import           HStream.Processing.Store
import           HStream.SQL
import qualified HStream.Stats                         as Stats
#endif

--------------------------------------------------------------------------------
#ifdef HStreamUseV2Engine
--------------------------------------------------------------------------------
applyTempFilter :: DiffFlow.Shard Row Int64 -> In -> DiffFlow.DataChange Row Int64 -> IO ()
applyTempFilter shard In{..} dataChange = do
  let insert_ms = DiffFlow.timestampTime (DiffFlow.dcTimestamp dataChange)
  case inWindow of
    Nothing -> return ()
    Just (Tumbling interval) -> do
      let interval_ms = calendarDiffTimeToMs interval
      let _start_ms = interval_ms * (insert_ms `div` interval_ms)
          end_ms    = interval_ms * (1 + insert_ms `div` interval_ms)
      let negatedDataChange = dataChange
                              { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                              , DiffFlow.dcDiff = - (DiffFlow.dcDiff dataChange)
                              }
      DiffFlow.pushInput shard inNode negatedDataChange -- negated update
    Just (Hopping interval hop) -> do
      let interval_ms = calendarDiffTimeToMs interval
          hop_ms      = calendarDiffTimeToMs hop
      -- FIXME: determine the accurate semantic of HOPPING window
      let _start_ms = hop_ms * (insert_ms `div` hop_ms)
          end_ms = interval_ms + hop_ms * (insert_ms `div` hop_ms)
      let negatedDataChange = dataChange
                              { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                              , DiffFlow.dcDiff = - (DiffFlow.dcDiff dataChange)
                              }
      DiffFlow.pushInput shard inNode negatedDataChange -- negated update
    Just (Sliding interval) -> do
      let interval_ms = calendarDiffTimeToMs interval
      let _start_ms = insert_ms
          end_ms    = insert_ms + interval_ms
      let negatedDataChange = dataChange
                              { DiffFlow.dcTimestamp = DiffFlow.Timestamp end_ms []
                              , DiffFlow.dcDiff = - (DiffFlow.dcDiff dataChange)
                              }
      DiffFlow.pushInput shard inNode negatedDataChange -- negated update
    _ -> return ()

runTask :: ServerContext
        -> Text
        -> Text
        -> [(In, IdentifierRole)]
        -> (Out, IdentifierRole)
        -> DiffFlow.GraphBuilder FlowObject
        -> IO ()
runTask ctx@ServerContext{..} taskName sink insWithRole outWithRole graphBuilder = do
  ------------------
  let consumerName = taskName
  let graph = DiffFlow.buildGraph graphBuilder
  shard <- DiffFlow.buildShard graph
  stop_m <- newEmptyMVar

  ------------------
  -- the task itself
  tid1 <- forkIO $ DiffFlow.run shard stop_m

  ------------------
  -- In
  srcConnectors_m <- forM insWithRole $ \(In{..}, role) -> do
    case role of
      RoleStream -> do
        let connector@SourceConnectorWithoutCkp{..} = HStore.hstoreSourceConnectorWithoutCkp ctx consumerName
        subscribeToStreamWithoutCkp inStream SpecialOffsetLATEST
        return (Just connector)
      RoleView -> return Nothing

  tids2_m <- forM (insWithRole `zip` srcConnectors_m) $ \((in_@In{..}, role), srcConnector_m) -> do
    case role of
      RoleStream -> do
        let (Just SourceConnectorWithoutCkp{..}) = srcConnector_m
        tid <- forkIO $ withReadRecordsWithoutCkp inStream Just Just $ \sourceRecords -> do
          forM_ sourceRecords $ \SourceRecord{..} -> do
            ts <- HCT.getCurrentTimestamp
            let dataChange
                  = DiffFlow.DataChange
                    { dcRow = (jsonObjectToFlowObject srcStream) . fromJust . Aeson.decode $ srcValue
                    , dcTimestamp = DiffFlow.Timestamp ts [] -- Timestamp srcTimestamp []
                    , dcDiff = 1
                    }
            Log.debug . Log.buildString $ "Get input: " <> show dataChange
            DiffFlow.pushInput shard inNode dataChange -- original update
            -- insert new negated updates to limit the valid range of this update
            applyTempFilter shard in_ dataChange
            DiffFlow.flushInput shard inNode
        return (Just tid)
      RoleView -> do
        viewStore_m <- readIORef P.groupbyStores >>= \hm -> return (hm HM.! inStream)
        dcb <- readMVar viewStore_m
        forM (DiffFlow.dcbChanges dcb) $ \change -> do
          ts <- HCT.getCurrentTimestamp
          let thisChange = change { DiffFlow.dcTimestamp = DiffFlow.Timestamp ts [] }
          Log.debug . Log.buildString $ "Get input(from view): " <> show thisChange
          DiffFlow.pushInput shard inNode thisChange
        return Nothing

  ------------------
  -- second loop: advance input after an interval
  tid3 <- forkIO . forever $ do
    forM_ insWithRole $ \(In{..}, _) -> do
      ts <- HCT.getCurrentTimestamp
      -- Log.debug . Log.buildString $ "### Advance time to " <> show ts
      DiffFlow.advanceInput shard inNode (DiffFlow.Timestamp ts [])
    threadDelay 1000000

  -- third loop: push output from OUTPUT node to output stream
  let (out, outRole) = outWithRole
  forever (do
    DiffFlow.popOutput shard (outNode out) (threadDelay 100000) $ \dcb@DiffFlow.DataChangeBatch{..} -> do
      Log.debug . Log.buildString $ "~~~ POPOUT: " <> show dcb
      case outRole of
        RoleStream -> do
          let SinkConnector{..} = HStore.hstoreSinkConnector ctx
          forM_ dcbChanges $ \change -> do
            Log.debug . Log.buildString $ "<<< this change: " <> show change
            when (DiffFlow.dcDiff change > 0) $ do
              let sinkRecord = SinkRecord
                    { snkStream = sink
                    , snkKey = Nothing
                    , snkValue = (Aeson.encode . flowObjectToJsonObject) (DiffFlow.dcRow change)
                    , snkTimestamp = DiffFlow.timestampTime (DiffFlow.dcTimestamp change)
                    }
              replicateM_ (DiffFlow.dcDiff change) $ writeRecord Just Just sinkRecord
        RoleView -> do
          viewStore_m <- readIORef P.groupbyStores >>= \hm -> return (hm HM.! sink)
          modifyMVar_ viewStore_m
            (\old -> return $ DiffFlow.updateDataChangeBatch' old (\xs -> xs ++ dcbChanges))
          ) `onException` (do
    let childrenThreads = tid1 : (catMaybes tids2_m) ++ [tid3]
    mapM_ killThread childrenThreads
    forM (insWithRole `zip` srcConnectors_m) $ \((in_@In{..}, role), srcConnector_m) -> do
      case srcConnector_m of
        Just sc -> unSubscribeToStreamWithoutCkp sc inStream
        Nothing -> return ()
                          )

runImmTask :: ServerContext
           -> [(In, IdentifierRole)]
           -> Out
           -> MVar (DiffFlow.DataChangeBatch Row Int64)
           -> DiffFlow.GraphBuilder FlowObject
           -> IO ()
runImmTask ctx@ServerContext{..} insWithRole out out_m graphBuilder = do
  let graph = DiffFlow.buildGraph graphBuilder
  shard <- DiffFlow.buildShard graph
  stop_m <- newEmptyMVar

  -- run DiffFlow shard
  task_async <- async $ DiffFlow.run shard stop_m

  -- In
  forM_ insWithRole $ \(in_@In{..}, role) -> do
    case role of
      RoleView -> do
        viewStore_m <- readIORef P.groupbyStores >>= \hm -> return (hm HM.! inStream)
        dcb <- readMVar viewStore_m
        forM (DiffFlow.dcbChanges dcb) $ \change -> do
          ts <- HCT.getCurrentTimestamp
          let thisChange = change { DiffFlow.dcTimestamp = DiffFlow.Timestamp ts [] }
          Log.debug . Log.buildString $ "Get input(from view): " <> show thisChange
          DiffFlow.pushInput shard inNode thisChange
          -- insert new negated updates to limit the valid range of this update
          applyTempFilter shard in_ thisChange
          DiffFlow.flushInput shard inNode
      RoleStream ->
        throwIO $ HE.InvalidSqlStatement "Can not perform non-pushing SELECT from streams. "

  -- advance input
  forM_ insWithRole $ \(In{..}, _) -> replicateM_ 10 $ do
    ts <- HCT.getCurrentTimestamp
    -- Log.debug . Log.buildString $ "### Advance time to " <> show ts
    DiffFlow.advanceInput shard inNode (DiffFlow.Timestamp ts [])
    threadDelay 1000

  -- push output from OUTPUT node
  replicateM_ 5 $ do
    DiffFlow.popOutput shard (outNode out) (threadDelay 100000) $ \dcb@DiffFlow.DataChangeBatch{..} -> do
      Log.debug . Log.buildString $ "~~~ POPOUT: " <> show dcb
      modifyMVar_ out_m
        (\old -> return $ DiffFlow.updateDataChangeBatch' old (\xs -> xs ++ dcbChanges))

  -- stop DiffFlow.run
  putMVar stop_m ()
  wait task_async

-- TODO: return info in a more maintainable way
createQueryAndRun :: ServerContext
                     -> Text
                     -> [(In, IdentifierRole)]
                     -> (Out, IdentifierRole)
                     -> DiffFlow.GraphBuilder Row
                     -> Text
                     -> P.RelatedStreams
                     -> IO P.QueryInfo
createQueryAndRun ctx@ServerContext{..} sink insWithRole outWithRole builder commandQueryStmtText related = do
  taskName <- newRandomText 10
  qInfo@P.QueryInfo{..} <- P.createInsertQueryInfo
                      taskName commandQueryStmtText related metaHandle
  M.updateMeta queryId P.QueryRunning Nothing metaHandle
  tid <- forkIO $ catches (action queryId taskName) (cleanup queryId)
  modifyMVar_ runningQueries (return . HM.insert queryId tid)
  return qInfo
  where
    msgPrefix = "createQueryAndRun (from CREATE AS SELECT or INSERT SELECT): query "
    action qid taskName = do
      Log.debug . Log.buildString
        $ msgPrefix <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTask ctx taskName sink insWithRole outWithRole builder
    cleanup qid =
      [ Handler (\(e :: AsyncException) -> do
                    Log.debug . Log.buildString
                       $ msgPrefix <> show qid
                      <> " is killed because of " <> show e
                    M.updateMeta qid P.QueryTerminated Nothing metaHandle
                    void $ releasePid qid)
      , Handler (\(e :: SomeException) -> do
                    Log.warning . Log.buildString
                       $ msgPrefix <> show qid
                      <> " died because of " <> show e
                    M.updateMeta qid P.QueryAborted Nothing metaHandle
                    void $ releasePid qid)
      ]
    releasePid qid = modifyMVar_ runningQueries (return . HM.delete qid)

--------------------------------------------------------------------------------
#else
--------------------------------------------------------------------------------

data QueryRunner = QueryRunner {
    qRTaskBuilder     :: TaskBuilder
  , qRQueryName       :: Text
  , qRWhetherToHStore :: Bool
  , qRQueryString     :: Text
  , qRQuerySources    :: [Text]
  , qRConsumerClosed  :: TVar Bool
  }

---- store processing node states (changelog)
-- do nothing
instance ChangeLogger () where
  logChangelog () _ = return ()
  getChangelogProgress () = return minBound

-- use logdevice stream
instance ChangeLogger (S.LDClient, S.C_LogID) where
  logChangelog (ldClient, logId) bs =
    void $ S.append ldClient logId (lazyByteStringToBytes bs) Nothing
  getChangelogProgress (ldClient, logId) = S.getTailLSN ldClient logId

---- store processing node states (snapshot)
-- do nothing
instance Snapshotter () where
  snapshot () _ _ = return ()

-- use rocksdb
instance Snapshotter RocksDB.DB where
  snapshot db = RocksDB.put db def

doSnapshot :: (ChangeLogger h1, Snapshotter h2) => h1 -> h2 -> Task -> IO ()
doSnapshot h1 h2 Task{..} = do
  changelogTail <- getChangelogProgress h1
  forM_ (HM.toList taskStores) $ \(storeName, (ess,_)) -> do
    let key = StateStoreSnapshotKey
            { snapshotQueryId = taskName
            , snapshotStoreName = storeName
            }
    value <- case ess of
      EKVStateStore dekvs -> do
        let ekvs = fromDEKVStoreToEKVStore @K @V dekvs
        extData <- ksDump ekvs
        return $ SnapshotKS changelogTail extData
      ESessionStateStore desss -> do
        let esss = fromDESessionStoreToESessionStore @K @V desss
        extData <- ssDump esss
        return $ SnapshotSS changelogTail extData
      ETimestampedKVStateStore detkvs -> do
        let etkvs = fromDETimestampedKVStoreToETimestampedKVStore @Ser @Ser detkvs
        extData <- tksDump etkvs
        return $ SnapshotTKS changelogTail extData
    let keySer   = BL.toStrict $ Aeson.encode key
        valueSer = BL.toStrict $ Aeson.encode value
    snapshot h2 keySer valueSer
  Log.debug $ "Query " <> Log.build taskName <> ": I have successfully done a snapshot!"

--------------------------------------------------------------------------------

-- This function may throw exceptions just like 'runTask'.
runTaskWrapper :: ServerContext -> SourceConnectorWithoutCkp -> SinkConnector -> TaskBuilder -> Text -> S.C_LogID -> IO ()
runTaskWrapper ServerContext{..} sourceConnector sinkConnector taskBuilder queryId logId = do
  -- RUN TASK
  let transKSrc = \s bl -> case Aeson.decode bl of
                          Nothing -> Nothing
                          Just k  -> Just . Aeson.encode $
                                     jsonObjectToFlowObject s k
      transVSrc = transKSrc
      transKSnk = \bl -> case Aeson.decode bl of
                             Nothing -> Nothing
                             Just k  -> Just . Aeson.encode $
                                        flowObjectToJsonObject k
      transVSnk = transKSnk
  case querySnapshotter of
    Nothing -> do
      Log.warning "Snapshotting is not available. Only changelog is working..."
      runTask scStatsHolder
              sourceConnector
              sinkConnector
              taskBuilder
              queryId
              (scLDClient,logId)
              ()
              (\_ -> return ())
#ifdef HStreamEnableSchema
              (P.getSchema metaHandle)
#endif
              transKSrc
              transVSrc
              transKSnk
              transVSnk
    Just db ->
      runTask scStatsHolder
              sourceConnector
              sinkConnector
              taskBuilder
              queryId
              (scLDClient,logId)
              db
              (doSnapshot (scLDClient,logId) db)
#ifdef HStreamEnableSchema
              (P.getSchema metaHandle)
#endif
              transKSrc
              transVSrc
              transKSnk
              transVSnk

createQueryAndRun :: HasCallStack => ServerContext -> QueryRunner -> IO ()
createQueryAndRun ctx@ServerContext{..} qRunner@QueryRunner{..} = do
  -- prepare logdevice stream for restoration
  let streamId = transToTempStreamName qRQueryName
  let attrs = S.def { S.logReplicationFactor = S.defAttr1 1 }
  S.doesStreamExist scLDClient streamId >>= \case
    True  -> S.removeStream scLDClient streamId
    False -> return ()
  logId <- do
    try @SomeException (S.createStream scLDClient streamId attrs
      >> S.createStreamPartition scLDClient streamId Nothing mempty)
    >>= \case Right logId -> return logId
              Left  err   -> P.deleteQueryInfo qRQueryName metaHandle
                          >> throwIO err
  -- update metadata & fork working thread
  runQuery ctx qRunner logId

restoreStateAndRun :: ServerContext -> QueryRunner -> IO ()
restoreStateAndRun ctx@ServerContext{..} qRunner@QueryRunner{..} = do
  M.updateMeta qRQueryName P.QueryResuming Nothing metaHandle
  (newBuilder, logId) <- try @SomeException (restoreState ctx qRunner) >>= \case
    Right x  -> return x
    Left err -> M.updateMeta qRQueryName P.QueryAborted Nothing metaHandle
             >> throwIO err
  runQuery ctx qRunner {qRTaskBuilder = newBuilder} logId

restoreState :: ServerContext -> QueryRunner -> IO (TaskBuilder, S.C_LogID)
restoreState ServerContext{..} QueryRunner{..} = do
  (builder_1, lsn_1) <- case querySnapshotter of
    Nothing -> do
      Log.warning "Snapshot is not available. Roll back to changelog-based restoration..."
      return (qRTaskBuilder, S.LSN_MIN)
    Just db ->
      foldlM (\(acc_builder,acc_lsn) storeName -> do
                 let key = StateStoreSnapshotKey
                         { snapshotQueryId = qRQueryName
                         , snapshotStoreName = storeName
                         }
                     keySer = BL.toStrict $ Aeson.encode key
                 RocksDB.get db def keySer >>= \case
                   Nothing       -> return (acc_builder,acc_lsn)
                   Just valueSer -> do
                     let (sv :: StateStoreSnapshotValue S.LSN K V Ser) =
                           fromJust (Aeson.decode $ BL.fromStrict valueSer)
                     (builder_m, i) <- applyStateStoreSnapshot acc_builder key sv
                     case builder_m of
                       Nothing       -> return (acc_builder,acc_lsn)
                       Just builder' -> return (builder',i)
             ) (qRTaskBuilder, S.LSN_MIN) (HM.keys $ stores qRTaskBuilder)
  -- changelog
  Log.debug $ "Snapshot restoration for query with name" <> Log.build qRQueryName
           <> " completed, restoring the rest of the computation from changelog"
  let streamId = transToTempStreamName qRQueryName
  logId <- S.getUnderlyingLogId scLDClient streamId Nothing
  tailLSN <- S.getTailLSN scLDClient logId
  reader <- S.newLDReader scLDClient 1 Nothing
  S.readerStartReading reader logId lsn_1 tailLSN
  newBuilder <- fix (\f (builder, _lsn)-> do
                       new@(newBuilder, endLsn) <- reconstruct reader builder
                       if endLsn >= tailLSN then return newBuilder else f new)
                    (builder_1, lsn_1)
  Log.info $ "Computation restored for query with name" <> Log.build qRQueryName
  return (newBuilder, logId)
  where
    reconstruct reader oldBuilder = do
      !read_start <- getPOSIXTime
      res <- S.readerReadAllowGap reader 10 >>= \case
        Right dataRecords -> do
          let cStreamName = textToCBytes qRQueryName
          Stats.stream_stat_add_read_in_bytes scStatsHolder cStreamName (fromIntegral . sum $ map (BS.length . S.recordPayload) dataRecords)
          Stats.stream_stat_add_read_in_batches scStatsHolder cStreamName (fromIntegral $ length dataRecords)
          foldlM (\(acc_builder, _acc_lsn) dr@S.DataRecord{..} -> do
                   let (cl :: StateStoreChangelog K V Ser) = fromJust (Aeson.decode $ BL.fromStrict recordPayload)
                   builder' <- applyStateStoreChangelog acc_builder cl
                   return (builder', S.recordLSN dr)
             ) (oldBuilder, S.LSN_MIN) dataRecords
        Left S.GapRecord{..} -> return (oldBuilder, gapHiLSN)
      Stats.serverHistogramAdd scStatsHolder Stats.SHL_ReadLatency =<< msecSince read_start
      return res

runQuery :: HasCallStack => ServerContext -> QueryRunner -> S.C_LogID -> IO ()
runQuery ctx@ServerContext{..} QueryRunner{..} logId = do
  M.updateMeta qRQueryName P.QueryRunning Nothing metaHandle
  tid <- forkIO $ catches action cleanup
  modifyMVar_ runningQueries (return . HM.insert qRQueryName (tid, qRConsumerClosed))
  where
    msgPrefix = "createQueryAndRun (from CREATE AS SELECT or INSERT SELECT): query "
    sinkConnector = if qRWhetherToHStore then HStore.hstoreSinkConnector ctx
                                         else HStore.blackholeSinkConnector
    sourceConnector = HStore.hstoreSourceConnectorWithoutCkp ctx qRQueryName qRConsumerClosed
    action = do
      Log.debug $ "Start Query " <> Log.build qRQueryString
                <> "with name: " <> Log.build qRQueryName
      runTaskWrapper ctx sourceConnector sinkConnector qRTaskBuilder qRQueryName logId
    cleanup =
      [ Handler (\(e :: HE.StreamReadClose) -> do
                    Log.info . Log.buildString
                       $ msgPrefix <> show qRQueryName
                      <> " is killed because of " <> show e
                    M.updateMeta qRQueryName P.QueryTerminated Nothing metaHandle
                    releasePid
                )
      , Handler (\(e :: SomeException) -> do
                    Log.warning . Log.buildString
                       $ msgPrefix <> show qRQueryName
                      <> " died because of " <> show e
                    M.updateMeta qRQueryName P.QueryAborted Nothing metaHandle
                    releasePid
                )
      ]
    releasePid = modifyMVar_ runningQueries (return . HM.delete qRQueryName)

--------------------------------------------------------------------------------
#endif
-------------------------------------------------------------------------------

data IdentifierRole = RoleStream | RoleView deriving (Eq, Show)

findIdentifierRole :: ServerContext -> Text -> IO (Maybe IdentifierRole)
findIdentifierRole ServerContext{..} name = do
  isStream <- S.doesStreamExist scLDClient (transToStreamName name)
  case isStream of
    True  -> return (Just RoleStream)
    False -> do
      hm <- readIORef P.groupbyStores
      case HM.lookup name hm of
        Nothing -> return Nothing
        Just _  -> return (Just RoleView)

amIStream :: ServerContext -> Text -> IO Bool
amIStream ServerContext{..} name = S.doesStreamExist scLDClient (transToStreamName name)

amIView :: ServerContext -> Text -> IO Bool
amIView ServerContext{..} name = M.checkMetaExists @P.ViewInfo name metaHandle
