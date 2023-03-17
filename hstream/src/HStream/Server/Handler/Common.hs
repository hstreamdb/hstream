{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Server.Handler.Common where

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception                     (Handler (Handler),
                                                        SomeException (..),
                                                        bracket, catches,
                                                        onException, throw,
                                                        throwIO, try)
import           Control.Exception.Base                (AsyncException (..))
import           Control.Monad
import qualified Data.Aeson                            as Aeson
import qualified Data.ByteString                       as BS
import qualified Data.ByteString.Lazy                  as BL
import           Data.Default                          (def)
import           Data.Foldable                         (foldlM)
import           Data.Function                         (fix)
import           Data.Functor                          ((<&>))
import qualified Data.HashMap.Strict                   as HM
import           Data.Int                              (Int64)
import           Data.IORef
import qualified Data.List                             as L
import           Data.Maybe                            (catMaybes, fromJust)
import           Data.Text                             (Text)
import qualified Data.Text                             as T
import qualified Data.Time                             as Time
import           Network.GRPC.HighLevel.Generated
import           Network.GRPC.LowLevel.Op              (Op (OpRecvCloseOnServer),
                                                        OpRecvResult (OpRecvCloseOnServerResult),
                                                        runOps)
import qualified System.Directory                      as Directory
import qualified System.Posix.Files                    as Files

import qualified HStream.Exception                     as HE
import qualified HStream.Logger                        as Log
import qualified HStream.MetaStore.Types               as M
import qualified HStream.Server.HStore                 as HStore
import           HStream.Server.HStreamApi
import qualified HStream.Server.MetaData               as P
import           HStream.Server.Types
import           HStream.SQL.AST
import qualified HStream.Store                         as S
import           HStream.Utils                         (TaskStatus (..),
                                                        cBytesToText,
                                                        lazyByteStringToBytes,
                                                        newRandomText,
                                                        textToCBytes)

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
import           HStream.SQL.Codegen
#else
import qualified Database.RocksDB                      as RocksDB
import           HStream.Processing.Connector
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Snapshot
import           HStream.Processing.Store
import           HStream.SQL.Codegen.V1
#endif

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

--------------------------------------------------------------------------------
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

--------------------------------------------------------------------------------
-- GRPC Handler Helper

-- TODO: return info in a more maintainable way
handleCreateAsSelect :: ServerContext
                     -> Text
                     -> [(In, IdentifierRole)]
                     -> (Out, IdentifierRole)
                     -> DiffFlow.GraphBuilder Row
                     -> Text
                     -> P.RelatedStreams
                     -> IO P.QueryInfo
handleCreateAsSelect ctx@ServerContext{..} sink insWithRole outWithRole builder commandQueryStmtText related = do
  taskName <- newRandomText 10
  qInfo@P.QueryInfo{..} <- P.createInsertQueryInfo
                      taskName commandQueryStmtText related metaHandle
  M.updateMeta queryId P.QueryRunning Nothing metaHandle
  tid <- forkIO $ catches (action queryId taskName) (cleanup queryId)
  modifyMVar_ runningQueries (return . HM.insert queryId tid)
  return qInfo
  where
    action qid taskName = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTask ctx taskName sink insWithRole outWithRole builder
    cleanup qid =
      [ Handler (\(e :: AsyncException) -> do
                    Log.debug . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " is killed because of " <> show e
                    M.updateMeta qid P.QueryTerminated Nothing metaHandle
                    void $ releasePid qid)
      , Handler (\(e :: SomeException) -> do
                    Log.warning . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " died because of " <> show e
                    M.updateMeta qid P.QueryAbort Nothing metaHandle
                    void $ releasePid qid)
      ]
    releasePid qid = modifyMVar_ runningQueries (return . HM.delete qid)
#else

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
  snapshot db keySer valueSer = RocksDB.put db def keySer valueSer

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
runTaskWrapper :: ServerContext -> TaskBuilder -> S.C_LogID -> Maybe RocksDB.DB -> Bool -> IO ()
runTaskWrapper ctx@ServerContext{..} taskBuilder logId db_m writeToHStore = do
  -- taskName: randomly generated in `Codegen.V1`
  let taskName = getTaskName taskBuilder
  let consumerName = textToCBytes taskName

  -- create a new sourceConnector
  let sourceConnector = HStore.hstoreSourceConnectorWithoutCkp ctx (cBytesToText consumerName)
  -- create a new sinkConnector
  let sinkConnector = if writeToHStore
                         then HStore.hstoreSinkConnector ctx
                         else HStore.blackholeSinkConnector
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
  case db_m of
    Nothing -> do
      Log.warning "Snapshotting is not available. Only changelog is working..."
      runTask sourceConnector
              sinkConnector
              taskBuilder
              (scLDClient,logId)
              ()
              (\_ -> return ())
              transKSrc
              transVSrc
              transKSnk
              transVSnk
    Just db ->
      runTask sourceConnector
              sinkConnector
              taskBuilder
              (scLDClient,logId)
              db
              (doSnapshot (scLDClient,logId) db)
              transKSrc
              transVSrc
              transKSnk
              transVSnk

handleCreateAsSelect :: ServerContext
                     -> TaskBuilder
                     -> Text
                     -> Text
                     -> P.RelatedStreams
                     -> Bool
                     -> IO P.QueryInfo
handleCreateAsSelect ctx@ServerContext{..} taskBuilder queryId commandQueryStmtText related writeToHStore = do
  ---- check if the query exists and try to recover it ----
  M.getMeta @P.QueryInfo queryId metaHandle >>= \case
    Just qInfo@P.QueryInfo{..} -> M.getMeta @P.QueryStatus queryId metaHandle >>= \case
      Just P.QueryTerminated -> restoreStateAndRun >> return qInfo
      Just P.QueryAbort      -> restoreStateAndRun >> return qInfo
      Just P.QueryRunning    -> throwIO $ HE.UnexpectedError ("Query " <> T.unpack queryId <> " is already running") -- FIXME: which exception should it throw?
      Just P.QueryCreated    -> throwIO $ HE.UnexpectedError ("Query " <> T.unpack queryId <> " has state CREATED") -- FIXME: do what?
      _                      -> throwIO $ HE.UnexpectedError ("Query " <> T.unpack queryId <> " has unknown state")
    Nothing    -> do
      -- state store(changelog)
      let streamId = transToTempStreamName queryId
      let attrs = S.def { S.logReplicationFactor = S.defAttr1 1 }
      S.createStream scLDClient streamId attrs
      logId <- S.createStreamPartition scLDClient streamId Nothing mempty
      -- state store(snapshot)
      let dbPath = querySnapshotPath
      let dbOption = def { RocksDB.createIfMissing = True }
      db_m <- Directory.doesPathExist dbPath >>= \case
        -- path exists, check read and write permissions
        True  -> Files.fileAccess dbPath True True False >>= \case
          True  -> RocksDB.open dbOption dbPath <&> Just
          False -> return Nothing
        -- path does not exist, try to create it
        False -> try (Directory.createDirectory dbPath) >>= \case
          Left (e :: SomeException)  -> return Nothing
          Right _                    -> RocksDB.open dbOption dbPath <&> Just
      -- update metadata
      qInfo@P.QueryInfo{..} <- P.createInsertQueryInfo
                          queryId commandQueryStmtText related metaHandle
      M.updateMeta queryId P.QueryRunning Nothing metaHandle
      tid <- forkIO $ catches (action queryId taskBuilder logId db_m) (cleanup queryId db_m)
      modifyMVar_ runningQueries (return . HM.insert queryId tid)
      return qInfo
  where
    restoreStateAndRun = do
      -- snapshot
      let dbPath = querySnapshotPath
      db_m <- Directory.doesPathExist dbPath >>= \case
        -- path exists, check read permission
        True  -> Files.fileAccess dbPath True False False >>= \case
          True  -> RocksDB.open def dbPath <&> Just
          False -> return Nothing
        -- path does not exist, which means no snapshot
        False -> return Nothing
      (builder_1, lsn_1) <- case db_m of
        Nothing -> do
          Log.warning "Snapshotting is not available. Roll back to changelog-based restoration..."
          return (taskBuilder, S.LSN_MIN)
        Just db ->
          foldlM (\(acc_builder,acc_lsn) storeName -> do
                     let key = StateStoreSnapshotKey
                             { snapshotQueryId = queryId
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
                 ) (taskBuilder, S.LSN_MIN) (HM.keys $ stores taskBuilder)
      -- changelog
      let streamId = transToTempStreamName queryId
      logId <- S.getUnderlyingLogId scLDClient streamId Nothing
      tailLSN <- S.getTailLSN scLDClient logId
      reader <- S.newLDReader scLDClient 1 Nothing
      S.readerStartReading reader logId lsn_1 tailLSN
      newBuilder <-
        fix $ \go -> S.readerRead reader 10 >>= \dataRecords -> do
          (curBuilder, curLSN) <-
            foldlM (\(acc_builder,acc_lsn) dr@S.DataRecord{..} -> do
                       let (cl :: StateStoreChangelog K V Ser) = fromJust (Aeson.decode $ BL.fromStrict recordPayload)
                       builder' <- applyStateStoreChangelog acc_builder cl
                       return (builder', S.recordLSN dr)
                   ) (builder_1, lsn_1) dataRecords
          if curLSN >= tailLSN
            then return curBuilder
            else go
      -- update metadata and run task
      M.updateMeta queryId P.QueryRunning Nothing metaHandle
      tid <- forkIO $ catches (action queryId newBuilder logId db_m) (cleanup queryId db_m)
      modifyMVar_ runningQueries (return . HM.insert queryId tid)
    action qid builder logId db_m = do
      Log.debug . Log.buildString
        $ "CREATE AS SELECT: query " <> show qid
       <> " has stared working on " <> show commandQueryStmtText
      runTaskWrapper ctx builder logId db_m writeToHStore
    cleanup qid db_m =
      [ Handler (\(e :: AsyncException) -> do
                    Log.debug . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " is killed because of " <> show e
                    M.updateMeta qid P.QueryTerminated Nothing metaHandle
                    void $ releasePid qid
                    case db_m of
                      Nothing -> return ()
                      Just db -> RocksDB.close db
                )
      , Handler (\(e :: SomeException) -> do
                    Log.warning . Log.buildString
                       $ "CREATE AS SELECT: query " <> show qid
                      <> " died because of " <> show e
                    M.updateMeta qid P.QueryAbort Nothing metaHandle
                    void $ releasePid qid
                    case db_m of
                      Nothing -> return ()
                      Just db -> RocksDB.close db
                )
      ]
    releasePid qid = modifyMVar_ runningQueries (return . HM.delete qid)

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

--------------------------------------------------------------------------------

handlePushQueryCanceled :: ServerCall () -> IO () -> IO ()
handlePushQueryCanceled ServerCall{..} handle = do
  x <- runOps unsafeSC callCQ [OpRecvCloseOnServer]
  case x of
    Left err   -> print err
    Right []   -> putStrLn "GRPCIOInternalUnexpectedRecv"
    Right [OpRecvCloseOnServerResult b]
      -> when b handle
    _ -> putStrLn "impossible happened"
