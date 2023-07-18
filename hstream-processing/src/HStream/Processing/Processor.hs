{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData          #-}

module HStream.Processing.Processor
  ( build,
    buildTask,
    taskBuilderWithName,
    addSource,
    addProcessor,
    addSink,
    addStateStore,
    runTask,
    runImmTask,
    forward,
    getKVStateStore,
    getSessionStateStore,
    getTimestampedKVStateStore,
    getTaskName,
    Materialized (..),
    Record (..),
    Processor (..),
    SourceConfig (..),
    SinkConfig (..),
    TaskBuilder (..),
    TaskTopologyConfig (..),
    Task (..),

    ChangeLogger (..),
    StateStoreChangelog (..),
    applyStateStoreChangelog,
    applyStateStoreSnapshot
  )
where

import           Control.Concurrent
import           Control.Exception                      (throw)
import qualified Data.Aeson                             as Aeson
import           Data.Maybe
import           Data.Typeable
import qualified Prelude
import qualified RIO
import           RIO
import qualified RIO.ByteString.Lazy                    as BL
import qualified RIO.HashMap                            as HM
import           RIO.HashMap.Partial                    as HM'
import qualified RIO.HashSet                            as HS
import qualified RIO.List                               as L
import qualified RIO.Map                                as Map
import qualified RIO.Text                               as T

import qualified HStream.Exception                      as HE
import qualified HStream.Logger                         as Log
import           HStream.Processing.Connector
import           HStream.Processing.Encoding
import           HStream.Processing.Error               (HStreamProcessingError (..))
import           HStream.Processing.Processor.ChangeLog
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Processor.Snapshot
import           HStream.Processing.Store
import           HStream.Processing.Type
import           HStream.Processing.Util
import qualified HStream.Server.HStreamApi              as API
import           HStream.Stats                          (StatsHolder,
                                                         query_stat_add_total_execute_errors,
                                                         query_stat_add_total_input_records,
                                                         query_stat_add_total_output_records)
import           HStream.Utils                          (textToCBytes)

data Materialized k v s = Materialized
  { mKeySerde   :: Serde k s,
    mValueSerde :: Serde v s,
    -- mStateStore :: StateStore BL.ByteString BL.ByteString
    mStateStore :: StateStore k v
  }

build :: TaskBuilder -> Task
build tp@TaskTopologyConfig {..} =
  let _ = validateTopology tp
      topologyForward =
        HM.foldlWithKey'
          ( \acc k v ->
              let childName = k
                  (curP, parentsName) = v
                  nacc =
                    if HM.member childName acc
                      then acc
                      else HM.insert childName (curP, []) acc
               in foldl'
                    ( \acc' parent ->
                        if HM.member parent acc'
                          then
                            let (p, cs) = acc' HM'.! parent
                             in HM.insert parent (p, cs ++ [childName]) acc'
                          else
                            let (p, _) = topology HM'.! parent
                             in HM.insert parent (p, [childName]) acc'
                    )
                    nacc
                    parentsName
          )
          (HM.empty :: HM.HashMap T.Text (EProcessor, [T.Text]))
          topology
   in Task
        { taskName = ttcName,
          taskSourceConfig = sourceCfgs,
          taskSinkConfig = sinkCfgs,
          taskTopologyReversed = topology,
          taskTopologyForward = topologyForward,
          taskStores = stores
        }

buildTask ::
  T.Text ->
  TaskBuilder
buildTask taskName =
  mempty
    { ttcName = taskName
    }

taskBuilderWithName ::
  TaskBuilder -> T.Text -> TaskBuilder
taskBuilderWithName builder taskName =
  builder
    { ttcName = taskName
    }

-- | Run a task. This function will block the current thread.
--   And this function will throw an exception if any (unrecoverable)
--   error occurs, see the comments at 'go' following.
runTask
  :: (ChangeLogger h1, Snapshotter h2)
  => StatsHolder
  -> SourceConnectorWithoutCkp
  -> SinkConnector
  -> TaskBuilder
  -> Text  -- ^ queryId, use for stats gathering
  -> h1
  -> h2
  -> (Task -> IO ())
  -> (T.Text -> BL.ByteString -> Maybe BL.ByteString)
  -> (T.Text -> BL.ByteString -> Maybe BL.ByteString)
  -> (BL.ByteString -> Maybe BL.ByteString)
  -> (BL.ByteString -> Maybe BL.ByteString)
  -> IO ()
runTask statsHolder SourceConnectorWithoutCkp {..} sinkConnector taskBuilder@TaskTopologyConfig {..} qid changeLogger snapshotter doSnapshot transKSrc transVSrc transKSnk transVSnk = do
  -- build and add internalSinkProcessor
  let sinkProcessors =
        HM.map
          ( buildInternalSinkProcessor
              sinkConnector
              transKSnk
              transVSnk
          )
          sinkCfgs
  let newTaskBuilder =
        HM.foldlWithKey'
          (\a k v -> a <> addProcessor k v [T.append k serializerNameSuffix])
          taskBuilder
          sinkProcessors

  -- build forward topology
  let task@Task {..} = build newTaskBuilder

  -- runTask
  let sourceStreamNames = HM.keys taskSourceConfig
  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    ctx <- buildTaskContext task lf changeLogger snapshotter
    forM_ sourceStreamNames $ \stream -> do
      isSubscribedToStreamWithoutCkp stream >>= \case
        True  -> return ()
        False -> subscribeToStreamWithoutCkp stream API.SpecialOffsetLATEST

    chan <- newTChanIO

    -- Note: Start many threads of 'f' and one thread of 'g':
    -- 1. 'f' is responsible for reading records from source streams and
    --    writing them to the channel.
    -- 2. 'g' is responsible for reading records from the channel and doing
    --    the actual processing

    -- [important] we use 'forConcurrently_' to ensure other threads are
    --             cancelled when one of them throws an exception.
    -- [important] we use 'waitEitherCancel' to ensure both threads are
    --             cleaned up when one of them throws an exception.
    --             The 'finally' here is just for setting 'connectorClosed'.
    --             Otherwise, the consumers will not exit and the
    --             subscriptions will not be deleted. We do not do any
    --             error handling here and re-throw it to the top level
    --             (by 'finally').


    -- 'forConcurrently_' here: important! See the comment above.
    withAsync (forConcurrently_ sourceStreamNames (f chan connectorClosed)) $ \a ->
      withAsync (g task ctx chan) $ \b ->
        -- 'finally' here: important! See the comment above.
        void (waitEitherCancel a b) `finally` do
          atomically $ writeTVar connectorClosed True
          forM_ sourceStreamNames (\stream -> do
            isSubscribedToStreamWithoutCkp stream >>= \case
              True  -> unSubscribeToStreamWithoutCkp stream
              False -> return ()  )
  where
    f :: TChan ([SourceRecord], MVar ()) -> TVar Bool -> T.Text -> IO ()
    f chan consumerClosed sourceStreamName =
      withReadRecordsWithoutCkp sourceStreamName (transKSrc sourceStreamName) (transVSrc sourceStreamName) consumerClosed $ \sourceRecords -> do
        mvar <- RIO.newEmptyMVar
        let callback  = do
              query_stat_add_total_input_records statsHolder (textToCBytes qid) (fromIntegral . length $ sourceRecords)
              atomically $ writeTChan chan (sourceRecords, mvar)
            beforeAck = RIO.takeMVar mvar
        return (callback,beforeAck)

    g :: Task -> TaskContext -> TChan ([SourceRecord], MVar ()) -> IO ()
    g task@Task{..} ctx chan = do
      timer <- newIORef False
      -- Start two threads here. One is used to set the timer of snapshotting
      -- and the other is used to do the **snapshotting** and **processing**.

      -- [important] we use 'waitEitherCancel' to ensure both threads are
      --             cancelled when one of them throws an exception.
      withAsync (forever $ do
        Control.Concurrent.threadDelay $ 10 * 1000 * 1000
        atomicWriteIORef timer True) $ \a -> withAsync (forever $ do
          readIORef timer >>= \case
            False -> go
            True  -> do
              doSnapshot task
              atomicWriteIORef timer False) $ \b -> void (waitEitherCancel a b)
      where
        go = do
          (sourceRecords, mvar) <- atomically $ readTChan chan
          runRIO ctx $ forM_ sourceRecords $ \SourceRecord {..} -> do
            let acSourceName = iSourceName (taskSourceConfig HM'.! srcStream)
            let (sourceEProcessor, _) = taskTopologyForward HM'.! acSourceName
            liftIO $ updateTimestampInTaskContext ctx srcTimestamp
            -- [WARNING] [FIXME]
            -- The following code means that we only consider 'StreamNotFound'
            -- as a fatal error so we re-throw it. This causes all threads
            -- related to this task to exit. As for other errors, we just
            -- log them and continue.
            -- HOWEVER, this should be re-considered. Which errors are actually
            -- fatal?
            catches (runEP sourceEProcessor (mkERecord Record {recordKey = srcKey, recordValue = srcValue, recordTimestamp = srcTimestamp}))
              [ Handler $ \(err :: HE.StreamNotFound) -> do
                liftIO $ query_stat_add_total_execute_errors statsHolder (textToCBytes qid) 1
                -- 'throw' here: very important! Or the threads related to the
                --               task will not be cleaned up. See above.
                throw err
              , Handler $ \(err :: SomeException) -> do
                liftIO $ Log.warning $ Log.buildString' err
                liftIO $ query_stat_add_total_execute_errors statsHolder (textToCBytes qid) 1
                -- No 'throw' here: very important! Just omit the error and
                --                  continue processing. See above.
              ]
            liftIO $ query_stat_add_total_output_records statsHolder (textToCBytes qid) 1
          -- NOTE: tell the server that we have processed this "batch" of records
          --       so it can mark them as acked. Here we associate a batch of
          --       'SourceRecord' with a single ack. This granularity may be too
          --       coarse if the batch size is large.
          -- CAUTION: the position of the following line!!!
          liftIO $ RIO.putMVar mvar ()

runImmTask ::
  (Ord t, Semigroup t, Aeson.FromJSON t, Aeson.ToJSON t, Typeable t, ChangeLogger h1, Snapshotter h2) =>
  [(T.Text, Materialized t t t)] ->
  SinkConnector ->
  TaskBuilder ->
  h1 ->
  h2 ->
  (BL.ByteString -> Maybe BL.ByteString) ->
  (BL.ByteString -> Maybe BL.ByteString) ->
  IO ()
runImmTask srcTups sinkConnector taskBuilder@TaskTopologyConfig {..} changeLogger snapshotter transKSnk transVSnk = do
  -- build and add internalSinkProcessor
  let sinkProcessors =
        HM.map
          ( buildInternalSinkProcessor
              sinkConnector
              transKSnk
              transVSnk
          )
          sinkCfgs
  let newTaskBuilder =
        HM.foldlWithKey'
          (\a k v -> a <> addProcessor k v [T.append k serializerNameSuffix])
          taskBuilder
          sinkProcessors

  -- build forward topology
  let task@Task {..} = build newTaskBuilder

  -- runTask
  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    ctx <- buildTaskContext task lf changeLogger snapshotter

    loop task ctx srcTups []
  where
    runOnePath :: Task -> TaskContext -> (T.Text, [SourceRecord]) -> IO ()
    runOnePath Task {..} ctx (sourceStreamName, sourceRecords) = runRIO ctx $ do
      forM_ sourceRecords $ \r@SourceRecord {..} -> do
        let acSourceName = iSourceName (taskSourceConfig HM'.! srcStream)
        let (sourceEProcessor, _) = taskTopologyForward HM'.! acSourceName
        liftIO $ updateTimestampInTaskContext ctx srcTimestamp
        e' <- try $ runEP sourceEProcessor (mkERecord Record {recordKey = srcKey, recordValue = srcValue, recordTimestamp = srcTimestamp})
        case e' of
          Left (e :: SomeException) -> liftIO $ Log.warning $ Log.buildString (Prelude.show e)
          Right _ -> return ()
    loop :: (Ord t, Semigroup t, Aeson.FromJSON t, Aeson.ToJSON t, Typeable t)
         => Task
         -> TaskContext
         -> [(T.Text, Materialized t t t)]
         -> [Async ()]
         -> IO ()
    loop task ctx [] asyncs = return ()
    loop task ctx [(sourceStreamName, mat)] asyncs = do
      case mStateStore mat of
        KVStateStore ekvStore      -> do
          kvMap <- ksDump ekvStore
          sourceRecords <- forM (Map.toList kvMap) $ \(k,v) -> do
            let v' = k <> v
            ts <- getCurrentTimestamp
            return $ SourceRecord
                   { srcStream = sourceStreamName
                   , srcOffset = 0
                   , srcTimestamp = ts
                   , srcKey = Nothing
                   , srcValue = Aeson.encode v'
                   }
          runOnePath task ctx (sourceStreamName, sourceRecords)
            `onException` forM_ asyncs cancel
        SessionStateStore essStore -> do
          tmMap <- ssDump essStore
          sourceRecords <- (L.concat . L.concat) <$> (forM (Map.elems tmMap) $ \kmap -> do
            forM (Map.toList kmap) $ \(k,m) -> do
              forM (Map.toList m) $ \(ts,v) -> do
                let v' = k <> v
                return $ SourceRecord
                       { srcStream = sourceStreamName
                       , srcOffset = 0
                       , srcTimestamp = ts
                       , srcKey = Nothing
                       , srcValue = Aeson.encode v'
                       })
          runOnePath task ctx (sourceStreamName, sourceRecords)
            `onException` forM_ asyncs cancel
        TimestampedKVStateStore etskvStore -> error "impossible"
    loop task ctx (tup : tups) asyncs = do
      withAsync (loop task ctx [tup] asyncs) $ \a -> do
        loop task ctx tups (asyncs ++ [a])

validateTopology :: TaskTopologyConfig -> ()
validateTopology TaskTopologyConfig {..} =
  if L.null sourceCfgs
    then throw $ TaskTopologyBuildError "task build error: no valid source config"
    else
      if L.null sinkCfgs
        then throw $ TaskTopologyBuildError "task build error: no valid sink config"
        else ()

data SourceConfig k v s = SourceConfig
  { sourceName        :: T.Text,
    sourceStreamName  :: T.Text,
    keyDeserializer   :: Maybe (Deserializer k s),
    valueDeserializer :: Deserializer v s
  }

data SinkConfig k v s = SinkConfig
  { sinkName        :: T.Text,
    sinkStreamName  :: T.Text,
    keySerializer   :: Maybe (Serializer k s),
    valueSerializer :: Serializer v s
  }

addSource ::
  (Typeable k, Typeable v, Typeable s) =>
  SourceConfig k v s ->
  TaskBuilder
addSource cfg@SourceConfig {..} =
  mempty
    { sourceCfgs =
        HM.singleton
          sourceStreamName
          InternalSourceConfig
            { iSourceName = sourceName,
              iSourceStreamName = sourceStreamName
            },
      topology =
        HM.singleton
          sourceName
          (mkEProcessor $ buildSourceProcessor cfg, [])
    }

buildSourceProcessor ::
  (Typeable k, Typeable v) =>
  SourceConfig k v s ->
  Processor s s -- BL.ByteString BL.ByteString
buildSourceProcessor SourceConfig {..} = Processor $ \r@Record {..} -> do
  -- deserialize and forward
  logDebug "enter source processor"
  ctx <- ask
  writeIORef (curProcessor ctx) sourceName
  let rk = fmap runDeser keyDeserializer <*> recordKey
  let rv = runDeser valueDeserializer recordValue
  let rr =
        r
          { recordKey = rk,
            recordValue = rv
          }
  forward rr

addProcessor ::
  (Typeable kin, Typeable vin) =>
  T.Text ->
  Processor kin vin ->
  [T.Text] ->
  TaskBuilder
addProcessor name processor parentNames =
  mempty
    { topology = HM.singleton name (mkEProcessor processor, parentNames)
    }

buildSinkProcessor ::
  (Typeable k, Typeable v, Typeable s) =>
  SinkConfig k v s ->
  Processor k v
buildSinkProcessor SinkConfig {..} = Processor $ \r@Record {..} -> do
  logDebug $ "enter sink serializer processor for stream " <> display sinkName
  let rk = liftA2 runSer keySerializer recordKey
  let rv = runSer valueSerializer recordValue
  forward r {recordKey = rk, recordValue = rv}

-- liftIO $ writeRecord SinkRecord {snkStream = sinkStreamName, snkKey = rk, snkValue = rv, snkTimestamp = recordTimestamp}

serializerNameSuffix :: T.Text
serializerNameSuffix = "-SERIALIZER"

addSink ::
  (Typeable k, Typeable v, Typeable s) =>
  SinkConfig k v s ->
  [T.Text] ->
  TaskBuilder
addSink cfg@SinkConfig {..} parentNames =
  mempty
    { topology =
        HM.singleton
          (T.append sinkName serializerNameSuffix)
          (mkEProcessor $ buildSinkProcessor cfg, parentNames),
      sinkCfgs =
        HM.singleton
          sinkName
          InternalSinkConfig
            { iSinkName = sinkName,
              iSinkStreamName = sinkStreamName
            }
    }

buildInternalSinkProcessor ::
  SinkConnector ->
  (BL.ByteString -> Maybe BL.ByteString) ->
  (BL.ByteString -> Maybe BL.ByteString) ->
  InternalSinkConfig ->
  Processor BL.ByteString BL.ByteString
buildInternalSinkProcessor sinkConnector transK transV InternalSinkConfig {..} = Processor $ \Record {..} -> do
  ts <- liftIO getCurrentTimestamp
  liftIO $
    writeRecord
      sinkConnector
      transK
      transV
      SinkRecord
        { snkStream = iSinkStreamName,
          snkKey = recordKey,
          snkValue = recordValue,
          snkTimestamp = ts
        }

addStateStore ::
  (Typeable k, Typeable v, Ord k) =>
  T.Text ->
  StateStore k v ->
  [T.Text] ->
  TaskBuilder
addStateStore storeName store processors =
  mempty
    { stores =
        HM.singleton
          storeName
          (wrapStateStore store, HS.fromList processors)
    }

forward ::
  (Typeable k, Typeable v) =>
  Record k v ->
  RIO TaskContext ()
forward record = do
  ctx <- ask
  curProcessorName <- readIORef $ curProcessor ctx
  logDebug $ "enter forward, curProcessor is " <> display curProcessorName
  let taskInfo = taskConfig ctx
  let tplgy = taskTopologyForward taskInfo
  let (_, children) = tplgy HM'.! curProcessorName
  for_ children $ \cname -> do
    logDebug $ "forward to child: " <> display cname
    writeIORef (curProcessor ctx) cname
    let (eProcessor, _) = tplgy HM'.! cname
    runEP eProcessor (mkERecord record)

getKVStateStore ::
  (Typeable k, Typeable v, Ord k) =>
  T.Text ->
  RIO TaskContext (EKVStore k v)
getKVStateStore storeName = do
  ctx <- ask
  curProcessorName <- readIORef $ curProcessor ctx
  logDebug $ display curProcessorName <> " ready to get state store " <> display storeName
  let taskInfo = taskConfig ctx
  case HM.lookup storeName (taskStores taskInfo) of
    Just (stateStore, processors) ->
      if HS.member curProcessorName processors
        then return $ fromEStateStoreToKVStore stateStore
        else error "no state store found"
    Nothing -> error "no state store found"

getSessionStateStore ::
  (Typeable k, Typeable v, Ord k) =>
  T.Text ->
  RIO TaskContext (ESessionStore k v)
getSessionStateStore storeName = do
  ctx <- ask
  curProcessorName <- readIORef $ curProcessor ctx
  logDebug $ display curProcessorName <> " ready to get state store " <> display storeName
  let taskInfo = taskConfig ctx
  case HM.lookup storeName (taskStores taskInfo) of
    Just (stateStore, processors) ->
      if HS.member curProcessorName processors
        then return $ fromEStateStoreToSessionStore stateStore
        else error "no state store found"
    Nothing -> error "no state store found"

getTimestampedKVStateStore ::
  (Typeable k, Typeable v, Ord k) =>
  T.Text ->
  RIO TaskContext (ETimestampedKVStore k v)
getTimestampedKVStateStore storeName = do
  ctx <- ask
  curProcessorName <- readIORef $ curProcessor ctx
  logDebug $ display curProcessorName <> " ready to get state store " <> display storeName
  let taskInfo = taskConfig ctx
  case HM.lookup storeName (taskStores taskInfo) of
    Just (stateStore, processors) ->
      if HS.member curProcessorName processors
        then return $ fromEStateStoreToTimestampedKVStore stateStore
        else error "no state store found"
    Nothing -> error "no state store found"
