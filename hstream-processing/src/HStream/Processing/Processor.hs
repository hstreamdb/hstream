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
    forward,
    getKVStateStore,
    getSessionStateStore,
    getTimestampedKVStateStore,
    getTaskName,
    Record (..),
    Processor (..),
    SourceConfig (..),
    SinkConfig (..),
    TaskBuilder,
  )
where

import           Control.Concurrent
import           Control.Exception                     (throw)
import           Data.Maybe
import           Data.Typeable
import qualified Prelude                               as Prelude
import           RIO
import qualified RIO.ByteString.Lazy                   as BL
import qualified RIO.HashMap                           as HM
import           RIO.HashMap.Partial                   as HM'
import qualified RIO.HashSet                           as HS
import qualified RIO.List                              as L
import qualified RIO.Text                              as T

import qualified HStream.Logger                        as Log
import           HStream.Processing.Connector
import           HStream.Processing.Encoding
import           HStream.Processing.Error              (HStreamError (..))
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           HStream.Processing.Type
import           HStream.Processing.Util
import qualified HStream.Server.HStreamApi             as API

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
    {
      ttcName = taskName
    }

runTask ::
  SourceConnectorWithoutCkp ->
  SinkConnector ->
  TaskBuilder ->
  IO ()
runTask SourceConnectorWithoutCkp {..} sinkConnector taskBuilder@TaskTopologyConfig {..} = do
  -- build and add internalSinkProcessor
  let sinkProcessors = HM.map (buildInternalSinkProcessor sinkConnector) sinkCfgs
  let newTaskBuilder =
        HM.foldlWithKey' (\a k v -> a <> addProcessor k v [T.append k serializerNameSuffix])
                         taskBuilder
                         sinkProcessors

  -- build forward topology
  let task@Task {..} = build newTaskBuilder

  -- runTask
  let sourceStreamNames = HM.keys taskSourceConfig
  logOptions <- logOptionsHandle stderr True
  withLogFunc logOptions $ \lf -> do
    ctx <- buildTaskContext task lf
    let offset = API.SpecialOffsetLATEST
    forM_ sourceStreamNames (flip subscribeToStreamWithoutCkp offset)
    loop task ctx sourceStreamNames []
  where
    runOnePath :: Task -> TaskContext -> T.Text -> IO ()
    runOnePath Task{..} ctx sourceStreamName = withReadRecordsWithoutCkp sourceStreamName $ \sourceRecords -> runRIO ctx $ do
      forM_ sourceRecords $ \r@SourceRecord {..} -> do
        let acSourceName = iSourceName (taskSourceConfig HM'.! srcStream)
        let (sourceEProcessor, _) = taskTopologyForward HM'.! acSourceName
        liftIO $ updateTimestampInTaskContext ctx srcTimestamp
        e' <- try $ runEP sourceEProcessor (mkERecord Record {recordKey = srcKey, recordValue = srcValue, recordTimestamp = srcTimestamp})
        case e' of
          Left (e :: SomeException) -> liftIO $ Log.fatal $ Log.buildString (Prelude.show e)
          Right _                   -> return ()
    loop :: Task -> TaskContext -> [T.Text] -> [Async ()] -> IO ()
    loop task ctx [] asyncs = return ()
    loop task ctx [sourceStreamName] asyncs =
      runOnePath task ctx sourceStreamName `onException` do
        forM_ asyncs cancel
        let sourceStreamNames = HM.keys (taskSourceConfig task)
        forM_ sourceStreamNames unSubscribeToStreamWithoutCkp
    loop task ctx (sourceStreamName:xs) asyncs = do
      withAsync (runOnePath task ctx sourceStreamName) $ \a -> do
        loop task ctx xs (asyncs ++ [a])

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
  Processor s s-- BL.ByteString BL.ByteString
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
  InternalSinkConfig ->
  Processor BL.ByteString BL.ByteString
buildInternalSinkProcessor sinkConnector InternalSinkConfig {..} = Processor $ \Record {..} -> do
  ts <- liftIO getCurrentTimestamp
  liftIO $
    writeRecord
      sinkConnector
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
