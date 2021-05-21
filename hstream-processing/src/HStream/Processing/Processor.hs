{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Processor
  ( buildTask,
    build,
    addSource,
    addProcessor,
    addSink,
    addStateStore,
    -- runTask,
    forward,
    getKVStateStore,
    getSessionStateStore,
    getTimestampedKVStateStore,
    Record (..),
    Processor (..),
    SourceConfig (..),
    SinkConfig (..),
    -- TaskConfig (..),
    -- MessageStoreType (..),
  )
where

import           Control.Exception                     (throw)
import           Data.Maybe
import           Data.Typeable
import           HStream.Processing.Encoding
import           HStream.Processing.Error              (HStreamError (..))
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
-- import           HStream.Processing.Util
import           RIO
import qualified RIO.ByteString.Lazy                   as BL
import qualified RIO.HashMap                           as HM
import           RIO.HashMap.Partial                   as HM'
import qualified RIO.HashSet                           as HS
import qualified RIO.List                              as L
import qualified RIO.Text                              as T

-- import qualified Prelude as P

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

validateTopology :: TaskTopologyConfig -> ()
validateTopology TaskTopologyConfig {..} =
  if L.null sourceCfgs
    then throw $ TaskTopologyBuildError "task build error: no valid source config"
    else
      if L.null sinkCfgs
        then throw $ TaskTopologyBuildError "task build error: no valid sink config"
        else ()

data SourceConfig k v = SourceConfig
  { sourceName :: T.Text,
    sourceTopicName :: T.Text,
    keyDeserializer :: Maybe (Deserializer k),
    valueDeserializer :: Deserializer v
  }

data SinkConfig k v = SinkConfig
  { sinkName :: T.Text,
    sinkTopicName :: T.Text,
    keySerializer :: Maybe (Serializer k),
    valueSerializer :: Serializer v
  }

addSource ::
  (Typeable k, Typeable v) =>
  SourceConfig k v ->
  TaskBuilder
addSource cfg@SourceConfig {..} =
  mempty
    { sourceCfgs =
        HM.singleton
          sourceTopicName
          InternalSourceConfig
            { iSourceName = sourceName,
              iSourceTopicName = sourceTopicName
            },
      topology =
        HM.singleton
          sourceName
          (mkEProcessor $ buildSourceProcessor cfg, [])
    }

buildSourceProcessor ::
  (Typeable k, Typeable v) =>
  SourceConfig k v ->
  Processor BL.ByteString BL.ByteString
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
  (Typeable k, Typeable v) =>
  SinkConfig k v ->
  Processor k v
buildSinkProcessor SinkConfig {..} = Processor $ \r@Record {..} -> do
  logDebug "enter sink processor"
  let rk = liftA2 runSer keySerializer recordKey
  let rv = runSer valueSerializer recordValue
  forward r {recordKey = rk, recordValue = rv}

addSink ::
  (Typeable k, Typeable v) =>
  SinkConfig k v ->
  [T.Text] ->
  TaskBuilder
addSink cfg@SinkConfig {..} parentNames =
  mempty
    { topology =
        HM.singleton
          sinkName
          (mkEProcessor $ buildSinkProcessor cfg, parentNames),
      sinkCfgs =
        HM.singleton
          sinkName
          InternalSinkConfig
            { iSinkName = sinkName,
              iSinkTopicName = sinkTopicName
            }
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
