{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.Internal
  ( InternalStreamBuilder (..),
    Stream (..),
    Materialized (..),
    mkStream,
    mkInternalStreamBuilder,
    mkInternalProcessorName,
    mkInternalStoreName,
    addSourceInternal,
    addProcessorInternal,
    addSinkInternal,
    addStateStoreInternal,
    buildInternal,
    mergeInternalStreamBuilder,
  )
where

import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           RIO
import qualified RIO.ByteString.Lazy                   as BL
import qualified RIO.Text                              as T

data Stream k v
  = Stream
      { streamKeySerde :: Maybe (Serde k),
        streamValueSerde :: Maybe (Serde v),
        streamProcessorName :: T.Text,
        streamInternalBuilder :: InternalStreamBuilder
      }

mkStream ::
  (Typeable k, Typeable v) =>
  Maybe (Serde k) ->
  Maybe (Serde v) ->
  T.Text ->
  InternalStreamBuilder ->
  Stream k v
mkStream keySerde valueSerde processorName builder =
  Stream
    { streamKeySerde = keySerde,
      streamValueSerde = valueSerde,
      streamProcessorName = processorName,
      streamInternalBuilder = builder
    }

data InternalStreamBuilder
  = InternalStreamBuilder
      { isbTaskBuilder :: TaskBuilder,
        isbProcessorId :: IORef Int
      }

mkInternalStreamBuilder :: TaskBuilder -> IO InternalStreamBuilder
mkInternalStreamBuilder taskBuilder = do
  index <- newIORef 0
  return
    InternalStreamBuilder
      { isbTaskBuilder = taskBuilder,
        isbProcessorId = index
      }

mergeInternalStreamBuilder :: InternalStreamBuilder -> InternalStreamBuilder -> InternalStreamBuilder
mergeInternalStreamBuilder builder1 builder2 =
  InternalStreamBuilder
    { isbTaskBuilder = (isbTaskBuilder builder1) <> (isbTaskBuilder builder2),
      isbProcessorId = isbProcessorId builder1
    }

addSourceInternal ::
  (Typeable k, Typeable v) =>
  SourceConfig k v ->
  InternalStreamBuilder ->
  InternalStreamBuilder
addSourceInternal sourceCfg builder@InternalStreamBuilder {..} =
  let taskBuilder = isbTaskBuilder <> addSource sourceCfg
   in builder {isbTaskBuilder = taskBuilder}

addProcessorInternal ::
  (Typeable k, Typeable v) =>
  T.Text ->
  Processor k v ->
  [T.Text] ->
  InternalStreamBuilder ->
  InternalStreamBuilder
addProcessorInternal processorName processor parents builder@InternalStreamBuilder {..} =
  let taskBuilder = isbTaskBuilder <> addProcessor processorName processor parents
   in builder {isbTaskBuilder = taskBuilder}

addSinkInternal ::
  (Typeable k, Typeable v) =>
  SinkConfig k v ->
  [T.Text] ->
  InternalStreamBuilder ->
  InternalStreamBuilder
addSinkInternal sinkCfg parents builder@InternalStreamBuilder {..} =
  let taskBuilder = isbTaskBuilder <> addSink sinkCfg parents
   in builder {isbTaskBuilder = taskBuilder}

addStateStoreInternal ::
  (Typeable k, Typeable v, Ord k) =>
  T.Text ->
  StateStore k v ->
  [T.Text] ->
  InternalStreamBuilder ->
  InternalStreamBuilder
addStateStoreInternal storeName store processors builder@InternalStreamBuilder {..} =
  let taskBuilder = isbTaskBuilder <> addStateStore storeName store processors
   in builder {isbTaskBuilder = taskBuilder}

buildInternal :: InternalStreamBuilder -> Task
buildInternal InternalStreamBuilder {..} = build isbTaskBuilder

mkInternalProcessorName :: T.Text -> InternalStreamBuilder -> IO T.Text
mkInternalProcessorName namePrefix InternalStreamBuilder {..} = do
  index <- readIORef isbProcessorId
  writeIORef isbProcessorId (index + 1)
  return $ namePrefix `T.append` T.pack (show index)

mkInternalStoreName :: T.Text -> T.Text
mkInternalStoreName namePrefix =
  namePrefix `T.append` "-STORE"

data Materialized k v
  = Materialized
      { mKeySerde :: Serde k,
        mValueSerde :: Serde v,
        mStateStore :: StateStore BL.ByteString BL.ByteString
        -- mStateStore :: StateStore k v
      }
