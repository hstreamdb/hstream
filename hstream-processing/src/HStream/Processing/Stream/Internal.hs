{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.Internal
  ( InternalStreamBuilder (..),
    Stream (..),
    mkStream,
    mkInternalStreamBuilder,
    mkInternalProcessorName,
    mkInternalStoreName,
    addSourceInternal,
    addProcessorInternal,
    addSinkInternal,
    addStateStoreInternal,
    mergeInternalStreamBuilder,
  )
where

import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Store
import           RIO
import qualified RIO.ByteString.Lazy          as BL
import qualified RIO.Text                     as T

data Stream k v s = Stream
  { streamKeySerde        :: Maybe (Serde k s),
    streamValueSerde      :: Maybe (Serde v s),
    streamProcessorName   :: T.Text,
    streamInternalBuilder :: InternalStreamBuilder
  }

mkStream ::
  (Typeable k, Typeable v) =>
  Maybe (Serde k s) ->
  Maybe (Serde v s) ->
  T.Text ->
  InternalStreamBuilder ->
  Stream k v s
mkStream keySerde valueSerde processorName builder =
  Stream
    { streamKeySerde = keySerde,
      streamValueSerde = valueSerde,
      streamProcessorName = processorName,
      streamInternalBuilder = builder
    }

data InternalStreamBuilder = InternalStreamBuilder
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
  (Typeable k, Typeable v, Typeable s) =>
  SourceConfig k v s ->
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
  (Typeable k, Typeable v, Typeable s) =>
  SinkConfig k v s ->
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

mkInternalProcessorName :: T.Text -> InternalStreamBuilder -> IO T.Text
mkInternalProcessorName namePrefix InternalStreamBuilder {..} = do
  index <- atomicModifyIORef' isbProcessorId (\x -> (x+1, x))
  return $ namePrefix `T.append` T.pack (show index)

mkInternalStoreName :: T.Text -> T.Text
mkInternalStoreName namePrefix =
  namePrefix `T.append` "-STORE"
