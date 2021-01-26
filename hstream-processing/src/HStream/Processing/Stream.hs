{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream
  ( mkStreamBuilder,
    mkStream,
    HStream.Processing.Stream.stream,
    HStream.Processing.Stream.table,
    HStream.Processing.Stream.build,
    HStream.Processing.Stream.to,
    HStream.Processing.Stream.filter,
    HStream.Processing.Stream.map,
    HStream.Processing.Stream.groupBy,
    HStream.Processing.Stream.joinStream,
    HStream.Processing.Stream.joinTable,
    Stream,
    StreamBuilder,
    StreamSourceConfig (..),
    StreamSinkConfig (..),
    GroupedStream,
    Materialized (..),
    StreamJoined (..),
  )
where

import           Data.Maybe
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           HStream.Processing.Stream.GroupedStream
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.JoinWindows
import           HStream.Processing.Table
import           HStream.Processing.Topic
import           HStream.Processing.Type
import           RIO
import qualified RIO.ByteString.Lazy                     as BL
import qualified RIO.Text                                as T

data StreamBuilder
  = StreamBuilder
      { sbInternalBuilder :: InternalStreamBuilder
      }

mkStreamBuilder :: T.Text -> IO StreamBuilder
mkStreamBuilder taskName = do
  internalStreamBuilder <- mkInternalStreamBuilder $ buildTask taskName
  return StreamBuilder {sbInternalBuilder = internalStreamBuilder}

data StreamSourceConfig k v
  = StreamSourceConfig
      { sscTopicName :: TopicName,
        sscKeySerde :: Serde k,
        sscValueSerde :: Serde v
      }

data StreamSinkConfig k v
  = StreamSinkConfig
      { sicTopicName :: TopicName,
        sicKeySerde :: Serde k,
        sicValueSerde :: Serde v
      }

stream ::
  (Typeable k, Typeable v) =>
  StreamSourceConfig k v ->
  StreamBuilder ->
  IO (Stream k v)
stream StreamSourceConfig {..} StreamBuilder {..} = do
  sourceProcessorName <- mkInternalProcessorName (sscTopicName `T.append` "-STREAM-SOURCE-") sbInternalBuilder
  let sourceCfg =
        SourceConfig
          { sourceName = sourceProcessorName,
            sourceTopicName = sscTopicName,
            keyDeserializer = Just $ deserializer sscKeySerde,
            valueDeserializer = deserializer sscValueSerde
          }
  let newBuilder = addSourceInternal sourceCfg sbInternalBuilder
  return
    Stream
      { streamKeySerde = Just sscKeySerde,
        streamValueSerde = Just sscValueSerde,
        streamProcessorName = sourceProcessorName,
        streamInternalBuilder = newBuilder
      }

table ::
  (Typeable k, Typeable v) =>
  StreamSourceConfig k v ->
  StreamBuilder ->
  IO (Table k v)
table StreamSourceConfig {..} StreamBuilder {..} = do
  tableSourceName <- mkInternalProcessorName (sscTopicName `T.append` "-TABLE-SOURCE-") sbInternalBuilder
  tableStore <- mkInMemoryStateKVStore :: IO (StateStore BL.ByteString BL.ByteString)
  let sourceCfg =
        SourceConfig
          { sourceName = tableSourceName,
            sourceTopicName = sscTopicName,
            keyDeserializer = Just $ deserializer sscKeySerde,
            valueDeserializer = deserializer sscValueSerde
          }
  tableStoreProcessorName <- mkInternalProcessorName "TABLE-STORE-" sbInternalBuilder
  let tableSourceStoreName = mkInternalStoreName tableStoreProcessorName
  let tableSourceProcessor = tableStoreProcessor sscKeySerde sscValueSerde tableSourceStoreName
  let newTaskBuilder =
        isbTaskBuilder sbInternalBuilder
          <> addSource sourceCfg
          <> addProcessor tableStoreProcessorName tableSourceProcessor [tableSourceName]
          <> addStateStore tableSourceStoreName tableStore [tableStoreProcessorName]
  return
    Table
      { tableKeySerde = Just sscKeySerde,
        tableValueSerde = Just sscValueSerde,
        tableProcessorName = tableStoreProcessorName,
        tableStoreName = tableSourceStoreName,
        tableInternalBuilder = sbInternalBuilder {isbTaskBuilder = newTaskBuilder}
      }

tableStoreProcessor ::
  (Typeable k, Typeable v) =>
  Serde k ->
  Serde v ->
  T.Text ->
  Processor k v
tableStoreProcessor keySerde valueSerde storeName = Processor $ \r@Record {..} -> do
  store <- getKVStateStore storeName
  let keyBytes = runSer (serializer keySerde) (fromJust recordKey)
  let valueBytes = runSer (serializer valueSerde) recordValue
  liftIO $ ksPut keyBytes valueBytes store
  forward r

to ::
  (Typeable k, Typeable v) =>
  StreamSinkConfig k v ->
  Stream k v ->
  IO StreamBuilder
to StreamSinkConfig {..} Stream {..} = do
  sinkProcessorName <- mkInternalProcessorName (sicTopicName `T.append` "-SINK-") streamInternalBuilder
  let sinkCfg =
        SinkConfig
          { sinkName = sinkProcessorName,
            sinkTopicName = sicTopicName,
            keySerializer = Just $ serializer sicKeySerde,
            valueSerializer = serializer sicValueSerde
          }
  let newBuilder = addSinkInternal sinkCfg [streamProcessorName] streamInternalBuilder
  return $ StreamBuilder {sbInternalBuilder = newBuilder}

build :: StreamBuilder -> Task
build StreamBuilder {..} = buildInternal sbInternalBuilder

filter ::
  (Typeable k, Typeable v) =>
  (Record k v -> Bool) ->
  Stream k v ->
  IO (Stream k v)
filter f s@Stream {..} = do
  name <- mkInternalProcessorName "FILTER-" streamInternalBuilder
  let p = filterProcessor f
  let newBuilder = addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    s
      { streamInternalBuilder = newBuilder,
        streamProcessorName = name
      }

filterProcessor ::
  (Typeable k, Typeable v) =>
  (Record k v -> Bool) ->
  Processor k v
filterProcessor f = Processor $ \r ->
  when (f r) $ forward r

mapProcessor ::
  (Typeable k1, Typeable v1, Typeable k2, Typeable v2) =>
  (Record k1 v1 -> Record k2 v2) ->
  Processor k1 v1
mapProcessor f = Processor $ forward . f

map ::
  (Typeable k1, Typeable v1, Typeable k2, Typeable v2) =>
  (Record k1 v1 -> Record k2 v2) ->
  Stream k1 v1 ->
  IO (Stream k2 v2)
map f s@Stream {..} = do
  name <- mkInternalProcessorName "MAP-" streamInternalBuilder
  let p = mapProcessor f
  let newBuilder = addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    s
      { streamInternalBuilder = newBuilder,
        streamProcessorName = name,
        streamKeySerde = Nothing,
        streamValueSerde = Nothing
      }

groupBy ::
  (Typeable k1, Typeable v1, Typeable k2) =>
  (Record k1 v1 -> k2) ->
  Stream k1 v1 ->
  IO (GroupedStream k2 v1)
groupBy f Stream {..} = do
  name <- mkInternalProcessorName "GROUP-BY-" streamInternalBuilder
  let p = mapProcessor (\r -> r {recordKey = Just $ f r})
  let newBuilder = addProcessorInternal name p [streamProcessorName] streamInternalBuilder
  return
    GroupedStream
      { gsInternalBuilder = newBuilder,
        gsProcessorName = name,
        gsKeySerde = Nothing,
        gsValueSerde = Nothing
      }

data StreamJoined k1 v1 k2 v2
  = StreamJoined
      { sjK1Serde :: Serde k1,
        sjV1Serde :: Serde v1,
        sjK2Serde :: Serde k2,
        sjV2Serde :: Serde v2,
        sjThisStore :: StateStore BL.ByteString BL.ByteString,
        sjOtherStore :: StateStore BL.ByteString BL.ByteString
      }

joinStream ::
  (Typeable k1, Typeable v1, Typeable k2, Typeable v2, Typeable k3, Typeable v3, Eq k3) =>
  Stream k2 v2 ->
  (v1 -> v2 -> v3) ->
  (Record k1 v1 -> k3) ->
  (Record k2 v2 -> k3) ->
  JoinWindows ->
  StreamJoined k1 v1 k2 v2 ->
  Stream k1 v1 ->
  IO (Stream k3 v3)
joinStream otherStream joiner thisKeySelector otherKeySelector JoinWindows {..} StreamJoined {..} thisStream = do
  let mergedStreamBuilder = mergeInternalStreamBuilder (streamInternalBuilder thisStream) (streamInternalBuilder otherStream)
  thisJoinProcessorName <- mkInternalProcessorName "STREAM-JOIN-STREAM-THIS-" mergedStreamBuilder
  let thisJoinStoreName = mkInternalStoreName thisJoinProcessorName
  otherJoinProcessorName <- mkInternalProcessorName "STREAM-JOIN-STREAM-Other-" mergedStreamBuilder
  let otherJoinStoreName = mkInternalStoreName otherJoinProcessorName
  -- ts1 - beforeMs <= ts2 <= ts1 + afterMs
  -- ts2 - afterMs <= ts1 <= ts2 + beforeMs
  let thisJoinProcessor = joinStreamProcessor joiner thisKeySelector otherKeySelector jwBeforeMs jwAfterMs thisJoinStoreName otherJoinStoreName sjK1Serde sjV1Serde sjK2Serde sjV2Serde
  let otherJoinProcessor = joinStreamProcessor (flip joiner) otherKeySelector thisKeySelector jwAfterMs jwBeforeMs otherJoinStoreName thisJoinStoreName sjK2Serde sjV2Serde sjK1Serde sjV1Serde
  mergeProcessorName <- mkInternalProcessorName "PASSTHROUGH-" mergedStreamBuilder
  let mergeProcessor = passThroughProcessor thisStream joiner
  let newTaskBuilder =
        isbTaskBuilder mergedStreamBuilder
          <> addProcessor thisJoinProcessorName thisJoinProcessor [streamProcessorName thisStream]
          <> addProcessor otherJoinProcessorName otherJoinProcessor [streamProcessorName otherStream]
          <> addProcessor mergeProcessorName mergeProcessor [thisJoinProcessorName, otherJoinProcessorName]
          <> addStateStore thisJoinStoreName sjThisStore [thisJoinProcessorName, otherJoinProcessorName]
          <> addStateStore otherJoinStoreName sjOtherStore [thisJoinProcessorName, otherJoinProcessorName]
  return
    Stream
      { streamKeySerde = Nothing,
        streamValueSerde = Nothing,
        streamProcessorName = mergeProcessorName,
        streamInternalBuilder = mergedStreamBuilder {isbTaskBuilder = newTaskBuilder}
      }
  where
    passThroughProcessor ::
      (Typeable k, Typeable v1, Typeable v2, Typeable v3) =>
      Stream k v1 ->
      (v1 -> v2 -> v3) ->
      Processor k v3
    passThroughProcessor _ _ = Processor $ \r ->
      forward r

joinStreamProcessor ::
  (Typeable k1, Typeable v1, Typeable k2, Typeable v2, Typeable k3, Typeable v3, Eq k3) =>
  (v1 -> v2 -> v3) ->
  (Record k1 v1 -> k3) ->
  (Record k2 v2 -> k3) ->
  Int64 ->
  Int64 ->
  Text ->
  Text ->
  Serde k1 ->
  Serde v1 ->
  Serde k2 ->
  Serde v2 ->
  Processor k1 v1
joinStreamProcessor joiner keySelector1 keySelector2 beforeMs afterMs storeName1 storeName2 k1Serde v1Serde k2Serde v2Serde = Processor $ \r1@Record {..} -> do
  store1 <- getTimestampedKVStateStore storeName1
  let k1 = fromJust recordKey
  let k1Bytes = runSer (serializer k1Serde) k1
  let v1Bytes = runSer (serializer v1Serde) recordValue
  liftIO $ tksPut (mkTimestampedKey k1Bytes recordTimestamp) v1Bytes store1
  store2 <- getTimestampedKVStateStore storeName2
  candinates <- liftIO $ tksRange (mkTimestampedKey k1Bytes $ recordTimestamp - beforeMs) (mkTimestampedKey k1Bytes $ recordTimestamp + afterMs) store2
  forM_
    candinates
    ( \(timestampedKey, v2Bytes) -> do
        let k2 = runDeser (deserializer k2Serde) (tkKey timestampedKey)
        let v2 = runDeser (deserializer v2Serde) v2Bytes
        let ts2 = tkTimestamp timestampedKey
        let jk1 = keySelector1 r1
        let jk2 = keySelector2 Record {recordKey = Just k2, recordValue = v2, recordTimestamp = ts2}
        when (jk1 == jk2) $ do
          let v3 = joiner recordValue v2
          forward $ Record {recordKey = Just jk1, recordValue = v3, recordTimestamp = max recordTimestamp ts2}
    )

joinTable ::
  (Typeable k, Typeable v1, Typeable v2, Typeable v3) =>
  Table k v2 ->
  (v1 -> v2 -> v3) ->
  Serde k ->
  Serde v2 ->
  Stream k v1 ->
  IO (Stream k v3)
joinTable joinedTable@Table {..} joiner keySerde v2Serde Stream {..} = do
  let mergedStreamBuilder = mergeInternalStreamBuilder streamInternalBuilder tableInternalBuilder
  joinProcessorName <- mkInternalProcessorName "Stream-JOIN-TABLE-" mergedStreamBuilder
  let joinProcessor = joinTableProcessor joinedTable joiner keySerde v2Serde
  let tableStore = fromJust $ getStateStoreFromTaskBuilder tableStoreName (isbTaskBuilder mergedStreamBuilder)
  let newTaskBuilder =
        isbTaskBuilder mergedStreamBuilder
          <> addProcessor joinProcessorName joinProcessor [streamProcessorName]
          <> addEStateStore tableStoreName tableStore [joinProcessorName]
  return
    Stream
      { streamKeySerde = Just keySerde,
        streamValueSerde = Nothing,
        streamProcessorName = joinProcessorName,
        streamInternalBuilder = mergedStreamBuilder {isbTaskBuilder = newTaskBuilder}
      }

joinTableProcessor ::
  (Typeable k, Typeable v1, Typeable v2, Typeable v3) =>
  Table k v2 ->
  (v1 -> v2 -> v3) ->
  Serde k ->
  Serde v2 ->
  Processor k v1
joinTableProcessor Table {..} joiner keySerde v2Serde = Processor $ \r@Record {..} -> do
  let key = fromJust recordKey
  let keyBytes = runSer (serializer keySerde) key
  tableStore <- getKVStateStore tableStoreName
  mr <- liftIO $ ksGet keyBytes tableStore
  case mr of
    Just v2Bytes -> do
      let v2 = runDeser (deserializer v2Serde) v2Bytes
      let v3 = joiner recordValue v2
      forward r {recordValue = v3}
    Nothing -> return ()
