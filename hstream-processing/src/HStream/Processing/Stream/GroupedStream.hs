{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.GroupedStream
  ( GroupedStream (..),
    aggregate,
    count,
    timeWindowedBy,
    sessionWindowedBy,
  )
where

import           Data.Maybe
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Store
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.SessionWindowedStream (SessionWindowedStream (..))
import           HStream.Processing.Stream.SessionWindows
import           HStream.Processing.Stream.TimeWindowedStream    (TimeWindowedStream (..))
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Table
import           RIO
import qualified RIO.Text                                        as T

data GroupedStream k v s = GroupedStream
  { gsKeySerde :: Maybe (Serde k s),
    gsValueSerde :: Maybe (Serde v s),
    gsProcessorName :: T.Text,
    gsInternalBuilder :: InternalStreamBuilder
  }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a, Ord s1, Typeable s1, Ord s2, Typeable s2) =>
  a ->
  (a -> Record k v -> a) ->
  Serde k s2 ->
  Serde a s2 ->
  Materialized k a s1 ->
  GroupedStream k v s2 ->
  IO (Table k a s2)
aggregate initialValue aggF kSerde2 aSerde2 Materialized {..} GroupedStream {..} = do
  processorName <- mkInternalProcessorName "STREAM-AGGREGATE-" gsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF mKeySerde mValueSerde
  let builder' = addProcessorInternal processorName p [gsProcessorName] gsInternalBuilder
  let newBuilder = addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = Just kSerde2,
        tableValueSerde = Just aSerde2,
        tableStoreName = storeName
      }

count ::
  (Typeable k, Typeable v, Ord k, Ord s, Typeable s) =>
  Materialized k Int s ->
  Serde k s ->
  Serde Int s ->
  GroupedStream k v s ->
  IO (Table k Int s)
count materialized kSerde intSerde = aggregate 0 aggF kSerde intSerde materialized
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1

aggregateProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a, Ord s, Typeable s) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  Serde k s ->
  Serde a s ->
  Processor k v
aggregateProcessor storeName initialValue aggF keySerde accSerde = Processor $ \r -> do
  store <- getKVStateStore storeName
  let key = runSer (serializer keySerde) (fromJust $ recordKey r)
  ma <- liftIO $ ksGet key store
  let acc = maybe initialValue (runDeser $ deserializer accSerde) ma
  let newAcc = aggF acc r
  let sNewAcc = runSer (serializer accSerde) newAcc
  liftIO $ ksPut key sNewAcc store
  forward r {recordValue = newAcc}

timeWindowedBy ::
  (Typeable k, Typeable v) =>
  TimeWindows ->
  GroupedStream k v s ->
  IO (TimeWindowedStream k v s)
timeWindowedBy timeWindows GroupedStream {..} =
  return $
    TimeWindowedStream
      { twsKeySerde = gsKeySerde,
        twsValueSerde = gsValueSerde,
        twsProcessorName = gsProcessorName,
        twsTimeWindows = timeWindows,
        twsInternalBuilder = gsInternalBuilder
      }

sessionWindowedBy ::
  (Typeable k, Typeable v) =>
  SessionWindows ->
  GroupedStream k v s ->
  IO (SessionWindowedStream k v s)
sessionWindowedBy sessionWindows GroupedStream {..} =
  return $
    SessionWindowedStream
      { swsKeySerde = gsKeySerde,
        swsValueSerde = gsValueSerde,
        swsProcessorName = gsProcessorName,
        swsSessionWindows = sessionWindows,
        swsInternalBuilder = gsInternalBuilder
      }
