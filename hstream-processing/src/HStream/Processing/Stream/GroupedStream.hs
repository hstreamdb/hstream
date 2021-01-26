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

data GroupedStream k v
  = GroupedStream
      { gsKeySerde :: Maybe (Serde k),
        gsValueSerde :: Maybe (Serde v),
        gsProcessorName :: T.Text,
        gsInternalBuilder :: InternalStreamBuilder
      }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  a ->
  (a -> Record k v -> a) ->
  Materialized k a ->
  GroupedStream k v ->
  IO (Table k a)
aggregate initialValue aggF Materialized {..} GroupedStream {..} = do
  processorName <- mkInternalProcessorName "STREAM-AGGREGATE-" gsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF mKeySerde mValueSerde
  let builder' = addProcessorInternal processorName p [gsProcessorName] gsInternalBuilder
  let newBuilder = addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = Just mKeySerde,
        tableValueSerde = Just mValueSerde,
        tableStoreName = storeName
      }

count ::
  (Typeable k, Typeable v, Ord k) =>
  Materialized k Int ->
  GroupedStream k v ->
  IO (Table k Int)
count materialized groupedStream = aggregate 0 aggF materialized groupedStream
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1

aggregateProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  Serde k ->
  Serde a ->
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
  GroupedStream k v ->
  IO (TimeWindowedStream k v)
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
  GroupedStream k v ->
  IO (SessionWindowedStream k v)
sessionWindowedBy sessionWindows GroupedStream {..} =
  return $
    SessionWindowedStream
      { swsKeySerde = gsKeySerde,
        swsValueSerde = gsValueSerde,
        swsProcessorName = gsProcessorName,
        swsSessionWindows = sessionWindows,
        swsInternalBuilder = gsInternalBuilder
      }
