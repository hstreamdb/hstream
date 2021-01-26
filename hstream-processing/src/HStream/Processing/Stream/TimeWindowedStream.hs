{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.TimeWindowedStream
  ( TimeWindowedStream (..),
    aggregate,
    count,
  )
where

import           Data.Maybe
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Table
import           RIO
import qualified RIO.Text                              as T

data TimeWindowedStream k v
  = TimeWindowedStream
      { twsKeySerde :: Maybe (Serde k),
        twsValueSerde :: Maybe (Serde v),
        twsProcessorName :: T.Text,
        twsTimeWindows :: TimeWindows,
        twsInternalBuilder :: InternalStreamBuilder
      }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  a ->
  (a -> Record k v -> a) ->
  Materialized k a ->
  TimeWindowedStream k v ->
  IO (Table (TimeWindowKey k) a)
aggregate initialValue aggF Materialized {..} TimeWindowedStream {..} = do
  processorName <- mkInternalProcessorName "TIME-WINDOWED-STREAM-AGGREGATE-" twsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF mKeySerde mValueSerde twsTimeWindows
  let builder' = addProcessorInternal processorName p [twsProcessorName] twsInternalBuilder
  let newBuilder = addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = Just (timeWindowKeySerde mKeySerde (twSizeMs twsTimeWindows)),
        tableValueSerde = Just mValueSerde,
        tableStoreName = storeName
      }

count ::
  (Typeable k, Typeable v, Ord k) =>
  Materialized k Int ->
  TimeWindowedStream k v ->
  IO (Table (TimeWindowKey k) Int)
count = aggregate 0 aggF
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
  TimeWindows ->
  Processor k v
aggregateProcessor storeName initialValue aggF keySerde accSerde windows@TimeWindows {..} = Processor $ \r@Record {..} -> do
  ctx <- ask
  store <- getKVStateStore storeName
  logDebug $ "recordTimestamp: " <> displayShow recordTimestamp
  let matchedWindows = windowsFor recordTimestamp windows
  logDebug $ "matchedWindows: " <> displayShow matchedWindows
  observedStreamTime <- liftIO $ getTimestampInTaskContext ctx
  forM_
    matchedWindows
    ( \tw@TimeWindow {..} ->
        if observedStreamTime < tWindowEnd + twGraceMs
          then do
            let windowKey = mkTimeWindowKey (fromJust recordKey) tw
            let key = runSer (timeWindowKeySerializer (serializer keySerde)) windowKey
            ma <- liftIO $ ksGet key store
            let acc = maybe initialValue (runDeser $ deserializer accSerde) ma
            let newAcc = aggF acc r
            let sNewAcc = runSer (serializer accSerde) newAcc
            liftIO $ ksPut key sNewAcc store
            forward r {recordKey = Just windowKey, recordValue = newAcc}
          else logWarn "Skipping record for expired window."
    )

windowsFor :: Int64 -> TimeWindows -> [TimeWindow]
windowsFor timestamp TimeWindows {..} =
  let windowStart = max 0 (timestamp - twSizeMs + twAdvanceMs) `quot` twAdvanceMs * twAdvanceMs
   in addWindow windowStart []
  where
    addWindow :: Int64 -> [TimeWindow] -> [TimeWindow]
    addWindow startTs acc =
      if startTs <= timestamp
        then
          let endTs = startTs + twSizeMs
              newAcc = acc ++ [mkTimeWindow startTs endTs]
           in addWindow (startTs + twAdvanceMs) newAcc
        else acc
