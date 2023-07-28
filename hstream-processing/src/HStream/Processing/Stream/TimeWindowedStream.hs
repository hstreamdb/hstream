{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.Processing.Stream.TimeWindowedStream
  ( TimeWindowedStream (..),
    aggregate,
    count,
  )
where

import qualified Data.Aeson                             as Aeson
import           Data.Default
import           Data.Maybe
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Processor.ChangeLog
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Table
import           RIO
import qualified RIO.ByteString.Lazy                    as BL
import qualified RIO.Text                               as T

data TimeWindowedStream k v s = TimeWindowedStream
  { twsKeySerde        :: Maybe (Serde k s),
    twsValueSerde      :: Maybe (Serde v s),
    twsProcessorName   :: T.Text,
    twsTimeWindows     :: TimeWindows,
    twsInternalBuilder :: InternalStreamBuilder
  }

aggregate ::
  (Default k, Typeable k, Typeable v, Ord k, Typeable a, Ord s1, Ord s2, Typeable s1, Serialized s1, Typeable s2, Serialized s2, Aeson.ToJSON s1) =>
  a ->
  (a -> Record k v -> a) ->
  (a -> k -> TimeWindow -> a) ->
  Serde TimeWindow s1 ->
  Serde TimeWindow s2 ->
  Serde a s2 ->
  Materialized k a s1 ->
  TimeWindowedStream k v s2 ->
  IO (Table (TimeWindowKey k) a s2)
aggregate initialValue aggF outputF twSerde1 twSerde2 aSerde2 Materialized {..} TimeWindowedStream {..} = do
  processorName <- mkInternalProcessorName "TIME-WINDOWED-STREAM-AGGREGATE-" twsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF outputF mKeySerde mValueSerde twSerde1 twsTimeWindows
  let builder' = addProcessorInternal processorName p [twsProcessorName] twsInternalBuilder
  let newBuilder = addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = case twsKeySerde of
          Nothing -> Nothing
          Just serde -> Just $ timeWindowKeySerde serde twSerde2 (twSizeMs twsTimeWindows),
        tableValueSerde = Just aSerde2,
        tableStoreName = storeName
      }

count ::
  (Default k, Typeable k, Typeable v, Ord k, Ord s, Typeable s, Serialized s, Aeson.ToJSON s) =>
  Materialized k Int s ->
  Serde TimeWindow s ->
  Serde TimeWindow s ->
  Serde Int s ->
  TimeWindowedStream k v s ->
  IO (Table (TimeWindowKey k) Int s)
count mat twSerde1 twSerde2 intSerde = aggregate 0 aggF (\ v _ _ -> v) twSerde1 twSerde2 intSerde mat
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1

aggregateProcessor ::
  (Default k, Typeable k, Typeable v, Ord k, Typeable a, Ord s, Typeable s, Serialized s, Aeson.ToJSON s) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  (a -> k -> TimeWindow -> a) ->
  Serde k s ->
  Serde a s ->
  Serde TimeWindow s ->
  TimeWindows ->
  Processor k v
aggregateProcessor storeName initialValue aggF outputF keySerde accSerde twSerde windows@TimeWindows {..} = Processor $ \r@Record {..} -> do
  ctx@TaskContext{..} <- ask
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
            let key = runSer (timeWindowKeySerializer (serializer keySerde) (serializer twSerde)) windowKey
            ma <- liftIO $ ksGet key store
            let acc = maybe initialValue (runDeser $ deserializer accSerde) ma
            let newAcc = aggF acc r
            let sNewAcc = runSer (serializer accSerde) newAcc
            liftIO $ ksPut key sNewAcc store
            let changeLog = CLKSPut @_ @_ @BL.ByteString storeName key sNewAcc
            liftIO $ logChangelog tcChangeLogger (Aeson.encode changeLog)
            -- Erase key info because we may meet further joins and
            -- joining relies on'same key' to work.
            forward r {recordKey = Just (def `asTypeOf` windowKey), recordValue = outputF newAcc (fromJust recordKey) tw}
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
