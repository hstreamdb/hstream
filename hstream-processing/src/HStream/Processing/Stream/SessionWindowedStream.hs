{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.SessionWindowedStream
  ( SessionWindowedStream (..),
    aggregate,
    count,
  )
where

import           Data.Maybe
import           Data.Typeable
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Store
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.SessionWindows
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Table
import           RIO
import qualified RIO.Text                                 as T

data SessionWindowedStream k v
  = SessionWindowedStream
      { swsKeySerde :: Maybe (Serde k),
        swsValueSerde :: Maybe (Serde v),
        swsProcessorName :: T.Text,
        swsSessionWindows :: SessionWindows,
        swsInternalBuilder :: InternalStreamBuilder
      }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  a ->
  (a -> Record k v -> a) ->
  (k -> a -> a -> a) ->
  Materialized k a ->
  SessionWindowedStream k v ->
  IO (Table (TimeWindowKey k) a)
aggregate initialValue aggF sessionMergeF Materialized {..} SessionWindowedStream {..} = do
  processorName <- mkInternalProcessorName "SESSION-WINDOWED-STREAM-AGGREGATE-" swsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF sessionMergeF mKeySerde mValueSerde swsSessionWindows
  let builder' = addProcessorInternal processorName p [swsProcessorName] swsInternalBuilder
  let newBuilder = addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = Just (sessionWindowKeySerde mKeySerde),
        tableValueSerde = Just mValueSerde,
        tableStoreName = storeName
      }

count ::
  (Typeable k, Typeable v, Ord k) =>
  Materialized k Int ->
  SessionWindowedStream k v ->
  IO (Table (TimeWindowKey k) Int)
count = aggregate 0 aggF sessionMergeF
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1
    sessionMergeF :: k -> Int -> Int -> Int
    sessionMergeF _ acc1 acc2 = acc1 + acc2

aggregateProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  (k -> a -> a -> a) ->
  Serde k ->
  Serde a ->
  SessionWindows ->
  Processor k v
aggregateProcessor storeName initialValue aggF sessionMergeF keySerde accSerde SessionWindows {..} = Processor $ \r@Record {..} -> do
  store <- getSessionStateStore storeName
  logDebug $ "recordTimestamp: " <> displayShow recordTimestamp
  let rk = fromJust recordKey
  let rkBytes = runSer (serializer keySerde) rk
  overlappedSessions <- liftIO $ findSessions rkBytes (recordTimestamp - swInactivityGap) (recordTimestamp + swInactivityGap) store
  logDebug $ "overlappedSessions: " <> displayShow (length overlappedSessions)
  if null overlappedSessions
    then do
      let newSession = mkTimeWindowKey rkBytes (mkTimeWindow recordTimestamp recordTimestamp)
      let newAcc = aggF initialValue r
      let newAccBytes = runSer (serializer accSerde) newAcc
      liftIO $ ssPut newSession newAccBytes store
      forward r {recordKey = Just newSession {twkKey = rk}, recordValue = newAcc}
    else do
      (mergedWindowKey, mergedAccBytes) <-
        foldM
          ( \(mergedWindowKey, accValueBytes) (curWindowKey, curValueBytes) -> do
              logDebug $ "mergedSessionWindow: " <> displayShow (twkWindow mergedWindowKey)
              logDebug $ "curSessionWindow: " <> displayShow (twkWindow curWindowKey)
              let newStartTime = min (tWindowStart $ twkWindow mergedWindowKey) (tWindowStart $ twkWindow curWindowKey)
              let newEndTime = max (tWindowEnd $ twkWindow mergedWindowKey) (tWindowEnd $ twkWindow curWindowKey)
              let newWindowKey = mergedWindowKey {twkWindow = mkTimeWindow newStartTime newEndTime}
              let accValue = runDeser (deserializer accSerde) accValueBytes
              let curValue = runDeser (deserializer accSerde) curValueBytes
              let newValue = sessionMergeF rk accValue curValue
              liftIO $ ssRemove curWindowKey store
              logDebug $ "removed session window: " <> displayShow (twkWindow curWindowKey)
              return (newWindowKey, runSer (serializer accSerde) newValue)
          )
          (mkTimeWindowKey rkBytes (mkTimeWindow recordTimestamp recordTimestamp), runSer (serializer accSerde) (aggF initialValue r))
          overlappedSessions
      liftIO $ ssPut mergedWindowKey mergedAccBytes store
      logDebug $ "last merged session window: " <> displayShow (twkWindow mergedWindowKey)
      forward r {recordKey = Just mergedWindowKey {twkKey = rk}, recordValue = runDeser (deserializer accSerde) mergedAccBytes}
