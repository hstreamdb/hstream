{-# LANGUAGE GADTs             #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.Processing.Stream.SessionWindowedStream
  ( SessionWindowedStream (..),
    aggregate,
    count,
  )
where

import qualified Data.Aeson                               as Aeson
import           Data.Maybe
import           Data.Typeable
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Processor.ChangeLog
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.SessionWindows
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Table
import           RIO
import qualified RIO.ByteString.Lazy                      as BL
import qualified RIO.Text                                 as T

data SessionWindowedStream k v s = SessionWindowedStream
  { swsKeySerde        :: Maybe (Serde k s),
    swsValueSerde      :: Maybe (Serde v s),
    swsProcessorName   :: T.Text,
    swsSessionWindows  :: SessionWindows,
    swsInternalBuilder :: InternalStreamBuilder
  }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a, Ord s1, Typeable s1, Serialized s1, Ord s2, Typeable s2, Serialized s2, Aeson.ToJSON s1)  =>
  a ->
  (a -> Record k v -> a) ->
  (k -> a -> a -> a) ->
  (a -> k -> a) ->
  Serde TimeWindow s2 ->
  Serde a s2 ->
  Materialized k a s1 ->
  SessionWindowedStream k v s2 ->
  IO (Table (TimeWindowKey k) a s2)
aggregate initialValue aggF sessionMergeF outputF twSerde2 aSerde2 Materialized {..} SessionWindowedStream {..} = do
  processorName <- mkInternalProcessorName "SESSION-WINDOWED-STREAM-AGGREGATE-" swsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF sessionMergeF outputF mKeySerde mValueSerde swsSessionWindows
  let builder' = addProcessorInternal processorName p [swsProcessorName] swsInternalBuilder
  let newBuilder = addStateStoreInternal storeName mStateStore [processorName] builder'
  return
    Table
      { tableInternalBuilder = newBuilder,
        tableProcessorName = processorName,
        tableKeySerde = case swsKeySerde of
          Nothing    -> Nothing
          Just serde -> Just $ sessionWindowKeySerde serde twSerde2,
        tableValueSerde = Just aSerde2,
        tableStoreName = storeName
      }

count ::
  (Typeable k, Typeable v, Ord k, Ord s, Typeable s, Serialized s, Aeson.ToJSON s) =>
  Materialized k Int s ->
  Serde TimeWindow s ->
  Serde Int s ->
  SessionWindowedStream k v s ->
  IO (Table (TimeWindowKey k) Int s)
count mat twSerde intSerde = aggregate 0 aggF sessionMergeF const twSerde intSerde mat
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1
    sessionMergeF :: k -> Int -> Int -> Int
    sessionMergeF _ acc1 acc2 = acc1 + acc2

aggregateProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a, Ord s, Typeable s, Aeson.ToJSON s) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  (k -> a -> a -> a) ->
  (a -> k -> a) ->
  Serde k s ->
  Serde a s ->
  SessionWindows ->
  Processor k v
aggregateProcessor storeName initialValue aggF sessionMergeF outputF keySerde accSerde SessionWindows {..} = Processor $ \r@Record {..} -> do
  TaskContext{..} <- ask
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
      let changeLog = CLSSPut @_ @_ @BL.ByteString storeName newSession newAccBytes
      liftIO $ logChangelog tcChangeLogger (Aeson.encode changeLog)
      forward r {recordKey = Just newSession {twkKey = rk}, recordValue = outputF newAcc rk}
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
              let changeLog = CLSSRemove @_ @() @BL.ByteString storeName curWindowKey
              liftIO $ logChangelog tcChangeLogger (Aeson.encode changeLog)
              logDebug $ "removed session window: " <> displayShow (twkWindow curWindowKey)
              return (newWindowKey, runSer (serializer accSerde) newValue)
          )
          (mkTimeWindowKey rkBytes (mkTimeWindow recordTimestamp recordTimestamp), runSer (serializer accSerde) (aggF initialValue r))
          overlappedSessions
      liftIO $ ssPut mergedWindowKey mergedAccBytes store
      let changeLog = CLSSPut @_ @_ @BL.ByteString storeName mergedWindowKey mergedAccBytes
      liftIO $ logChangelog tcChangeLogger (Aeson.encode changeLog)
      logDebug $ "last merged session window: " <> displayShow (twkWindow mergedWindowKey)
      forward r {recordKey = Just mergedWindowKey {twkKey = rk}, recordValue = outputF (runDeser (deserializer accSerde) mergedAccBytes) rk}
