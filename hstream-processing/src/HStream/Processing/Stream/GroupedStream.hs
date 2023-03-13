{-# LANGUAGE GADTs             #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.Processing.Stream.GroupedStream
  ( GroupedStream (..),
    aggregate,
    count,
    timeWindowedBy,
    sessionWindowedBy,
  )
where

import qualified Data.Aeson                                      as Aeson
import           Data.Maybe
import           HStream.Processing.Encoding
import           HStream.Processing.Processor
import           HStream.Processing.Processor.ChangeLog
import           HStream.Processing.Processor.Internal
import           HStream.Processing.Store
import           HStream.Processing.Stream.Internal
import           HStream.Processing.Stream.SessionWindowedStream (SessionWindowedStream (..))
import           HStream.Processing.Stream.SessionWindows
import           HStream.Processing.Stream.TimeWindowedStream    (TimeWindowedStream (..))
import           HStream.Processing.Stream.TimeWindows
import           HStream.Processing.Table
import           RIO
import qualified RIO.ByteString.Lazy                             as BL
import qualified RIO.Text                                        as T

data GroupedStream k v s = GroupedStream
  { gsKeySerde        :: Maybe (Serde k s),
    gsValueSerde      :: Maybe (Serde v s),
    gsProcessorName   :: T.Text,
    gsInternalBuilder :: InternalStreamBuilder
  }

aggregate ::
  (Typeable k, Typeable v, Ord k, Typeable a, Ord s1, Typeable s1, Ord s2, Typeable s2, Aeson.ToJSON s1) =>
  a ->
  (a -> Record k v -> a) ->
  (a -> k -> a) ->
  Serde k s2 ->
  Serde a s2 ->
  Materialized k a s1 ->
  GroupedStream k v s2 ->
  IO (Table k a s2)
aggregate initialValue aggF outputF kSerde2 aSerde2 Materialized {..} GroupedStream {..} = do
  processorName <- mkInternalProcessorName "STREAM-AGGREGATE-" gsInternalBuilder
  let storeName = mkInternalStoreName processorName
  let p = aggregateProcessor storeName initialValue aggF outputF mKeySerde mValueSerde
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
  (Typeable k, Typeable v, Ord k, Ord s, Typeable s, Aeson.ToJSON s) =>
  Materialized k Int s ->
  Serde k s ->
  Serde Int s ->
  GroupedStream k v s ->
  IO (Table k Int s)
count materialized kSerde intSerde = aggregate 0 aggF const kSerde intSerde materialized
  where
    aggF :: Int -> Record k v -> Int
    aggF acc _ = acc + 1

aggregateProcessor ::
  (Typeable k, Typeable v, Ord k, Typeable a, Ord s, Typeable s, Aeson.ToJSON s) =>
  T.Text ->
  a ->
  (a -> Record k v -> a) ->
  (a -> k -> a) ->
  Serde k s ->
  Serde a s ->
  Processor k v
aggregateProcessor storeName initialValue aggF outputF keySerde accSerde = Processor $ \r -> do
  TaskContext{..} <- ask
  store <- getKVStateStore storeName
  let key = fromJust $ recordKey r
      sKey = runSer (serializer keySerde) key
  ma <- liftIO $ ksGet sKey store
  let acc = maybe initialValue (runDeser $ deserializer accSerde) ma
  let newAcc = aggF acc r
  let sNewAcc = runSer (serializer accSerde) newAcc
  liftIO $ ksPut sKey sNewAcc store
  let changeLog = CLKSPut @_ @_ @BL.ByteString storeName sKey sNewAcc
  liftIO $ logChangelog tcChangeLogger (Aeson.encode changeLog)
  forward r {recordValue = outputF newAcc key}

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
