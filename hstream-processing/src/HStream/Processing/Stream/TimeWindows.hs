{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.TimeWindows
  ( TimeWindow (..),
    TimeWindows (..),
    TimeWindowKey (..),
    timeWindowKeySerde,
    timeWindowKeySerializer,
    timeWindowKeyDeserializer,
    mkTimeWindow,
    mkTimeWindowKey,
    mkTumblingWindow,
    mkHoppingWindow,
  )
where

import           HStream.Processing.Encoding
import           RIO

data TimeWindows = TimeWindows
  { twSizeMs    :: Int64,
    twAdvanceMs :: Int64,
    twGraceMs   :: Int64
  }

mkTumblingWindow :: Int64 -> TimeWindows
mkTumblingWindow windowSize =
  TimeWindows
    { twSizeMs = windowSize,
      twAdvanceMs = windowSize,
      twGraceMs = 24 * 3600 * 1000
    }

mkHoppingWindow :: Int64 -> Int64 -> TimeWindows
mkHoppingWindow windowSize stepSize =
  TimeWindows
    { twSizeMs = windowSize,
      twAdvanceMs = stepSize,
      twGraceMs = 24 * 3600 * 1000
    }

data TimeWindow = TimeWindow
  { tWindowStart :: Int64,
    tWindowEnd   :: Int64
  }

instance Show TimeWindow where
  show TimeWindow {..} = "[" ++ show tWindowStart ++ ", " ++ show tWindowEnd ++ "]"

mkTimeWindow :: Int64 -> Int64 -> TimeWindow
mkTimeWindow startTs endTs =
  TimeWindow
    { tWindowStart = startTs,
      tWindowEnd = endTs
    }

data TimeWindowKey k = TimeWindowKey
  { twkKey    :: k,
    twkWindow :: TimeWindow
  }

instance (Show k) => Show (TimeWindowKey k) where
  show TimeWindowKey {..} = "key: " ++ show twkKey ++ ", window: " ++ show twkWindow

timeWindowKeySerializer ::
  (Serialized s) =>
  Serializer k s ->
  Serializer TimeWindow s ->
  Serializer (TimeWindowKey k) s
timeWindowKeySerializer kSerializer winSerializer = Serializer $ \TimeWindowKey {..} ->
  let keySer = runSer kSerializer twkKey
      winSer = runSer winSerializer twkWindow
   in compose (winSer, keySer)

timeWindowKeyDeserializer ::
  (Serialized s) =>
  Deserializer k s ->
  Deserializer TimeWindow s ->
  Int64 ->
  Deserializer (TimeWindowKey k) s
timeWindowKeyDeserializer kDeserializer winDeserializer windowSize = Deserializer $ \s ->
  let (winSer, keySer) = separate s
      win = runDeser winDeserializer winSer
      key = runDeser kDeserializer keySer
      winStart = tWindowStart win
   in TimeWindowKey
        { twkKey = key,
          twkWindow = win {tWindowStart = winStart, tWindowEnd = winStart + windowSize}
        }

timeWindowKeySerde ::
  (Serialized s) =>
  Serde k s ->
  Serde TimeWindow s ->
  Int64 ->
  Serde (TimeWindowKey k) s
timeWindowKeySerde kSerde winSerde windowSize =
  Serde
    { serializer = timeWindowKeySerializer (serializer kSerde) (serializer winSerde),
      deserializer = timeWindowKeyDeserializer (deserializer kSerde) (deserializer winSerde) windowSize
    }

mkTimeWindowKey ::
  (Typeable k) =>
  k ->
  TimeWindow ->
  TimeWindowKey k
mkTimeWindowKey key window =
  TimeWindowKey
    { twkKey = key,
      twkWindow = window
    }
