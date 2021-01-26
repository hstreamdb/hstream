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

import           Data.Binary.Get
import qualified Data.ByteString.Builder     as BB
import           HStream.Processing.Encoding
import           RIO

data TimeWindows
  = TimeWindows
      { twSizeMs :: Int64,
        twAdvanceMs :: Int64,
        twGraceMs :: Int64
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

data TimeWindow
  = TimeWindow
      { tWindowStart :: Int64,
        tWindowEnd :: Int64
      }

instance Show TimeWindow where
  show TimeWindow {..} = "[" ++ show tWindowStart ++ ", " ++ show tWindowEnd ++ "]"

mkTimeWindow :: Int64 -> Int64 -> TimeWindow
mkTimeWindow startTs endTs =
  TimeWindow
    { tWindowStart = startTs,
      tWindowEnd = endTs
    }

data TimeWindowKey k
  = TimeWindowKey
      { twkKey :: k,
        twkWindow :: TimeWindow
      }

instance (Show k) => Show (TimeWindowKey k) where
  show TimeWindowKey {..} = "key: " ++ show twkKey ++ ", window: " ++ show twkWindow

timeWindowKeySerializer :: Serializer k -> Serializer (TimeWindowKey k)
timeWindowKeySerializer kSerializer = Serializer $ \TimeWindowKey {..} ->
  let keyBytes = runSer kSerializer twkKey
      bytesBuilder = BB.int64BE (tWindowStart twkWindow) <> BB.lazyByteString keyBytes
   in BB.toLazyByteString bytesBuilder

-- 为了反序列化出 TimeWindow,
-- 还需要传入 WindowSize,
-- 因为序列化的时候仅仅传入了 windowStartTimestamp.
timeWindowKeyDeserializer :: Deserializer k -> Int64 -> Deserializer (TimeWindowKey k)
timeWindowKeyDeserializer kDeserializer windowSize = Deserializer $ runGet decodeWindowKey
  where
    decodeWindowKey = do
      startTs <- getInt64be
      keyBytes <- getRemainingLazyByteString
      return
        TimeWindowKey
          { twkKey = runDeser kDeserializer keyBytes,
            twkWindow = mkTimeWindow startTs (startTs + windowSize)
          }

timeWindowKeySerde :: Serde k -> Int64 -> Serde (TimeWindowKey k)
timeWindowKeySerde kSerde windowSize =
  Serde
    { serializer = timeWindowKeySerializer $ serializer kSerde,
      deserializer = timeWindowKeyDeserializer (deserializer kSerde) windowSize
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
