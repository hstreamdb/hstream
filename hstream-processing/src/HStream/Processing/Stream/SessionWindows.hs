{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Stream.SessionWindows
  ( SessionWindows (..),
    mkSessionWindows,
    SessionWindowKey,
    sessionWindowKeySerializer,
    sessionWindowKeyDeserializer,
    sessionWindowKeySerde,
  )
where

import           HStream.Processing.Encoding
import           HStream.Processing.Stream.TimeWindows
import           RIO

data SessionWindows = SessionWindows
  { swInactivityGap :: Int64,
    swGraceMs :: Int64
  }

mkSessionWindows :: Int64 -> SessionWindows
mkSessionWindows inactivityGap =
  SessionWindows
    { swInactivityGap = inactivityGap,
      swGraceMs = 24 * 3600 * 1000
    }

type SessionWindowKey k = TimeWindowKey k

sessionWindowKeySerializer :: Serialized s =>
  Serializer k s -> Serializer TimeWindow s -> Serializer (TimeWindowKey k) s
sessionWindowKeySerializer kSerializer winSerializer = Serializer $ \TimeWindowKey{..} ->
  let keySer = runSer kSerializer twkKey
      winSer = runSer winSerializer twkWindow
   in compose (winSer, keySer)

sessionWindowKeyDeserializer :: Serialized s =>
  Deserializer k s -> Deserializer TimeWindow s -> Deserializer (TimeWindowKey k) s
sessionWindowKeyDeserializer kDeserializer winDeserializer = Deserializer $ \s ->
  let (winSer, keySer) = separate s
      win = runDeser winDeserializer winSer
      key = runDeser kDeserializer   keySer
   in TimeWindowKey
      { twkKey = key
      , twkWindow = win
      }

sessionWindowKeySerde :: Serialized s =>
  Serde k s -> Serde TimeWindow s -> Serde (TimeWindowKey k) s
sessionWindowKeySerde kSerde winSerde =
  Serde
    { serializer = sessionWindowKeySerializer (serializer kSerde) (serializer winSerde),
      deserializer = sessionWindowKeyDeserializer (deserializer kSerde) (deserializer winSerde)
    }
