{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE StrictData        #-}

module HStream.Processing.Type
  ( Timestamp,
    TimestampedKey (..),
    mkTimestampedKey,
  )
where

import           RIO

type Timestamp = Int64

data TimestampedKey k
  = TimestampedKey
      { tkKey :: k,
        tkTimestamp :: Timestamp
      }

mkTimestampedKey :: k -> Timestamp -> TimestampedKey k
mkTimestampedKey key timestamp =
  TimestampedKey
    { tkKey = key,
      tkTimestamp = timestamp
    }
