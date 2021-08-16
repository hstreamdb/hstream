{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module HStream.SQL.Codegen.Boilerplate where

import           Data.Aeson
import qualified Data.Aeson                            as Aeson
import qualified Data.Binary                           as B
import           Data.Binary.Get
import qualified Data.ByteString.Builder               as BB
import qualified Data.ByteString.Lazy                  as BL
import qualified Data.HashMap.Strict                   as HM
import           Data.Maybe                            (fromJust)
import           Data.Scientific                       (Scientific (coefficient),
                                                        coefficient, scientific)
import qualified Data.Text.Lazy                        as TL
import qualified Data.Text.Lazy.Encoding               as TLE
import           HStream.Processing.Encoding           (Deserializer (Deserializer),
                                                        Serde (..),
                                                        Serializer (Serializer))
import           HStream.Processing.Stream.TimeWindows
import           RIO                                   (Int64, Void)

textSerde :: Serde TL.Text BL.ByteString
textSerde =
  Serde
  { serializer   = Serializer   TLE.encodeUtf8
  , deserializer = Deserializer TLE.decodeUtf8
  }

objectSerde :: Serde Object BL.ByteString
objectSerde =
  Serde
  { serializer   = Serializer   encode
  , deserializer = Deserializer $ fromJust . decode
  }

intSerde :: Serde Int BL.ByteString
intSerde =
  Serde
  { serializer = Serializer B.encode
  , deserializer = Deserializer B.decode
  }

voidSerde :: Serde Void BL.ByteString
voidSerde =
  Serde
  { serializer = Serializer B.encode
  , deserializer = Deserializer B.decode
  }

objectObjectSerde :: Serde Object Object
objectObjectSerde =
  Serde
  { serializer = Serializer id
  , deserializer = Deserializer id
  }

timeWindowSerde :: Int64 -> Serde TimeWindow BL.ByteString
timeWindowSerde windowSize =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStartBuilder = BB.int64BE tWindowStart
          blankBuilder = BB.int64BE 0
       in BB.toLazyByteString $ winStartBuilder <> blankBuilder
  , deserializer = Deserializer $ runGet decodeTimeWindow
  }
  where
    decodeTimeWindow = do
      startTs <- getInt64be
      _       <- getInt64be
      return TimeWindow {tWindowStart = startTs, tWindowEnd = startTs + windowSize}

sessionWindowSerde :: Serde TimeWindow BL.ByteString
sessionWindowSerde =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStartBuilder = BB.int64BE tWindowStart
          winEndBuilder   = BB.int64BE tWindowEnd
       in BB.toLazyByteString $ winStartBuilder <> winEndBuilder
  , deserializer = Deserializer $ runGet decodeTimeWindow
  }
  where
    decodeTimeWindow = do
      startTs <- getInt64be
      endTs   <- getInt64be
      return TimeWindow {tWindowStart = startTs, tWindowEnd = endTs}

timeWindowObjectSerde :: Int64 -> Serde TimeWindow Object
timeWindowObjectSerde windowSize =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStart = [("winStart", Aeson.Number $ scientific (toInteger tWindowStart) 0)]
       in HM.fromList winStart
  , deserializer = Deserializer $ \obj ->
      let (Aeson.Number start) = (HM.!) obj "winStart"
          startInt64 = fromInteger $ coefficient start
       in mkTimeWindow startInt64 (startInt64 + windowSize)
  }

sessionWindowObjectSerde :: Serde TimeWindow Object
sessionWindowObjectSerde =
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStart = [("winStart", Aeson.Number $ scientific (toInteger tWindowStart) 0)]
          winEnd   = [("winEnd"  , Aeson.Number $ scientific (toInteger tWindowEnd  ) 0)]
       in HM.fromList $ winStart ++ winEnd
  , deserializer = Deserializer $ \obj ->
      let (Aeson.Number start) = (HM.!) obj "winStart"
          startInt64 = fromInteger $ coefficient start
          (Aeson.Number end)   = (HM.!) obj "winEnd"
          endInt64   = fromInteger $ coefficient end
       in mkTimeWindow startInt64 endInt64
  }
