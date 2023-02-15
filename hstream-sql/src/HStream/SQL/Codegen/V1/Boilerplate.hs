{-# LANGUAGE CPP                #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE StandaloneDeriving #-}

module HStream.SQL.Codegen.V1.Boilerplate where

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
import           HStream.Processing.Encoding
import           HStream.Processing.Stream.TimeWindows
import           HStream.SQL.AST
import           RIO                                   (Int64, Void)
#if MIN_VERSION_aeson(2,0,0)
import qualified Data.Aeson.Key                        as Key
import qualified Data.Aeson.KeyMap                     as KeyMap
#endif

deriving instance Serialized FlowObject

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

flowObjectSerde :: Serde FlowObject BL.ByteString
flowObjectSerde =
  Serde
  { serializer = Serializer encode
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

flowObjectFlowObjectSerde :: Serde FlowObject FlowObject
flowObjectFlowObjectSerde =
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
#if MIN_VERSION_aeson(2,0,0)
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStart = [(Key.fromText "winStart", Aeson.Number $ scientific (toInteger tWindowStart) 0)]
       in KeyMap.fromList winStart
  , deserializer = Deserializer $ \obj ->
      let (Aeson.Number start) = fromJust $ (KeyMap.lookup) "winStart" obj
          startInt64 = fromInteger $ coefficient start
       in mkTimeWindow startInt64 (startInt64 + windowSize)
  }
#else
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStart = [("winStart", Aeson.Number $ scientific (toInteger tWindowStart) 0)]
       in HM.fromList winStart
  , deserializer = Deserializer $ \obj ->
      let (Aeson.Number start) = (HM.!) obj "winStart"
          startInt64 = fromInteger $ coefficient start
       in mkTimeWindow startInt64 (startInt64 + windowSize)
  }
#endif

sessionWindowObjectSerde :: Serde TimeWindow Object
sessionWindowObjectSerde =
#if MIN_VERSION_aeson(2,0,0)
  Serde
  { serializer = Serializer $ \TimeWindow{..} ->
      let winStart = [(Key.fromText "winStart", Aeson.Number $ scientific (toInteger tWindowStart) 0)]
          winEnd   = [(Key.fromText "winEnd"  , Aeson.Number $ scientific (toInteger tWindowEnd  ) 0)]
       in KeyMap.fromList $ winStart ++ winEnd
  , deserializer = Deserializer $ \obj ->
      let (Aeson.Number start) = fromJust $ (KeyMap.lookup) "winStart" obj
          startInt64 = fromInteger $ coefficient start
          (Aeson.Number end)   = fromJust $ (KeyMap.lookup) "winEnd" obj
          endInt64   = fromInteger $ coefficient end
       in mkTimeWindow startInt64 endInt64
  }
#else
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
#endif

timeWindowFlowObjectSerde :: Int64 -> Serde TimeWindow FlowObject
timeWindowFlowObjectSerde windowSize =
  Serde
  { serializer = Serializer $ \tw -> (jsonObjectToFlowObject "") $ (runSer . serializer $ timeWindowObjectSerde windowSize) tw
  , deserializer = Deserializer $ \fo -> (runDeser . deserializer $ timeWindowObjectSerde windowSize) (flowObjectToJsonObject fo)
  }

sessionWindowFlowObjectSerde :: Serde TimeWindow FlowObject
sessionWindowFlowObjectSerde =
  Serde
  { serializer = Serializer $ \tw -> (jsonObjectToFlowObject "") $ (runSer . serializer $ sessionWindowObjectSerde) tw
  , deserializer = Deserializer $ \fo -> (runDeser . deserializer $ sessionWindowObjectSerde) (flowObjectToJsonObject fo)
  }
