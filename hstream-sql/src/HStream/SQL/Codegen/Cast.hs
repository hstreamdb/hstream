{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.Cast
  ( castOnValue
  ) where

import qualified Data.Map.Strict                as Map
import           Data.Scientific
import qualified Data.Text                      as T
import           Data.Time
import           Data.Time.Calendar.OrdinalDate (fromOrdinalDate)
import           HStream.SQL.AST
import           HStream.SQL.Exception
import qualified Z.Data.CBytes                  as CB

--------------------------------------------------------------------------------
castOnValue :: RDataType -> FlowValue -> FlowValue
castOnValue RTypeInteger     v = cast_integer v
castOnValue RTypeFloat       v = cast_float v
castOnValue RTypeNumeric     v = cast_numeric v
castOnValue RTypeBoolean     v = cast_boolean v
castOnValue RTypeBytea       v = cast_byte v
castOnValue RTypeText        v = cast_text v
castOnValue RTypeDate        v = cast_date v
castOnValue RTypeTime        v = cast_time v
castOnValue RTypeTimestamp   v = cast_timestamp v
castOnValue RTypeInterval    v = cast_interval v
castOnValue RTypeJsonb       v = cast_json v
castOnValue (RTypeArray t)   v = cast_array t v
castOnValue (RTypeMap kt vt) v = cast_map kt vt v

--------------------------------------------------------------------------------
cast_integer :: FlowValue -> FlowValue
cast_integer (FlowInt n) = FlowInt n
cast_integer (FlowFloat n) = FlowInt (floor n)
cast_integer (FlowNumeral n) = FlowInt (floor (toRealFloat n :: Double))
cast_integer v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Integer>"

cast_float :: FlowValue -> FlowValue
cast_float (FlowInt n) = FlowFloat (fromIntegral n)
cast_float (FlowFloat n) = FlowFloat n
cast_float (FlowNumeral n) = FlowFloat (toRealFloat n)
cast_float v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Float>"

cast_numeric :: FlowValue -> FlowValue
cast_numeric (FlowInt n) = FlowNumeral (scientific (fromIntegral n) 0)
cast_numeric (FlowFloat n) = FlowNumeral (fromFloatDigits n)
cast_numeric (FlowNumeral n) = FlowNumeral n
cast_numeric v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Numeric>"

cast_boolean :: FlowValue -> FlowValue
cast_boolean (FlowBoolean b) = FlowBoolean b
cast_boolean v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Boolean>"

cast_byte :: FlowValue -> FlowValue
cast_byte (FlowByte b) = FlowByte b
cast_byte v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Bytea>"

cast_text :: FlowValue -> FlowValue
cast_text FlowNull           = FlowText "NULL"
cast_text (FlowInt n)        = FlowText (T.pack . show $ n)
cast_text (FlowFloat n)      = FlowText (T.pack . show $ n)
cast_text (FlowNumeral n)    = FlowText (T.pack . show $ n)
cast_text (FlowBoolean b)    = FlowText (T.pack . show $ b)
cast_text (FlowByte cb)      = FlowText (T.pack . show . CB.toBytes $ cb)
cast_text (FlowText t)       = FlowText t
cast_text (FlowDate d)       = FlowText (T.pack . show $ d)
cast_text (FlowTime t)       = FlowText (T.pack . show $ t)
cast_text (FlowTimestamp ts) = FlowText (T.pack . show $ ts)
cast_text (FlowInterval d)   = FlowText (T.pack . show $ d)
cast_text (FlowJson o)       = FlowText (T.pack . show $ o)
cast_text (FlowArray arr)    = FlowText (T.pack . show $ arr)
cast_text (FlowMap m)        = FlowText (T.pack . show $ m)
cast_text (FlowSubObject o)  = FlowText (T.pack . show $ o)

cast_date :: FlowValue -> FlowValue
cast_date (FlowDate d) = FlowDate d
cast_date (FlowTimestamp ts) = FlowDate (utctDay . zonedTimeToUTC $ ts)
cast_date v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Date>"

cast_time :: FlowValue -> FlowValue
cast_time (FlowTime t) = FlowTime t
cast_time (FlowTimestamp ts) = FlowTime (timeToTimeOfDay . utctDayTime . zonedTimeToUTC $ ts)
cast_time v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Time>"

cast_timestamp :: FlowValue -> FlowValue
cast_timestamp (FlowTimestamp ts) = FlowTimestamp ts
cast_timestamp (FlowDate d) =
  let utcTime = UTCTime{utctDay = d, utctDayTime = 0}
   in FlowTimestamp (utcToZonedTime utc utcTime)
cast_timestamp (FlowTime t) =
  let utcTime = UTCTime{utctDay = fromOrdinalDate 1970 1, utctDayTime = timeOfDayToTime t}
   in FlowTimestamp (utcToZonedTime utc utcTime)
cast_timestamp v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Timestamp>"

cast_interval :: FlowValue -> FlowValue
cast_interval (FlowInterval i) = FlowInterval i
cast_interval (FlowInt n) =
  let cd = CalendarDiffTime{ ctMonths = 0, ctTime = fromIntegral n }
   in FlowInterval cd
cast_interval (FlowFloat n) =
  let cd = CalendarDiffTime{ ctMonths = 0, ctTime = realToFrac n }
   in FlowInterval cd
cast_interval (FlowNumeral n) =
  let cd = CalendarDiffTime{ ctMonths = 0, ctTime = fromRational . toRational $ n }
   in FlowInterval cd
cast_interval v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Interval>"

cast_json :: FlowValue -> FlowValue
cast_json (FlowJson o) = FlowJson o
cast_json v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Jsonb>"

cast_array :: RDataType -> FlowValue -> FlowValue
cast_array typ (FlowArray vs) = FlowArray (castOnValue typ <$> vs)
cast_array typ v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <[" <> show typ <> "]>"

cast_map :: RDataType -> RDataType -> FlowValue -> FlowValue
cast_map kt vt (FlowMap m) = FlowMap (Map.mapKeys (castOnValue kt) (Map.map (castOnValue vt) m))
cast_map kt vt v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type Map[" <> show kt <> "=>" <> show vt <> "]"
