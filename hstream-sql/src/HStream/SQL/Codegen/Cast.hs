{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}

module HStream.SQL.Codegen.Cast
  ( castOnValue
  ) where

import qualified Data.Map.Strict                as Map
import           Data.Scientific
import qualified Data.Text                      as T
import           Data.Time
import           Data.Time.Calendar.OrdinalDate (fromOrdinalDate)
#ifdef HStreamUseV2Engine
import           DiffFlow.Error
#else
import           HStream.Processing.Error
#endif
import           HStream.SQL.AST
import           HStream.SQL.Exception
import qualified Z.Data.CBytes                  as CB

#ifdef HStreamUseV2Engine
#define ERROR_TYPE DiffFlowError
#define ERR RunShardError
#else
#define ERROR_TYPE HStreamProcessingError
#define ERR OperationError
#endif

--------------------------------------------------------------------------------
castOnValue :: RDataType -> FlowValue -> Either ERROR_TYPE FlowValue
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
cast_integer :: FlowValue -> Either ERROR_TYPE FlowValue
cast_integer (FlowInt n) = Right $ FlowInt n
cast_integer (FlowFloat n) = Right $ FlowInt (floor n)
cast_integer (FlowNumeral n) = Right $ FlowInt (floor (toRealFloat n :: Double))
cast_integer v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Integer>"

cast_float :: FlowValue -> Either ERROR_TYPE FlowValue
cast_float (FlowInt n) = Right $ FlowFloat (fromIntegral n)
cast_float (FlowFloat n) = Right $ FlowFloat n
cast_float (FlowNumeral n) = Right $ FlowFloat (toRealFloat n)
cast_float v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Float>"

cast_numeric :: FlowValue -> Either ERROR_TYPE FlowValue
cast_numeric (FlowInt n) = Right $ FlowNumeral (scientific (fromIntegral n) 0)
cast_numeric (FlowFloat n) = Right $ FlowNumeral (fromFloatDigits n)
cast_numeric (FlowNumeral n) = Right $ FlowNumeral n
cast_numeric v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Numeric>"

cast_boolean :: FlowValue -> Either ERROR_TYPE FlowValue
cast_boolean (FlowBoolean b) = Right $ FlowBoolean b
cast_boolean v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Boolean>"

cast_byte :: FlowValue -> Either ERROR_TYPE FlowValue
cast_byte (FlowByte b) = Right $ FlowByte b
cast_byte v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Bytea>"

cast_text :: FlowValue -> Either ERROR_TYPE FlowValue
cast_text FlowNull           = Right $ FlowText "NULL"
cast_text (FlowInt n)        = Right $ FlowText (T.pack . show $ n)
cast_text (FlowFloat n)      = Right $ FlowText (T.pack . show $ n)
cast_text (FlowNumeral n)    = Right $ FlowText (T.pack . show $ n)
cast_text (FlowBoolean b)    = Right $ FlowText (T.pack . show $ b)
cast_text (FlowByte cb)      = Right $ FlowText (T.pack . show . CB.toBytes $ cb)
cast_text (FlowText t)       = Right $ FlowText t
cast_text (FlowDate d)       = Right $ FlowText (T.pack . show $ d)
cast_text (FlowTime t)       = Right $ FlowText (T.pack . show $ t)
cast_text (FlowTimestamp ts) = Right $ FlowText (T.pack . show $ ts)
cast_text (FlowInterval d)   = Right $ FlowText (T.pack . show $ d)
cast_text (FlowJson o)       = Right $ FlowText (T.pack . show $ o)
cast_text (FlowArray arr)    = Right $ FlowText (T.pack . show $ arr)
cast_text (FlowMap m)        = Right $ FlowText (T.pack . show $ m)
cast_text (FlowSubObject o)  = Right $ FlowText (T.pack . show $ o)

cast_date :: FlowValue -> Either ERROR_TYPE FlowValue
cast_date (FlowDate d) = Right $ FlowDate d
cast_date (FlowTimestamp ts) = Right $ FlowDate (utctDay . zonedTimeToUTC $ ts)
cast_date v =
  throwRuntimeException $ "Can not cast value <" <> show v <> "> to type <Date>"

cast_time :: FlowValue -> Either ERROR_TYPE FlowValue
cast_time (FlowTime t) = Right $ FlowTime t
cast_time (FlowTimestamp ts) = Right $ FlowTime (timeToTimeOfDay . utctDayTime . zonedTimeToUTC $ ts)
cast_time v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Time>"

cast_timestamp :: FlowValue -> Either ERROR_TYPE FlowValue
cast_timestamp (FlowTimestamp ts) = Right $ FlowTimestamp ts
cast_timestamp (FlowDate d) =
  let utcTime = UTCTime{utctDay = d, utctDayTime = 0}
   in Right $ FlowTimestamp (utcToZonedTime utc utcTime)
cast_timestamp (FlowTime t) =
  let utcTime = UTCTime{utctDay = fromOrdinalDate 1970 1, utctDayTime = timeOfDayToTime t}
   in Right $ FlowTimestamp (utcToZonedTime utc utcTime)
cast_timestamp v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Timestamp>"

cast_interval :: FlowValue -> Either ERROR_TYPE FlowValue
cast_interval (FlowInterval i) = Right $ FlowInterval i
cast_interval (FlowInt n) =
  let cd = CalendarDiffTime{ ctMonths = 0, ctTime = fromIntegral n }
   in Right $ FlowInterval cd
cast_interval (FlowFloat n) =
  let cd = CalendarDiffTime{ ctMonths = 0, ctTime = realToFrac n }
   in Right $ FlowInterval cd
cast_interval (FlowNumeral n) =
  let cd = CalendarDiffTime{ ctMonths = 0, ctTime = fromRational . toRational $ n }
   in Right $ FlowInterval cd
cast_interval v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Interval>"

cast_json :: FlowValue -> Either ERROR_TYPE FlowValue
cast_json (FlowJson o) = Right $ FlowJson o
cast_json v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <Jsonb>"

cast_array :: RDataType -> FlowValue -> Either ERROR_TYPE FlowValue
cast_array typ (FlowArray vs) = do
  vs' <- mapM (castOnValue typ) vs
  Right $ FlowArray vs'
cast_array typ v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type <[" <> T.pack (show typ) <> "]>"

cast_map :: RDataType -> RDataType -> FlowValue -> Either ERROR_TYPE FlowValue
cast_map kt vt (FlowMap m) = do
  let (ks,vs) = unzip $ Map.toList m
  ks' <- mapM (castOnValue kt) ks
  vs' <- mapM (castOnValue vt) vs
  Right . FlowMap $ Map.fromList (zip ks' vs')
cast_map kt vt v =
  Left . ERR $ "Can not cast value <" <> T.pack (show v) <> "> to type Map[" <> T.pack (show kt) <> "=>" <> T.pack (show vt) <> "]"
