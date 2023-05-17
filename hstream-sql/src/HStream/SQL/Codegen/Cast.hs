{-# LANGUAGE CPP               #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}

module HStream.SQL.Codegen.Cast
  ( castOnValue
  ) where

import qualified Data.Map.Strict                as Map
import           Data.Scientific
import qualified Data.Text                      as T
import           Data.Time                      as Time
import           Data.Time.Calendar.OrdinalDate (fromOrdinalDate)
import           Text.Read                      (readMaybe)
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
castOnValue RTypeInteger     v = castToInteger v
castOnValue RTypeFloat       v = castToFloat v
castOnValue RTypeBoolean     v = castToBoolean v
castOnValue RTypeBytea       v = castToByte v
castOnValue RTypeText        v = castToText v
castOnValue RTypeDate        v = castToDate v
castOnValue RTypeTime        v = castToTime v
castOnValue RTypeTimestamp   v = castToTimestamp v
castOnValue RTypeInterval    v = castToInterval v
castOnValue RTypeJsonb       v = castToJson v
castOnValue (RTypeArray t)   v = castToArray t v

--------------------------------------------------------------------------------
mkCanNotCastErr :: FlowValue -> T.Text -> Either ERROR_TYPE FlowValue
mkCanNotCastErr rawVal toTyp = Left . ERR $
  "CAST Error: Can not cast value <" <> T.pack (show rawVal)
    <> "> of type <" <> typText
    <> "> to type <" <> toTyp <>">"
  where
    typText :: T.Text
    typText = showTypeOfFlowValue rawVal

mkCanNotParseTextErr :: T.Text -> T.Text -> Either ERROR_TYPE FlowValue
mkCanNotParseTextErr x typ = Left . ERR $
  "CAST Error: Can not parse value <" <> T.pack (show x) <> "> of type <Text>"
    <> " to type <" <> typ <> ">"

showTypeOfFlowValue :: FlowValue -> T.Text
showTypeOfFlowValue = \case
  FlowNull        -> "Null"
  FlowInt _       -> "Integer"
  FlowFloat _     -> "Float"
  FlowBoolean _   -> "Boolean"
  FlowByte _      -> "Byte"
  FlowText _      -> "Text"
  FlowDate _      -> "Date"
  FlowTime _      -> "Time"
  FlowTimestamp _ -> "Timestamp"
  FlowInterval _  -> "Interval"
  FlowArray xs    -> inferFlowArrayType xs
  FlowSubObject _ -> "Jsonb"
  where
    inferFlowArrayType :: [FlowValue] -> T.Text
    inferFlowArrayType = \case
      []    -> "Array@[UNKNOW]"
      x : _ -> "Array@[" <> T.pack (show $ showTypeOfFlowValue x) <> "]"


--------------------------------------------------------------------------------
castToInteger :: FlowValue -> Either ERROR_TYPE FlowValue
castToInteger x =
  let mkOk :: Int -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowInt
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Integer"
  in case x of
    FlowInt x     -> mkOk x
    FlowFloat x   -> mkOk $ floor x
    FlowBoolean x -> mkOk $ case x of
      True  -> 1
      False -> 0
    FlowText x -> case readMaybe @Int $ T.unpack x of
      Nothing -> mkErr
      Just x  -> mkOk x
    _ -> mkErr

castToFloat :: FlowValue -> Either ERROR_TYPE FlowValue
castToFloat x =
  let mkOk :: Double -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowFloat
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Float"
  in case x of
    FlowInt x     -> mkOk $ fromIntegral x
    FlowFloat x   -> mkOk x
    FlowBoolean x -> mkOk $ case x of
      True  -> 1.0
      False -> 0.0
    FlowText x -> case readMaybe @Double $ T.unpack x of
      Nothing -> mkErr
      Just x  -> mkOk x
    _ -> mkErr

castToBoolean :: FlowValue -> Either ERROR_TYPE FlowValue
castToBoolean x =
  let mkOk :: Bool -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowBoolean
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Boolean"
  in case x of
    FlowInt   x   -> mkOk $ x /= 0
    FlowFloat x   -> mkOk $ x /= 0.0
    FlowBoolean x -> mkOk x
    FlowText x    -> case readMaybe @Double $ T.unpack x of
      Just x  -> mkOk $ x /= 0.0
      Nothing -> case T.toUpper $ T.strip x of
        "TRUE"  -> mkOk True
        "FALSE" -> mkOk False
    _             -> mkErr

castToByte :: FlowValue -> Either ERROR_TYPE FlowValue
castToByte x =
  let mkOk :: CB.CBytes -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowByte
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Byte"
  in case x of
    FlowByte x -> mkOk x
    _          -> mkErr

castToText :: FlowValue -> Either ERROR_TYPE FlowValue
castToText FlowNull           = Right $ FlowText "NULL"
castToText (FlowInt n)        = Right $ FlowText (T.pack . show $ n)
castToText (FlowFloat n)      = Right $ FlowText (T.pack . show $ n)
castToText (FlowBoolean b)    = Right $ FlowText (T.pack . show $ b)
castToText (FlowByte cb)      = Right $ FlowText (T.pack . show . CB.toBytes $ cb)
castToText (FlowText t)       = Right $ FlowText t
castToText (FlowDate d)       = Right $ FlowText (T.pack . show $ d)
castToText (FlowTime t)       = Right $ FlowText (T.pack . show $ t)
castToText (FlowTimestamp ts) = Right $ FlowText (T.pack . show $ ts)
castToText (FlowInterval d)   = Right $ FlowText (T.pack . show $ d)
castToText (FlowArray arr)    = Right $ FlowText (T.pack . show $ arr)
castToText (FlowSubObject o)  = Right $ FlowText (T.pack . show $ o)

castToDate :: FlowValue -> Either ERROR_TYPE FlowValue
castToDate x =
  let mkOk :: Time.Day -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowDate
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Date"
  in case x of
    FlowDate x      -> mkOk x
    FlowTimestamp x -> mkOk . utctDay . zonedTimeToUTC $ x
    _               -> mkErr

castToTime :: FlowValue -> Either ERROR_TYPE FlowValue
castToTime x =
  let mkOk :: Time.TimeOfDay -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowTime
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Time"
  in case x of
    FlowTime      x -> mkOk x
    FlowTimestamp x -> mkOk . timeToTimeOfDay . utctDayTime . zonedTimeToUTC $ x
    _               -> mkErr

castToTimestamp :: FlowValue -> Either ERROR_TYPE FlowValue
castToTimestamp x =
  let mkOk :: Time.ZonedTime -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowTimestamp
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Timestamp"
  in case x of
    FlowTimestamp x -> mkOk x
    FlowDate      x -> mkOk . utcToZonedTime utc $ UTCTime
      { utctDay     = x
      , utctDayTime = 0
      }
    FlowTime      x -> mkOk . utcToZonedTime utc $ UTCTime
      { utctDay     = fromOrdinalDate 1970 1
      , utctDayTime = timeOfDayToTime x
      }
    _ -> mkErr

castToInterval :: FlowValue -> Either ERROR_TYPE FlowValue
castToInterval x =
  let mkOk :: Time.CalendarDiffTime -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowInterval
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Interval"
  in case x of
    FlowInterval x -> mkOk x
    FlowInt      x -> mkOk $ CalendarDiffTime
      { ctMonths = 0
      , ctTime   = fromIntegral x
      }
    FlowFloat   x -> mkOk $ CalendarDiffTime
      { ctMonths = 0
      , ctTime   = realToFrac x
      }
    _ -> mkErr

castToJson :: FlowValue -> Either ERROR_TYPE FlowValue
castToJson x =
  let mkOk :: FlowObject -> Either ERROR_TYPE FlowValue
      mkOk = Right . FlowSubObject
      mkErr :: Either ERROR_TYPE FlowValue
      mkErr = mkCanNotCastErr x "Jsonb"
  in case x of
    FlowSubObject x -> mkOk x
    -- TODO: should we parse JSON here?
    _               -> mkErr

castToArray :: RDataType -> FlowValue -> Either ERROR_TYPE FlowValue
castToArray typ (FlowArray vs) = do
  vs' <- mapM (castOnValue typ) vs
  Right $ FlowArray vs'
castToArray typ v = mkCanNotCastErr v $ "Array@[" <> T.pack (show typ) <> "]"
