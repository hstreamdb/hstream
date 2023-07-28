{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module HStream.SQL.Rts.Old where

import qualified Data.Aeson               as Aeson
import qualified Data.Aeson.Types         as Aeson
import qualified Data.ByteString          as BS
import           Data.Default
import           Data.Hashable
import qualified Data.HashMap.Strict      as HM
import qualified Data.IntMap              as IntMap
import qualified Data.List                as L
import           Data.List.Extra          (anySame)
import qualified Data.Scientific          as Scientific
import           Data.Text                (Text)
import qualified Data.Text                as Text
import           Data.Text.Encoding       (decodeUtf8, encodeUtf8)
import qualified Data.Time                as Time
import           Data.Time.Format.ISO8601 (iso8601ParseM, iso8601Show)
import           Data.Typeable            (Typeable)
import qualified Data.Vector              as V
import           GHC.Generics
import           Text.Read                (readMaybe)
import qualified Z.Data.CBytes            as CB
import           Z.Data.CBytes            (CBytes)
import qualified Z.Data.Text              as ZT
import qualified Z.Data.Vector.Base64     as Base64

import           HStream.SQL.Exception
import           HStream.SQL.Extra
import           HStream.Utils            (cBytesToText, textToCBytes)
import qualified HStream.Utils.Aeson      as HsAeson

type FlowObject = HM.HashMap ColumnCatalog FlowValue
deriving instance Typeable FlowObject
deriving instance Aeson.FromJSONKey FlowObject
deriving instance Aeson.ToJSONKey FlowObject

instance Default FlowObject where
  def = HM.empty

instance Eq Time.ZonedTime where
  z1 == z2 = Time.zonedTimeToUTC z1 == Time.zonedTimeToUTC z2
instance Ord Time.ZonedTime where
  z1 `compare` z2 = Time.zonedTimeToUTC z1 `compare` Time.zonedTimeToUTC z2
instance Hashable Time.ZonedTime where
  hashWithSalt salt z = hashWithSalt salt (Time.zonedTimeToUTC z)

instance Ord Time.CalendarDiffTime where
  d1 `compare` d2 =
    case (Time.ctMonths d1) `compare` (Time.ctMonths d2) of
      GT -> GT
      LT -> LT
      EQ -> Time.ctTime d1 `compare` Time.ctTime d2
instance Hashable Time.CalendarDiffTime where
  hashWithSalt salt d = hashWithSalt salt (show d)

data FlowValue
  = FlowNull
  | FlowInt Int
  | FlowFloat Double
  | FlowBoolean Bool
  | FlowByte CBytes
  | FlowText Text
  | FlowDate Time.Day
  | FlowTime Time.TimeOfDay
  | FlowTimestamp Time.ZonedTime
  | FlowInterval Time.CalendarDiffTime
  | FlowArray [FlowValue]
  | FlowSubObject FlowObject
  deriving ( Eq, Ord, Generic, Hashable, Typeable
           , Aeson.ToJSONKey, Aeson.ToJSON
           , Aeson.FromJSONKey, Aeson.FromJSON
           )

instance Show FlowValue where
  show value = case value of
    FlowNull         -> "NULL"
    FlowInt n        -> show n
    FlowFloat n      -> show n
    FlowBoolean b    -> show b
    FlowByte bs      -> show bs
    FlowText t       -> Text.unpack t
    FlowDate day     -> show day
    FlowTime time    -> show time
    FlowTimestamp ts -> show ts
    FlowInterval i   -> show i
    FlowArray arr    -> show arr
    FlowSubObject o  -> show o

showTypeOfFlowValue :: FlowValue -> Text.Text
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
    inferFlowArrayType :: [FlowValue] -> Text.Text
    inferFlowArrayType = \case
      []    -> "Array@[UNKNOWN]"
      x : _ -> "Array@[" <> Text.pack (show $ showTypeOfFlowValue x) <> "]"

-- In new version, defined in HStream.SQL.Binder.Common
data ColumnCatalog = ColumnCatalog
  { columnName   :: Text
  , columnStream :: Maybe Text
  } deriving ( Eq, Ord, Generic, Hashable, Typeable
             , Aeson.FromJSON, Aeson.FromJSONKey
             , Aeson.ToJSON, Aeson.ToJSONKey
             )
instance Show ColumnCatalog where
  show ColumnCatalog{..} = case columnStream of
                             Nothing -> Text.unpack columnName
                             Just s  -> Text.unpack s <> "." <> Text.unpack columnName

flowValueToJsonValue :: FlowValue -> Aeson.Value
flowValueToJsonValue flowValue = case flowValue of
  FlowNull      -> Aeson.Null
  FlowInt n     -> Aeson.Object $ HsAeson.fromList [(HsAeson.fromText "$numberLong"  , Aeson.String (Text.pack $ show n))]
  FlowFloat n   -> Aeson.Object $ HsAeson.fromList [(HsAeson.fromText "$numberDouble", Aeson.String (Text.pack $ show n))]
  FlowBoolean b -> Aeson.Bool b
  FlowByte bs   -> Aeson.Object $ HsAeson.fromList
    [( HsAeson.fromText "$binary"
     , Aeson.Object $ HsAeson.fromList
       [ (HsAeson.fromText "base64" , Aeson.String (Text.pack $ ZT.unpack . Base64.base64EncodeText $ CB.toBytes bs))
       , (HsAeson.fromText "subType", Aeson.String "00")
       ]
     )]
  FlowText t -> Aeson.String t
  FlowDate d -> Aeson.Object $ HsAeson.fromList
    [(HsAeson.fromText "$date", Aeson.String (Text.pack $ iso8601Show d))]
  FlowTime t -> Aeson.Object $ HsAeson.fromList
    [(HsAeson.fromText "$time", Aeson.String (Text.pack $ iso8601Show t))]
  FlowTimestamp ts -> Aeson.Object $ HsAeson.fromList
    [(HsAeson.fromText "$timestamp", Aeson.String (Text.pack $ iso8601Show ts))]
  FlowInterval i   -> Aeson.Object $ HsAeson.fromList
    [(HsAeson.fromText "$interval", Aeson.String (Text.pack $ iso8601Show i))]
  FlowArray vs         -> Aeson.Array (V.fromList $ flowValueToJsonValue <$> vs)
  FlowSubObject object -> Aeson.Object (flowObjectToJsonObject object)

jsonValueToFlowValue :: Aeson.Value -> FlowValue
jsonValueToFlowValue v = case v of
  Aeson.Null       -> FlowNull
  Aeson.Bool b     -> FlowBoolean b
  Aeson.String t   -> FlowText t
  Aeson.Number n   -> case Scientific.floatingOrInteger @Double @Int n of
    Left  f -> FlowFloat f
    Right i -> FlowInt i
  Aeson.Array arr  -> FlowArray (V.toList $ jsonValueToFlowValue <$> arr)
  Aeson.Object obj -> case HsAeson.toList obj of
    [("$numberLong", Aeson.String t)] ->
      case readMaybe (Text.unpack t) of
        Just n  -> FlowInt n
        Nothing -> throwSQLException RefineException Nothing ("Invalid $numberLong value" <> Text.unpack t)
    [("$numberDouble", Aeson.String t)] ->
      case readMaybe (Text.unpack t) of
        Just n  -> FlowFloat n
        Nothing -> throwSQLException RefineException Nothing ("Invalid $numberDouble value" <> Text.unpack t)
    [("$binary", Aeson.Object obj')] -> case do
      Aeson.String t <- HsAeson.lookup "base64" obj'
      Base64.base64Decode (CB.toBytes $ textToCBytes t) of
        Nothing -> throwSQLException RefineException Nothing ("Invalid $binary value" <> show obj')
        Just bs -> FlowByte (CB.fromBytes bs)
    [("$date", Aeson.String t)] ->
      case iso8601ParseM (Text.unpack t) of
        Just d  -> FlowDate d
        Nothing -> throwSQLException RefineException Nothing ("Invalid $date value" <> Text.unpack t)
    [("$time", Aeson.String t)] ->
      case iso8601ParseM (Text.unpack t) of
        Just t  -> FlowTime t
        Nothing -> throwSQLException RefineException Nothing ("Invalid $time value" <> Text.unpack t)
    [("$timestamp", Aeson.String t)] ->
      case iso8601ParseM (Text.unpack t) of
        Just ts -> FlowTimestamp ts
        Nothing -> throwSQLException RefineException Nothing ("Invalid $timestamp value" <> Text.unpack t)
    [("$interval", Aeson.String t)] ->
      case iso8601ParseM (Text.unpack t) of
        Just i -> FlowInterval i
        Nothing -> throwSQLException RefineException Nothing ("Invalid $interval value" <> Text.unpack t)
    _ -> FlowSubObject (jsonObjectToFlowObject' obj)

flowObjectToJsonObject :: FlowObject -> Aeson.Object
flowObjectToJsonObject hm =
  let anySameFields = anySame $ L.map (\(ColumnCatalog k _) -> k) (HM.keys hm)
      list = L.map (\(ColumnCatalog k s_m, v) ->
                      let key = case s_m of
                                  Nothing -> k
                                  Just s  -> if anySameFields then s <> "." <> k else k
                       in (HsAeson.fromText key, flowValueToJsonValue v)
                   ) (HM.toList hm)
   in HsAeson.fromList list

jsonObjectToFlowObject :: Text -> Aeson.Object -> FlowObject
jsonObjectToFlowObject streamName object =
  HM.mapKeys (\k -> ColumnCatalog (HsAeson.toText k) (Just streamName))
             (HM.map jsonValueToFlowValue $ HsAeson.toHashMap object)

jsonObjectToFlowObject' :: Aeson.Object -> FlowObject
jsonObjectToFlowObject' object =
  HM.mapKeys (\k -> ColumnCatalog (HsAeson.toText k) Nothing)
             (HM.map jsonValueToFlowValue $ HsAeson.toHashMap object)
