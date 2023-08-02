{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeFamilies        #-}

module HStream.SQL.Rts.New where

import qualified Data.Aeson                   as Aeson
import qualified Data.Aeson.Types             as Aeson
import qualified Data.ByteString              as BS
import           Data.Default
import           Data.Function                (on)
import           Data.Hashable
import qualified Data.HashMap.Strict          as HM
import qualified Data.IntMap                  as IntMap
import qualified Data.List                    as L
import qualified Data.Scientific              as Scientific
import           Data.Text                    (Text)
import qualified Data.Text                    as Text
import           Data.Text.Encoding           (decodeUtf8, encodeUtf8)
import qualified Data.Time                    as Time
import           Data.Time.Format.ISO8601     (iso8601ParseM, iso8601Show)
import           Data.Typeable                (Typeable)
import qualified Data.Vector                  as V
import           GHC.Generics
import           Text.Read                    (readMaybe)
import qualified Z.Data.CBytes                as CB
import           Z.Data.CBytes                (CBytes)
import qualified Z.Data.Text                  as ZT
import qualified Z.Data.Vector.Base64         as Base64

import           HStream.SQL.Binder
import           HStream.SQL.Exception        (throwRuntimeException)
import           HStream.SQL.Extra
import           HStream.SQL.PlannerNew.Types
import           HStream.Utils                (cBytesToText, textToCBytes)
import qualified HStream.Utils.Aeson          as HsAeson

--------------------------------------------------------------------------------
-- The same between old and new version
--------------------------------------------------------------------------------
type FlowObject = HM.HashMap ColumnCatalog FlowValue
deriving instance Typeable FlowObject
deriving instance Aeson.FromJSONKey FlowObject
deriving instance Aeson.ToJSONKey FlowObject

instance Default FlowObject where
  def = HM.empty

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

constantToFlowValue :: Constant -> FlowValue
constantToFlowValue constant = case constant of
  ConstantNull         -> FlowNull
  ConstantInt n        -> FlowInt n
  ConstantFloat n      -> FlowFloat n
  ConstantText t       -> FlowText t
  ConstantBoolean b    -> FlowBoolean b
  ConstantDate d       -> FlowDate d
  ConstantTime t       -> FlowTime t
  ConstantTimestamp ts -> FlowTimestamp ts
  ConstantInterval i   -> FlowInterval i
  ConstantBytea bs     -> FlowByte bs
  ConstantJsonb o      -> FlowSubObject (jsonObjectToFlowObject' o)
  ConstantArray arr    -> FlowArray (constantToFlowValue <$> arr)

--------------------------------------------------------------------------------
-- Different between old and new version because of schema and catalog
--------------------------------------------------------------------------------
flowValueToJsonValue :: FlowValue -> Aeson.Value
flowValueToJsonValue flowValue = case flowValue of
  FlowNull      -> Aeson.Null
  FlowInt n     -> Aeson.Number (fromIntegral n)
  FlowFloat n   -> Aeson.Number (Scientific.fromFloatDigits n)
  FlowBoolean b -> Aeson.Bool b
  FlowByte bs   -> Aeson.Object $ HsAeson.fromList
    [( HsAeson.fromText "$binary"
     , Aeson.Object $ HsAeson.fromList
       [ (HsAeson.fromText "base64" , Aeson.String (Text.pack $ ZT.unpack . Base64.base64EncodeText $ CB.toBytes bs))
       , (HsAeson.fromText "subType", Aeson.String "00")
       ]
     )]
  FlowText t -> Aeson.String t
  FlowDate d -> Aeson.String (Text.pack $ iso8601Show d)
  FlowTime t -> Aeson.String (Text.pack $ iso8601Show t)
  FlowTimestamp ts -> Aeson.String (Text.pack $ iso8601Show ts)
  FlowInterval i   -> Aeson.String (Text.pack $ iso8601Show i)
  FlowArray vs         -> Aeson.Array (V.fromList $ flowValueToJsonValue <$> vs)
  FlowSubObject object -> Aeson.Object (flowObjectToJsonObject object)

jsonValueToFlowValue :: (BoundDataType, Aeson.Value) -> FlowValue
jsonValueToFlowValue (columnType, v) = case v of
  Aeson.Null       -> FlowNull
  Aeson.Bool b     -> FlowBoolean b
  Aeson.String t   -> case columnType of
    BTypeDate -> case iso8601ParseM (Text.unpack t) of
      Just d  -> FlowDate d
      Nothing -> throwRuntimeException $ "Invalid date value " <> Text.unpack t
    BTypeTime -> case iso8601ParseM (Text.unpack t) of
      Just d  -> FlowTime d
      Nothing -> throwRuntimeException $ "Invalid time value " <> Text.unpack t
    BTypeTimestamp -> case iso8601ParseM (Text.unpack t) of
      Just d  -> FlowTimestamp d
      Nothing -> throwRuntimeException $ "Invalid timestamp value " <> Text.unpack t
    BTypeInterval -> case iso8601ParseM (Text.unpack t) of
      Just d  -> FlowInterval d
      Nothing -> throwRuntimeException $ "Invalid interval value " <> Text.unpack t
    _ -> FlowText t -- FIXME
  Aeson.Number n   -> case columnType of
    BTypeFloat   -> FlowFloat (Scientific.toRealFloat n)
    BTypeInteger -> FlowInt (floor n)
    _            -> throwRuntimeException $ "Invalid number value " <> show n
  Aeson.Array arr  ->
    let (BTypeArray elemType) = columnType
     in FlowArray (V.toList $
                   jsonValueToFlowValue <$>
                   (V.replicate (V.length arr) elemType) `V.zip` arr)
  Aeson.Object obj -> case HsAeson.toList obj of
    [("$binary", Aeson.Object obj')] -> case do
      Aeson.String t <- HsAeson.lookup "base64" obj'
      Base64.base64Decode (CB.toBytes $ textToCBytes t) of
        Nothing -> throwRuntimeException $ "Invalid $binary value " <> show obj'
        Just bs -> FlowByte (CB.fromBytes bs)
    _ -> FlowSubObject (jsonObjectToFlowObject' obj)
  _ -> throwRuntimeException $ "Invalid json value: " <> show v

flowObjectToJsonObject :: FlowObject -> Aeson.Object
flowObjectToJsonObject hm =
  let list = L.map (\(ColumnCatalog{..}, v) ->
                      (HsAeson.fromText columnName, flowValueToJsonValue v)
                   ) (HM.toList hm)
   in HsAeson.fromList list

extractFlowObjectSchema :: FlowObject -> Schema
extractFlowObjectSchema hm =
  Schema { schemaOwner = "" -- FIXME
         , schemaColumns = let tups = L.map (\cata -> (columnId cata, cata)) (HM.keys hm)
                            in IntMap.fromList tups
         }

extractJsonObjectSchema :: Aeson.Object -> Schema
extractJsonObjectSchema json =
  let hm = HsAeson.toList json
      tups = L.map (\(i, (k,v)) ->
                      let cata =
                            ColumnCatalog { columnName = HsAeson.toText k
                                          , columnType = case v of
                                              Aeson.Null     -> undefined -- FIXME
                                              Aeson.Bool _   -> BTypeBoolean
                                              Aeson.String _ -> BTypeText
                                              Aeson.Number _ -> BTypeFloat
                                              Aeson.Array _  -> undefined -- FIXME
                                              Aeson.Object _ -> BTypeJsonb
                                          , columnId = i
                                          , columnStream = "unknown"
                                          , columnStreamId = 0
                                          , columnIsHidden = False
                                          , columnIsNullable = True
                                          }
                       in (i,cata)
                   ) ([0..] `zip` hm)
  in Schema { schemaOwner = "unknown"
            , schemaColumns = IntMap.fromList tups
            }

jsonObjectToFlowObject :: Schema -> Aeson.Object -> FlowObject
jsonObjectToFlowObject schema object =
  let list = HsAeson.toList object
      list' = L.map (\(k,v) ->
                       let catalog = case L.find (\col -> columnName col == HsAeson.toText k)
                                                 (IntMap.elems $ schemaColumns schema) of
                                       Just cata -> cata
                                       Nothing   -> throwRuntimeException $ -- FIXME: Just omit it instead of throwing exception?
                                         "Invalid key encountered: " <> show k <>
                                         ". Schema=" <> show schema
                        in (catalog, jsonValueToFlowValue (columnType catalog, v))
                    ) list
   in HM.fromList list'

jsonObjectToFlowObject' :: Aeson.Object -> FlowObject
jsonObjectToFlowObject' json = jsonObjectToFlowObject (extractJsonObjectSchema json) json


-- | Compose two 'FlowObject's. It assumes that the two 'FlowObject's
-- have contiguous column ids starting from 0.
-- The result 'FlowObject' will still have contiguous column ids
-- starting from 0 and of course, end with sum of the two 'FlowObject's
-- length.
-- Example: o1 = {(0,c1)->v1, (1,c2)->v2}, o2 = {(0,c3) -> v3}
--          o1 <::> o2 = {(0,c1)->v1, (1,c2)->v2, (2,c3)->v3}
infixl 5 <++>
(<++>) :: FlowObject -> FlowObject -> FlowObject
(<++>) o1 o2 =
  let tups1 = HM.toList o1
      tups2 = HM.toList o2
      maxId1 = if null tups1 then (-1) else (columnId . fst) (L.maximumBy (compare `on` (columnId . fst)) tups1)
      tups2' = L.map (\(cata, v) -> (cata { columnId = columnId cata + maxId1 + 1 }, v)) tups2
   in HM.fromList (tups1 <> tups2')

setFlowObjectStreamId :: Int -> FlowObject -> FlowObject
setFlowObjectStreamId streamId hm =
  let tups = HM.toList hm
      tups' = L.map (\(cata, v) -> (cata { columnStreamId = streamId }, v)) tups
   in HM.fromList tups'
