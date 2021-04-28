module HStream.Server.Utils
  ( jsonObjectToStruct
  , jsonValueToValue
  , structToJsonObject
  , valueToJsonValue
  ) where

import qualified Data.Aeson                        as Aeson
import qualified Data.HashMap.Strict               as HM
import qualified Data.Map                          as M
import qualified Data.Map.Strict                   as Map
import           Data.Scientific
import qualified Data.Text.Lazy                    as TL
import           Proto3.Suite
import           ThirdParty.Google.Protobuf.Struct

-- Aeson.Value  <-> PB.Value
-- Aeson.Object <-> PB.Struct

jsonObjectToStruct :: Aeson.Object -> Struct
jsonObjectToStruct object = Struct kvmap
  where kvmap = M.fromList $ map (\(k,v) -> (TL.fromStrict k,Just (jsonValueToValue v))) (HM.toList object)

jsonValueToValue :: Aeson.Value -> Value
jsonValueToValue (Aeson.Object object) = Value (Just $ ValueKindStructValue (jsonObjectToStruct object))
jsonValueToValue (Aeson.Array  array)  = Value (Just $ ValueKindListValue (ListValue $ jsonValueToValue <$> array))
jsonValueToValue (Aeson.String text)   = Value (Just $ ValueKindStringValue (TL.fromStrict text))
jsonValueToValue (Aeson.Number sci)    = Value (Just $ ValueKindNumberValue (toRealFloat sci))
jsonValueToValue (Aeson.Bool   bool)   = Value (Just $ ValueKindBoolValue bool)
jsonValueToValue Aeson.Null            = Value (Just $ ValueKindNullValue (Enumerated $ Right NullValueNULL_VALUE))

structToJsonObject :: Struct -> Aeson.Object
structToJsonObject (Struct kvmap) = HM.fromList $
  (\(text,value) -> (TL.toStrict text, convertMaybeValue value)) <$> kvTuples
  where kvTuples = Map.toList kvmap
        convertMaybeValue Nothing  = error "Nothing encountered"
        convertMaybeValue (Just v) = valueToJsonValue v

valueToJsonValue :: Value -> Aeson.Value
valueToJsonValue (Value Nothing) = error "Nothing encountered"
valueToJsonValue (Value (Just (ValueKindStructValue struct)))         = Aeson.Object (structToJsonObject struct)
valueToJsonValue (Value (Just (ValueKindListValue (ListValue list)))) = Aeson.Array (valueToJsonValue <$> list)
valueToJsonValue (Value (Just (ValueKindStringValue text)))           = Aeson.String (TL.toStrict text)
valueToJsonValue (Value (Just (ValueKindNumberValue num)))            = Aeson.Number (read . show $ num)
valueToJsonValue (Value (Just (ValueKindBoolValue bool)))             = Aeson.Bool bool
valueToJsonValue (Value (Just (ValueKindNullValue _)))                = Aeson.Null
