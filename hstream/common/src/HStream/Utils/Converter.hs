{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}

module HStream.Utils.Converter
  ( jsonObjectToStruct
  , jsonValueToValue
  , structToJsonObject
  , valueToJsonValue
  , zJsonObjectToStruct
  , zJsonValueToValue
  , structToZJsonObject
  , valueToZJsonValue
  , cbytesToText
  , textToCBytes
  , lazyByteStringToCBytes
  , cbytesToLazyByteString
  , cbytesToValue
  , listToStruct
  , structToStruct
  ) where

import           Control.Exception                 (Exception (..))
import qualified Data.Aeson                        as Aeson
import qualified Data.HashMap.Strict               as HM
import qualified Data.Map                          as M
import qualified Data.Map.Strict                   as Map
import           Data.Scientific
import qualified Data.Text.Lazy                    as TL
import qualified Data.Vector                       as V

import qualified Data.ByteString.Lazy              as BL
import qualified Data.Text                         as T
import           Proto3.Suite
import           ThirdParty.Google.Protobuf.Struct
import qualified Z.Data.CBytes                     as ZCB
import qualified Z.Data.JSON                       as Z
import qualified Z.Data.Text                       as ZT
import qualified Z.Data.Vector                     as ZV
import qualified Z.Foreign                         as ZF

pattern V x = Value (Just x)

jsonObjectToStruct :: Aeson.Object -> Struct
jsonObjectToStruct object = Struct kvmap
  where
    kvmap = M.fromList $ map (\(k,v) -> (TL.fromStrict k, Just (jsonValueToValue v))) (HM.toList object)

jsonValueToValue :: Aeson.Value -> Value
jsonValueToValue (Aeson.Object object) = V $ ValueKindStructValue (jsonObjectToStruct object)
jsonValueToValue (Aeson.Array  array)  = V $ ValueKindListValue   (ListValue $ jsonValueToValue <$> array)
jsonValueToValue (Aeson.String text)   = V $ ValueKindStringValue (TL.fromStrict text)
jsonValueToValue (Aeson.Number sci)    = V $ ValueKindNumberValue (toRealFloat sci)
jsonValueToValue (Aeson.Bool   bool)   = V $ ValueKindBoolValue   bool
jsonValueToValue Aeson.Null            = V $ ValueKindNullValue   (Enumerated $ Right NullValueNULL_VALUE)

structToJsonObject :: Struct -> Aeson.Object
structToJsonObject (Struct kvmap) = HM.fromList $
  (\(text,value) -> (TL.toStrict text, convertMaybeValue value)) <$> kvTuples
  where
    kvTuples = Map.toList kvmap
    convertMaybeValue Nothing  = error "Nothing encountered"
    convertMaybeValue (Just v) = valueToJsonValue v

valueToJsonValue :: Value -> Aeson.Value
valueToJsonValue (Value Nothing) = error "Nothing encountered"
valueToJsonValue (V (ValueKindStructValue struct))           = Aeson.Object (structToJsonObject struct)
valueToJsonValue (V (ValueKindListValue   (ListValue list))) = Aeson.Array  (valueToJsonValue <$> list)
valueToJsonValue (V (ValueKindStringValue text))             = Aeson.String (TL.toStrict text)
valueToJsonValue (V (ValueKindNumberValue num))              = Aeson.Number (read . show $ num)
valueToJsonValue (V (ValueKindBoolValue   bool))             = Aeson.Bool   bool
valueToJsonValue (V (ValueKindNullValue   _))                = Aeson.Null

zJsonObjectToStruct :: ZObject -> Struct
zJsonObjectToStruct object = Struct kvmap
 where
   kvmap = M.fromList $ map (\(k,v) -> (TL.pack $ ZT.unpack k, Just (zJsonValueToValue v))) (ZV.unpack object)

zJsonValueToValue :: Z.Value -> Value
zJsonValueToValue (Z.Object object) = V $ ValueKindStructValue (zJsonObjectToStruct object)
zJsonValueToValue (Z.Array  array)  = V $ ValueKindListValue   (ListValue $ V.fromList $ zJsonValueToValue <$> ZV.unpack array)
zJsonValueToValue (Z.String text)   = V $ ValueKindStringValue (TL.pack $ ZT.unpack text)
zJsonValueToValue (Z.Number sci)    = V $ ValueKindNumberValue (toRealFloat sci)
zJsonValueToValue (Z.Bool   bool)   = V $ ValueKindBoolValue   bool
zJsonValueToValue Z.Null            = V $ ValueKindNullValue   (Enumerated $ Right NullValueNULL_VALUE)

type ZObject = ZV.Vector (ZT.Text, Z.Value)
structToZJsonObject :: Struct -> ZObject
structToZJsonObject (Struct kvmap) = ZV.pack $
  (\(text,value) -> (ZT.pack $ TL.unpack text, convertMaybeValue value)) <$> kvTuples
  where
    kvTuples = Map.toList kvmap
    convertMaybeValue Nothing  = error "Nothing encountered"
    convertMaybeValue (Just v) = valueToZJsonValue v

valueToZJsonValue :: Value -> Z.Value
valueToZJsonValue (Value Nothing) = error "Nothing encountered"
valueToZJsonValue (V (ValueKindStructValue struct))           = Z.Object (structToZJsonObject struct)
valueToZJsonValue (V (ValueKindListValue   (ListValue list))) = Z.Array  (ZV.pack $ V.toList $ valueToZJsonValue <$> list)
valueToZJsonValue (V (ValueKindStringValue text))             = Z.String (ZT.pack $ TL.unpack text)
valueToZJsonValue (V (ValueKindNumberValue num))              = Z.Number (read . show $ num)
valueToZJsonValue (V (ValueKindBoolValue   bool))             = Z.Bool   bool
valueToZJsonValue (V (ValueKindNullValue   _))                = Z.Null

cbytesToText :: ZCB.CBytes -> T.Text
cbytesToText = T.pack . ZCB.unpack

textToCBytes :: T.Text -> ZCB.CBytes
textToCBytes = ZCB.pack . T.unpack

cbytesToLazyByteString :: ZCB.CBytes -> BL.ByteString
cbytesToLazyByteString = BL.fromStrict . ZF.toByteString . ZCB.toBytes

lazyByteStringToCBytes :: BL.ByteString -> ZCB.CBytes
lazyByteStringToCBytes = ZCB.fromBytes . ZF.fromByteString . BL.toStrict

listToStruct :: TL.Text -> [Value] -> Struct
listToStruct x = Struct . Map.singleton x . Just . Value . Just . ValueKindListValue . ListValue . V.fromList

structToStruct :: TL.Text -> Struct -> Struct
structToStruct x = Struct . Map.singleton x . Just . Value . Just . ValueKindStructValue

cbytesToValue :: ZCB.CBytes -> Value
cbytesToValue = Value . Just . ValueKindStringValue . TL.fromStrict . cbytesToText

getKeyWordFromException :: Exception a => a -> TL.Text
getKeyWordFromException =  TL.pack . takeWhile (/='{') . show
