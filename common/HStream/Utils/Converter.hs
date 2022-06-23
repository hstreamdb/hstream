{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms   #-}

module HStream.Utils.Converter
  ( jsonObjectToStruct
  , jsonValueToValue
  , structToJsonObject
  , valueToJsonValue
  , structToZJsonObject
  , valueToZJsonValue
  , zJsonObjectToStruct
  , zJsonValueToValue
    --
  , bs2str
  , cBytesToText
  , cbytes2bs
  , cBytesToLazyText
  , textToCBytes
  , textToZBuilder
  , lazyTextToZBuilder
  , lazyTextToCBytes
  , lazyByteStringToCBytes
  , cBytesToLazyByteString
  , byteStringToBytes
  , lazyByteStringToBytes
  , bytesToByteString
  , bytesToLazyByteString
    --
  , cBytesToValue
  , stringToValue
  , valueToBytes
  , listToStruct
  , structToStruct

  , cBytesToIntegral
  , integralToCBytes
  ) where

import qualified Data.Aeson             as Aeson
import           Data.Bifunctor         (Bifunctor (second))
import qualified Data.ByteString        as BS
import qualified Data.ByteString.Lazy   as BL
import qualified Data.HashMap.Strict    as HM
import qualified Data.Map               as M
import qualified Data.Map.Strict        as Map
import           Data.Scientific        (toRealFloat)
import qualified Data.Text              as T
import qualified Data.Text              as Text
import qualified Data.Text.Encoding     as Text
import qualified Data.Text.Lazy         as TL
import qualified Data.Vector            as V
import qualified Google.Protobuf.Struct as PB
import           Proto3.Suite           (Enumerated (Enumerated))
import qualified Z.Data.Builder         as Build
import qualified Z.Data.Builder         as Builder
import qualified Z.Data.CBytes          as ZCB
import qualified Z.Data.JSON            as Z
import qualified Z.Data.Parser          as Parser
import qualified Z.Data.Text            as ZT
import qualified Z.Data.Vector          as ZV
import qualified Z.Foreign              as ZF

pattern V :: PB.ValueKind -> PB.Value
pattern V x = PB.Value (Just x)

jsonObjectToStruct :: Aeson.Object -> PB.Struct
jsonObjectToStruct object = PB.Struct kvmap
  where
    kvmap = M.fromList $ map (\(k,v) -> (k, Just (jsonValueToValue v))) (HM.toList object)

jsonValueToValue :: Aeson.Value -> PB.Value
jsonValueToValue (Aeson.Object object) = V $ PB.ValueKindStructValue (jsonObjectToStruct object)
jsonValueToValue (Aeson.Array  array)  = V $ PB.ValueKindListValue   (PB.ListValue $ jsonValueToValue <$> array)
jsonValueToValue (Aeson.String text)   = V $ PB.ValueKindStringValue text
jsonValueToValue (Aeson.Number sci)    = V $ PB.ValueKindNumberValue (toRealFloat sci)
jsonValueToValue (Aeson.Bool   bool)   = V $ PB.ValueKindBoolValue   bool
jsonValueToValue Aeson.Null            = V $ PB.ValueKindNullValue   (Enumerated $ Right PB.NullValueNULL_VALUE)

structToJsonObject :: PB.Struct -> Aeson.Object
structToJsonObject (PB.Struct kvmap) = HM.fromList $
  second convertMaybeValue <$> kvTuples
  where
    kvTuples = Map.toList kvmap
    convertMaybeValue Nothing  = error "Nothing encountered"
    convertMaybeValue (Just v) = valueToJsonValue v

valueToJsonValue :: PB.Value -> Aeson.Value
valueToJsonValue (V (PB.ValueKindStructValue struct))           = Aeson.Object (structToJsonObject struct)
valueToJsonValue (V (PB.ValueKindListValue   (PB.ListValue list))) = Aeson.Array  (valueToJsonValue <$> list)
valueToJsonValue (V (PB.ValueKindStringValue text))             = Aeson.String text
valueToJsonValue (V (PB.ValueKindNumberValue num))              = Aeson.Number (read . show $ num)
valueToJsonValue (V (PB.ValueKindBoolValue   bool))             = Aeson.Bool   bool
valueToJsonValue (V (PB.ValueKindNullValue   _))                = Aeson.Null
valueToJsonValue (PB.Value Nothing) = error "Nothing encountered"
-- The following line of code is not used but to fix a warning
valueToJsonValue (PB.Value (Just _)) = error "impossible happened"

zJsonObjectToStruct :: ZObject -> PB.Struct
zJsonObjectToStruct object = PB.Struct kvmap
 where
   kvmap = M.fromList $ map (\(k,v) -> (T.pack $ ZT.unpack k, Just (zJsonValueToValue v))) (ZV.unpack object)

zJsonValueToValue :: Z.Value -> PB.Value
zJsonValueToValue (Z.Object object) = V $ PB.ValueKindStructValue (zJsonObjectToStruct object)
zJsonValueToValue (Z.Array  array)  = V $ PB.ValueKindListValue   (PB.ListValue $ V.fromList $ zJsonValueToValue <$> ZV.unpack array)
zJsonValueToValue (Z.String text)   = V $ PB.ValueKindStringValue (T.pack $ ZT.unpack text)
zJsonValueToValue (Z.Number sci)    = V $ PB.ValueKindNumberValue (toRealFloat sci)
zJsonValueToValue (Z.Bool   bool)   = V $ PB.ValueKindBoolValue   bool
zJsonValueToValue Z.Null            = V $ PB.ValueKindNullValue   (Enumerated $ Right PB.NullValueNULL_VALUE)

type ZObject = ZV.Vector (ZT.Text, Z.Value)
structToZJsonObject :: PB.Struct -> ZObject
structToZJsonObject (PB.Struct kvmap) = ZV.pack $
  (\(text,value) -> (ZT.pack $ T.unpack text, convertMaybeValue value)) <$> kvTuples
  where
    kvTuples = Map.toList kvmap
    convertMaybeValue Nothing  = error "Nothing encountered"
    convertMaybeValue (Just v) = valueToZJsonValue v

valueToZJsonValue :: PB.Value -> Z.Value
valueToZJsonValue (V (PB.ValueKindStructValue struct))           = Z.Object (structToZJsonObject struct)
valueToZJsonValue (V (PB.ValueKindListValue   (PB.ListValue list))) = Z.Array  (ZV.pack $ V.toList $ valueToZJsonValue <$> list)
valueToZJsonValue (V (PB.ValueKindStringValue text))             = Z.String (ZT.pack $ T.unpack text)
valueToZJsonValue (V (PB.ValueKindNumberValue num))              = Z.Number (read . show $ num)
valueToZJsonValue (V (PB.ValueKindBoolValue   bool))             = Z.Bool   bool
valueToZJsonValue (V (PB.ValueKindNullValue   _))                = Z.Null
valueToZJsonValue (PB.Value Nothing) = error "Nothing encountered"
-- The following line of code is not used but to fix a warning
valueToZJsonValue (PB.Value (Just _)) = error "impossible happened"

cBytesToText :: ZCB.CBytes -> T.Text
cBytesToText = T.pack . ZCB.unpack

cbytes2bs :: ZCB.CBytes -> BS.ByteString
cbytes2bs = ZF.toByteString . ZCB.toBytes

bs2str :: BS.ByteString -> String
bs2str = Text.unpack . Text.decodeUtf8

cBytesToLazyText :: ZCB.CBytes -> TL.Text
cBytesToLazyText = TL.fromStrict . cBytesToText

cBytesToValue :: ZCB.CBytes -> PB.Value
cBytesToValue = PB.Value . Just . PB.ValueKindStringValue . cBytesToText

textToCBytes :: T.Text -> ZCB.CBytes
textToCBytes = ZCB.pack . T.unpack

lazyTextToCBytes :: TL.Text -> ZCB.CBytes
lazyTextToCBytes = textToCBytes . TL.toStrict

textToZBuilder :: T.Text -> Builder.Builder ()
textToZBuilder = Builder.stringUTF8 . T.unpack
{-# INLINE textToZBuilder #-}

lazyTextToZBuilder :: TL.Text -> Builder.Builder ()
lazyTextToZBuilder = Builder.stringUTF8 . T.unpack . TL.toStrict
{-# INLINE lazyTextToZBuilder #-}

cBytesToLazyByteString :: ZCB.CBytes -> BL.ByteString
cBytesToLazyByteString = BL.fromStrict . ZF.toByteString . ZCB.toBytes

lazyByteStringToCBytes :: BL.ByteString -> ZCB.CBytes
lazyByteStringToCBytes = ZCB.fromBytes . ZF.fromByteString . BL.toStrict

listToStruct :: T.Text -> [PB.Value] -> PB.Struct
listToStruct x = PB.Struct . Map.singleton x . Just . PB.Value . Just . PB.ValueKindListValue . PB.ListValue . V.fromList

structToStruct :: T.Text -> PB.Struct -> PB.Struct
structToStruct x = PB.Struct . Map.singleton x . Just . PB.Value . Just . PB.ValueKindStructValue

stringToValue :: String -> PB.Value
stringToValue = PB.Value . Just . PB.ValueKindStringValue . T.pack

lazyByteStringToBytes :: BL.ByteString -> ZV.Bytes
lazyByteStringToBytes = ZV.pack . BL.unpack

byteStringToBytes :: BS.ByteString -> ZV.Bytes
byteStringToBytes = ZF.fromByteString

bytesToLazyByteString :: ZV.Bytes -> BL.ByteString
bytesToLazyByteString = BL.pack . ZV.unpack

bytesToByteString :: ZV.Bytes -> BS.ByteString
bytesToByteString = ZF.toByteString

valueToBytes :: (Aeson.ToJSON a) => a -> ZV.Bytes
valueToBytes = lazyByteStringToBytes . Aeson.encode

cBytesToIntegral :: (Integral a, Bounded a) => ZCB.CBytes -> a
cBytesToIntegral cbytes = case Parser.parse' Parser.int . ZCB.toBytes $ cbytes of
  Right x  -> x
  Left err -> error (show err)

integralToCBytes :: (Integral a, Bounded a) => a -> ZCB.CBytes
integralToCBytes = ZCB.buildCBytes . Build.int
