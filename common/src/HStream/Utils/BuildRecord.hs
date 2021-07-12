{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE RecordWildCards  #-}
{-# LANGUAGE TypeApplications #-}

module HStream.Utils.BuildRecord where

import           Control.Exception                    (displayException)
import           Data.Bits                            (shiftL)
import qualified Data.ByteString                      as B
import           Data.ByteString.Lazy                 (ByteString)
import qualified Data.ByteString.Lazy                 as BL
import           Data.Int                             (Int64)
import           Data.Map.Strict                      (Map)
import           Data.Maybe                           (fromJust)
import           Data.Text.Lazy                       (Text)
import           Data.Word                            (Word32)
import qualified Proto3.Suite                         as PT
import           Z.Data.Vector                        (Bytes)
import           Z.Foreign                            (fromByteString,
                                                       toByteString)

import           HStream.Server.HStreamApi
import           ThirdParty.Google.Protobuf.Timestamp

jsonPayloadFlag :: Word32
jsonPayloadFlag = shiftL 0x01 24

rawPayloadFlag :: Word32
rawPayloadFlag = shiftL 0x02 24

buildRecordHeader :: Word32 -> Map Text Text -> Timestamp -> Text -> HStreamRecordHeader
buildRecordHeader flag mp timestamp key =
  HStreamRecordHeader
    { hstreamRecordHeaderFlag = flag
    , hstreamRecordHeaderAttributes = mp
    , hstreamRecordHeaderPublishTime = Just timestamp
    , hstreamRecordHeaderKey = key
    }

buildRecord :: HStreamRecordHeader -> ByteString -> HStreamRecord
buildRecord header payload = HStreamRecord (Just header) (BL.toStrict payload)

encodeRecord :: HStreamRecord -> Bytes
encodeRecord = fromByteString . BL.toStrict . PT.toLazyByteString

decodeRecord :: Bytes -> HStreamRecord
decodeRecord = decodeByteStringRecord . toByteString

decodeByteStringRecord :: B.ByteString -> HStreamRecord
decodeByteStringRecord record =
  let rc = PT.fromByteString record
  in case rc of
      Left e    -> error $ "Decode HStreamRecord error: " <> displayException e
      Right res -> res

getPayload :: HStreamRecord -> Bytes
getPayload HStreamRecord{..} = fromByteString hstreamRecordPayload

getPayloadFlag :: HStreamRecord -> Word32
getPayloadFlag = hstreamRecordHeaderFlag . fromJust . hstreamRecordHeader

getTimeStamp :: HStreamRecord -> Int64
getTimeStamp HStreamRecord{..} =
  let Timestamp{..} = fromJust . hstreamRecordHeaderPublishTime . fromJust $ hstreamRecordHeader
      !ts = floor @Double $ (fromIntegral timestampSeconds * 1e3) + (fromIntegral timestampNanos / 1e6)
  in ts

updateRecordTimestamp :: HStreamRecord -> Timestamp -> HStreamRecord
updateRecordTimestamp HStreamRecord{..} timestamp =
  let oldHeader = fromJust hstreamRecordHeader
      newHeader = oldHeader { hstreamRecordHeaderPublishTime = Just timestamp }
  in HStreamRecord (Just newHeader) hstreamRecordPayload
