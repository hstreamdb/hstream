{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE RecordWildCards  #-}
{-# LANGUAGE TypeApplications #-}

module HStream.Utils.BuildRecord where

import           Control.Exception         (displayException)
import           Data.ByteString           (ByteString)
import qualified Data.ByteString           as B
import qualified Data.ByteString.Lazy      as BL
import           Data.Int                  (Int64)
import           Data.Map.Strict           (Map)
import           Data.Maybe                (fromJust)
import           Data.Text                 (Text)
import qualified Proto3.Suite              as PT
import           Z.Data.CBytes             (CBytes)
import           Z.Data.Vector             (Bytes)
import           Z.Foreign                 (fromByteString, toByteString)

import           Google.Protobuf.Timestamp
import           HStream.Server.HStreamApi
import           HStream.Utils.Converter   (textToCBytes)

buildRecordHeader
  :: HStreamRecordHeader_Flag
  -> Map Text Text
  -> Timestamp
  -> Text
  -> HStreamRecordHeader
buildRecordHeader flag mp timestamp key =
  HStreamRecordHeader
    { hstreamRecordHeaderFlag        = PT.Enumerated (Right flag)
    , hstreamRecordHeaderAttributes  = mp
    , hstreamRecordHeaderPublishTime = Just timestamp
    , hstreamRecordHeaderKey         = key
    }
{-# INLINE buildRecordHeader #-}

buildRecord :: HStreamRecordHeader -> ByteString -> HStreamRecord
buildRecord header = HStreamRecord (Just header)

encodeRecord :: HStreamRecord -> ByteString
encodeRecord = BL.toStrict . PT.toLazyByteString

decodeRecord :: Bytes -> HStreamRecord
decodeRecord = decodeByteStringRecord . toByteString

decodeByteStringRecord :: B.ByteString -> HStreamRecord
decodeByteStringRecord record =
  let rc = PT.fromByteString record
  in case rc of
      Left e    -> error $ "Decode HStreamRecord error: " <> displayException e
      Right res -> res

encodeBatch :: HStreamRecordBatch -> ByteString
encodeBatch = BL.toStrict . PT.toLazyByteString

decodeBatch :: Bytes -> HStreamRecordBatch
decodeBatch = decodeByteStringBatch . toByteString

decodeByteStringBatch :: B.ByteString -> HStreamRecordBatch
decodeByteStringBatch batch =
  let rc = PT.fromByteString batch
  in case rc of
      Left e    -> error $ "Decode HStreamRecord error: " <> displayException e
      Right res -> res

getPayload :: HStreamRecord -> Bytes
getPayload HStreamRecord{..} = fromByteString hstreamRecordPayload

getPayloadFlag :: HStreamRecord -> PT.Enumerated HStreamRecordHeader_Flag
getPayloadFlag = hstreamRecordHeaderFlag . fromJust . hstreamRecordHeader

getTimeStamp :: HStreamRecord -> Int64
getTimeStamp HStreamRecord{..} =
  let Timestamp{..} = fromJust . hstreamRecordHeaderPublishTime . fromJust $ hstreamRecordHeader
      !ts = floor @Double $ (fromIntegral timestampSeconds * 1e3) + (fromIntegral timestampNanos / 1e6)
  in ts

getRecordKey :: HStreamRecord -> Maybe Text
getRecordKey record =
  case fmap hstreamRecordHeaderKey . hstreamRecordHeader $ record of
    Just "__default__" -> Nothing
    Just ""            -> Nothing
    key'               -> key'

updateRecordTimestamp :: Timestamp -> HStreamRecord -> HStreamRecord
updateRecordTimestamp timestamp HStreamRecord{..} =
  let oldHeader = fromJust hstreamRecordHeader
      newHeader = oldHeader { hstreamRecordHeaderPublishTime = Just timestamp }
   in HStreamRecord (Just newHeader) hstreamRecordPayload

clientDefaultKey :: Text
clientDefaultKey = "__default__"

clientDefaultKey' :: CBytes
clientDefaultKey' = textToCBytes clientDefaultKey
