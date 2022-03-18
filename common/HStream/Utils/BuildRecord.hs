{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE RecordWildCards  #-}
{-# LANGUAGE TypeApplications #-}

module HStream.Utils.BuildRecord where

import           Control.Exception         (displayException)
import           Data.ByteString           (ByteString)
import qualified Data.ByteString.Lazy      as BL
import           Data.Int                  (Int64)
import           Data.Map.Strict           (Map)
import           Data.Maybe                (fromJust)
import           Data.Text                 (Text)
import           Google.Protobuf.Timestamp
import qualified Proto3.Suite              as PT
import           Z.Data.Vector             (Bytes)
import           Z.Foreign                 (fromByteString, toByteString)

import           HStream.Server.HStreamApi

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

encodeMessage :: PT.Message a => a  -> ByteString
encodeMessage = BL.toStrict . PT.toLazyByteString

decodeBytesToMessage :: PT.Message a => Bytes -> a
decodeBytesToMessage = decodeByteStringToMessage . toByteString

decodeByteStringToMessage :: PT.Message a => ByteString -> a
decodeByteStringToMessage msg =
  let rc = PT.fromByteString msg
  in case rc of
      Left e    -> error $ "Decode proto message error: " <> displayException e
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
getRecordKey HStreamRecord{..} =
  case hstreamRecordHeaderKey <$> hstreamRecordHeader of
    Just "__default__" -> Nothing
    Just ""            -> Nothing
    key                -> key

updateRecordTimestamp :: Timestamp -> HStreamRecord -> HStreamRecord
updateRecordTimestamp timestamp HStreamRecord{..} =
  HStreamRecord (update <$> hstreamRecordHeader) hstreamRecordPayload
  where
    update header = header { hstreamRecordHeaderPublishTime = Just timestamp }
