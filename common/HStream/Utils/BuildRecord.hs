{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Utils.BuildRecord where

import           Control.Exception         (Exception, displayException, throw)
import qualified Data.ByteString           as BS
import qualified Data.ByteString.Lazy      as BSL
import           Data.Int                  (Int64)
import           Data.Map.Strict           (Map)
import           Data.Maybe                (fromJust, fromMaybe)
import           Data.Text                 (Text)
import qualified Data.Vector               as V
import qualified Proto3.Suite              as PT
import           Z.Data.CBytes             (CBytes)
import           Z.Data.Vector             (Bytes)
import           Z.Foreign                 (fromByteString, toByteString)

import           Data.Word                 (Word32)
import           Google.Protobuf.Timestamp
import           HStream.Server.HStreamApi
import           HStream.Utils.Codec       (Encoder, EncoderType (..),
                                            deserializeHStreamRecords,
                                            serializeHStreamRecords)
import           HStream.Utils.Compression (compress, decompress)
import           HStream.Utils.Converter   (textToCBytes)

------------------------------------------------------------------------------------------------
-- HStreamRecord

buildRecordHeader
  :: HStreamRecordHeader_Flag
  -> Map Text Text
  -> Text
  -> HStreamRecordHeader
buildRecordHeader flag mp key =
  HStreamRecordHeader
    { hstreamRecordHeaderFlag        = PT.Enumerated (Right flag)
    , hstreamRecordHeaderAttributes  = mp
    , hstreamRecordHeaderKey         = key
    }
{-# INLINE buildRecordHeader #-}

mkHStreamRecord :: HStreamRecordHeader -> BS.ByteString -> HStreamRecord
mkHStreamRecord header = HStreamRecord (Just header)

-- compress HStreamRecords to batchedRecord, first serializeHStreamRecord, then compress
compressHStreamRecords :: PT.Enumerated CompressionType -> V.Vector HStreamRecord -> BS.ByteString
compressHStreamRecords = compressHStreamRecords' @'ProtoEncoder

compressHStreamRecords' :: forall a. Encoder a => PT.Enumerated CompressionType -> V.Vector HStreamRecord -> BS.ByteString
compressHStreamRecords' tp records = compress tp $ serializeHStreamRecords @a records

-- deserialize ByteString to HStreamRecord
decodeByteStringRecord :: BS.ByteString -> HStreamRecord
decodeByteStringRecord record =
  let rc = PT.fromByteString record
  in case rc of
      Left e    -> throw . DecodeHStreamRecordErr $ "Decode HStreamRecord error: " <> displayException e
      Right res -> res

getPayload :: HStreamRecord -> Bytes
getPayload HStreamRecord{..} = fromByteString hstreamRecordPayload

getPayloadFlag :: HStreamRecord -> PT.Enumerated HStreamRecordHeader_Flag
getPayloadFlag = hstreamRecordHeaderFlag . fromJust . hstreamRecordHeader

getRecordKey :: HStreamRecord -> Text
getRecordKey record =
  case fmap hstreamRecordHeaderKey . hstreamRecordHeader $ record of
    Just key -> key
    Nothing  -> throw NoRecordHeader

------------------------------------------------------------------------------------------------
-- BatchedRecord

mkBatchedRecord
  :: PT.Enumerated CompressionType
  -> Maybe Timestamp
  -> Word32
  -> V.Vector HStreamRecord
  -> BatchedRecord
mkBatchedRecord tp timestamp size records =
  let compressedRecord = compressHStreamRecords tp records
   in BatchedRecord tp timestamp size compressedRecord

-- decompress batchedRecord to HStreamRecords, first decompress batchRecordPayload, then deserializeHStreamRecord
decompressBatchedRecord :: BatchedRecord -> V.Vector HStreamRecord
decompressBatchedRecord = decompressBatchedRecord' @'ProtoEncoder

decompressBatchedRecord' :: forall a. Encoder a => BatchedRecord -> V.Vector HStreamRecord
decompressBatchedRecord' BatchedRecord{..} =
  deserializeHStreamRecords @a $ decompress batchedRecordCompressionType batchedRecordPayload

encodBatchRecord :: BatchedRecord -> BS.ByteString
encodBatchRecord = BSL.toStrict . PT.toLazyByteString

decodeBatchRecord :: Bytes -> BatchedRecord
decodeBatchRecord = decodeByteStringBatch . toByteString

decodeByteStringBatch :: BS.ByteString -> BatchedRecord
decodeByteStringBatch batch =
  let rc = PT.fromByteString batch
  in case rc of
      Left e    -> throw . DecodeHStreamRecordErr $ "Decode BatchedRecords error: " <> displayException e
      Right res -> res

getTimeStamp :: BatchedRecord -> Int64
getTimeStamp BatchedRecord{..} =
  let Timestamp{..} = fromMaybe (Timestamp 0 0) batchedRecordPublishTime
      !ts = floor @Double $ (fromIntegral timestampSeconds * 1e3) + (fromIntegral timestampNanos / 1e6)
   in ts

updateRecordTimestamp :: Timestamp -> BatchedRecord -> BatchedRecord
updateRecordTimestamp timestamp batch = batch { batchedRecordPublishTime = Just timestamp }

------------------------------------------------------------------------------------------------
-- others

clientDefaultKey :: Text
clientDefaultKey = ""

clientDefaultKey' :: CBytes
clientDefaultKey' = textToCBytes clientDefaultKey

newtype DecodeHStreamRecordErr = DecodeHStreamRecordErr String
  deriving(Show)
instance Exception DecodeHStreamRecordErr

data NoRecordHeader = NoRecordHeader
  deriving (Show)
instance Exception NoRecordHeader where
  displayException NoRecordHeader = "HStreamRecord doesn't have a header."

newtype DecodeBatchRecordErr = DecodeBatchRecordErr String
  deriving(Show)
instance Exception DecodeBatchRecordErr
