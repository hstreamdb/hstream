{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE KindSignatures      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module HStream.Utils.Codec where

import           Control.Exception          (throw)
import qualified Data.Attoparsec.ByteString as P
import           Data.Bits                  (Bits (shiftL), (.|.))
import qualified Data.ByteString            as BS
import qualified Data.ByteString.Builder    as BD
import qualified Data.ByteString.Lazy       as BSL
import           Data.Foldable              (foldl')
import           Data.Vector                (Vector)
import qualified Data.Vector                as V
import           Data.Word                  (Word32, Word8)
import qualified Proto3.Suite               as PT

import           HStream.Exception
import           HStream.Server.HStreamApi  (BatchHStreamRecords (..),
                                             HStreamRecord)

-- serializeHStreamRecords :: forall a. Encoder a => Serializer a -> Vector HStreamRecord -> BSL.ByteString
serializeHStreamRecords :: forall a. Encoder a => Vector HStreamRecord -> BSL.ByteString
serializeHStreamRecords = encode @a

-- deserializeHStreamRecords :: forall a. Encoder a => Serializer a -> BSL.ByteString -> Vector HStreamRecord
deserializeHStreamRecords :: forall a. Encoder a => BSL.ByteString -> Vector HStreamRecord
deserializeHStreamRecords = decode @a

class Encoder (a :: EncoderType) where
  encode :: Vector HStreamRecord -> BSL.ByteString
  decode :: BSL.ByteString -> Vector HStreamRecord

data EncoderType = OriginEncoder | ProtoEncoder

instance Encoder 'OriginEncoder where
  -- encode format: | size of payload1 | payload1 | size of payload2 | payload2 |
  --                | <- 32 bits BE -> |
  encode = BD.toLazyByteString . V.foldl' encodeRecord mempty
   where
     encodeRecord builder record =
      let encoded = PT.toLazyByteString record
          size = BSL.length encoded
       in builder <> BD.word32BE (fromIntegral size) <> BD.lazyByteString encoded

  decode payloads = case P.parseOnly (P.many' hStreamRecordParser) (BSL.toStrict payloads) of
    Left e        -> throw $ DecodeHStreamRecordErr (show e)
    Right records -> V.fromList records
   where
     hStreamRecordParser :: P.Parser HStreamRecord
     hStreamRecordParser = do
       size <- convert . BS.unpack <$> P.take 4
       hsRecord <- PT.fromByteString <$> P.take (fromIntegral size)
       case hsRecord of
         Left e       -> throw $ DecodeHStreamRecordErr (show e)
         Right record -> return record

     convert :: [Word8] -> Word32
     convert = foldl' (\acc o -> (acc `shiftL` 8) .|. fromIntegral o) 0

instance Encoder 'ProtoEncoder where
  encode records = let batchRecords = BatchHStreamRecords records
                    in PT.toLazyByteString batchRecords
  decode payloads = case PT.fromByteString $ BSL.toStrict payloads of
      Left e                        -> throw $ DecodeHStreamRecordErr (show e)
      Right BatchHStreamRecords{..} -> batchHStreamRecordsRecords
