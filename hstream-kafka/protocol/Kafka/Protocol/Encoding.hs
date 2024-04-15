{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE ViewPatterns          #-}

module Kafka.Protocol.Encoding
  ( -- * Parser
    Parser, DecodeError (..)
  , runGet
  , runGet'
  , runPut
  , runPutLazy
    -- * Message Format
  , RecordBatch (..)
  , decodeRecordBatch
  , updateRecordBatchBaseOffset
  , unsafeUpdateRecordBatchBaseOffset
    -- ** Attributes
  , Attributes (Attributes)
  , CompressionType (..)
  , TimestampType (..)
  , compressionType
  , timestampType
    -- ** Record
    -- * Old Message Format()
    -- TODO
    -- * Internals
    -- ** Parser
  , runParser
  , runParser'
  , Result (..)
  , takeBytes
    -- ** Builder
  , Builder
  , builderLength
  , toLazyByteString
    -- * Misc
  , pattern NonNullKaArray
  , unNonNullKaArray
  , kaArrayToCompact
  , kaArrayFromCompact

  , module Kafka.Protocol.Encoding.Types

    -- TODO: The following are outdated, and need to be updated.

    -- * Exports for Produce
  , decodeBatchRecordsForProduce
  , unsafeAlterBatchRecordsBsForProduce

    -- * Records
  , BatchRecord (..)
  , decodeBatchRecords
  , decodeBatchRecords'
  , encodeBatchRecords
  , encodeBatchRecordsLazy
  , modifyBatchRecordsOffset
  , RecordV0 (..)
  , RecordV1 (..)
  , RecordV2 (..)
  , RecordV2_ (..)
  , RecordBatch1 (..)
  , RecordHeader
    -- ** Helpers
  , decodeLegacyRecordBatch
  , decodeRecordMagic
  , decodeNextRecordOffset
  , unsafeAlterRecordBatchBs
  , unsafeAlterMessageSetBs
  ) where

import           Control.DeepSeq                  (NFData)
import           Control.Exception
import           Control.Monad
import           Data.Bits
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as BS
import           Data.ByteString.Internal         (ByteString (BS), w2c)
import qualified Data.ByteString.Lazy             as BL
import qualified Data.ByteString.Unsafe           as BSU
import           Data.Digest.CRC32                (crc32)
import           Data.Digest.CRC32C               (crc32c)
import           Data.Int
import           Data.Maybe
import           Data.Typeable                    (Typeable, showsTypeRep,
                                                   typeOf)
import           Data.Vector                      (Vector)
import qualified Data.Vector                      as V
import           Foreign.Ptr                      (plusPtr)
import           GHC.ForeignPtr                   (unsafeWithForeignPtr)
import           GHC.Generics

import qualified HStream.Base.Growing             as Growing
import           Kafka.Protocol.Encoding.Encode
import           Kafka.Protocol.Encoding.Internal
import           Kafka.Protocol.Encoding.Parser
import           Kafka.Protocol.Encoding.Types
import           Kafka.Protocol.Error

-------------------------------------------------------------------------------
-- Parser

newtype DecodeError = DecodeError (ErrorCode, String)
  deriving (Show)

instance Exception DecodeError

runParser' :: Typeable r => Parser r -> ByteString -> IO (r, ByteString)
runParser' parser bs = do
  result <- runParser parser bs
  case result of
    Done l r   -> pure (r, l)
    Fail _ err -> throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Fail, " <> err)
    More _     -> throwIO $ DecodeError $
      (CORRUPT_MESSAGE, showsTypeRep (typeOf parser) ", need more")
{-# INLINE runParser' #-}

runGet :: Serializable a => ByteString -> IO a
runGet bs = do
  (r, l) <- runParser' get bs
  if BS.null l then pure r
               else throwIO $ DecodeError $
                 let msg = "runGet done, but left " <> map w2c (BS.unpack l)
                  in (CORRUPT_MESSAGE, msg)
{-# INLINE runGet #-}

runGet' :: Serializable a => ByteString -> IO (a, ByteString)
runGet' = runParser' get
{-# INLINE runGet' #-}

runPutLazy :: Serializable a => a -> BL.ByteString
runPutLazy = toLazyByteString . put
{-# INLINE runPutLazy #-}

runPut :: Serializable a => a -> ByteString
runPut = BL.toStrict . toLazyByteString . put
{-# INLINE runPut #-}

-------------------------------------------------------------------------------

-- | Common Record base for all versions.
--
-- To help parse all Record version.
data RecordBase = RecordBase
  { baseOffset                :: {-# UNPACK #-} !Int64
  , batchLength               :: {-# UNPACK #-} !Int32
    -- ^ The total size of the record batch in bytes
    -- (from partitionLeaderEpochOrCrc to the end)
  , partitionLeaderEpochOrCrc :: {-# UNPACK #-} !Int32
    -- ^ For version 0-1, this is the CRC32 of the remainder of the record.
    -- For version 2, this is the partition leader epoch.
  , magic                     :: {-# UNPACK #-} !Int8
  } deriving (Generic, Show, Eq)

instance Serializable RecordBase

-------------------------------------------------------------------------------
-- RecordBatch: v2

-- Ref: https://kafka.apache.org/documentation/#recordbatch
--
-- Introduced in Kafka 0.11.0
data RecordBatch = RecordBatch
  { baseOffset           :: {-# UNPACK #-} !Int64
  , batchLength          :: {-# UNPACK #-} !Int32
    -- ^ The total size of the record batch in bytes,
    -- from partitionLeaderEpoch(included) to the end.
  , partitionLeaderEpoch :: {-# UNPACK #-} !Int32
  , magic                :: {-# UNPACK #-} !Int8
  , crc                  :: {-# UNPACK #-} !Int32
  , attributes           :: {-# UNPACK #-} !Attributes
  , lastOffsetDelta      :: {-# UNPACK #-} !Int32
  , baseTimestamp        :: {-# UNPACK #-} !Int64
  , maxTimestamp         :: {-# UNPACK #-} !Int64
  , producerId           :: {-# UNPACK #-} !Int64
  , producerEpoch        :: {-# UNPACK #-} !Int16
  , baseSequence         :: {-# UNPACK #-} !Int32
  , recordsCount         :: {-# UNPACK #-} !Int32
  , recordsData          :: !ByteString
    -- ^ Note that when compression is enabled, this is the compressed data
  } deriving (Generic, Show, Eq)

instance NFData RecordBatch

-- 49: partitionLeaderEpoch(4) + magic(1)
--   + crc(4) + attributes(2) + lastOffsetDelta(4)
--   + baseTimestamp(8) + maxTimestamp(8)
--   + producerId(8) + producerEpoch(2)
--   + baseSequence(4)
--   + recordsCount(4)
sizeOfPartitionLeaderEpochToRecordCount :: Int
sizeOfPartitionLeaderEpochToRecordCount = 49

decodeRecordBatch :: Bool -> ByteString -> IO RecordBatch
decodeRecordBatch shouldValidateCrc bs = do
  (RecordBase{..}, bs') <- runGet' @RecordBase bs
  case magic of
    2 -> do
      let partitionLeaderEpoch = partitionLeaderEpochOrCrc
      -- The CRC covers the data from the attributes to the end of
      -- the batch (i.e. all the bytes that follow the CRC).
      --
      -- The CRC-32C (Castagnoli) polynomial is used for the
      -- computation.
      (crc, bs'') <- runGet' @Int32 bs'
      when shouldValidateCrc $ do
        let crcPayload = BS.take (fromIntegral batchLength - 9) bs''
        when (fromIntegral (crc32c crcPayload) /= crc) $
          throwIO $ DecodeError (CORRUPT_MESSAGE, "Invalid CRC32")
      (RecordBatchHead{..}, recordBs) <- runGet' @RecordBatchHead bs''
      (recordsCount, recordsData) <- runGet' @Int32 recordBs
      if BS.length recordsData == fromIntegral batchLength - sizeOfPartitionLeaderEpochToRecordCount
         then pure RecordBatch{..}
         else throwIO $ DecodeError (INVALID_RECORD, "There are some bytes left")
    _ -> throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid magic " <> show magic)

-- Be sure to use this function after the calling of 'decodeRecordBatch',
-- since we do not check the bounds.
updateRecordBatchBaseOffset :: ByteString -> (Int64 -> Int64) -> IO ByteString
updateRecordBatchBaseOffset bs f = do
  let (BS ofp _) = BSU.unsafeTake 8 bs
  offset <- unsafeWithForeignPtr ofp $ fmap (f . fromIntegral) . peek64BE
  pure $ runPut offset <> (BSU.unsafeDrop 8 bs)

-- FIXME: I don't know how the ghc sharing does, so be careful.
--
-- https://hackage.haskell.org/package/bytestring-0.12.1.0/docs/Data-ByteString-Unsafe.html#v:unsafeUseAsCString
-- https://hackage.haskell.org/package/vector-0.13.1.0/docs/Data-Vector.html#v:unsafeThaw
unsafeUpdateRecordBatchBaseOffset :: ByteString -> (Int64 -> Int64) -> IO ()
unsafeUpdateRecordBatchBaseOffset (BS fp len) f =
  unsafeWithForeignPtr fp $ \p -> do
    -- FIXME improvement: does ghc provide a modifyPtr function?
    origin <- fromIntegral <$> peek64BE p
    poke64BE p (fromIntegral $ f origin)

-- | Internal type to help parse RecordBatch
--
-- RecordBatch = RecordBase + CRC32 + RecordBatchHead + recordsCount + recordsData
data RecordBatchHead = RecordBatchHead
  { attributes      :: {-# UNPACK #-} !Attributes
  , lastOffsetDelta :: {-# UNPACK #-} !Int32
  , baseTimestamp   :: {-# UNPACK #-} !Int64
  , maxTimestamp    :: {-# UNPACK #-} !Int64
  , producerId      :: {-# UNPACK #-} !Int64
  , producerEpoch   :: {-# UNPACK #-} !Int16
  , baseSequence    :: {-# UNPACK #-} !Int32
  } deriving (Generic, Show, Eq)

instance NFData RecordBatchHead
instance Serializable RecordBatchHead

newtype Attributes = Attributes Int16
  deriving newtype (Show, Eq, NFData, Serializable)

compressionCodecMask :: Int16
compressionCodecMask = 0x07

timestampTypeMask :: Int16
timestampTypeMask = 0x08

transactionalFlagMask :: Int16
transactionalFlagMask = 0x10

controlFlagMask :: Int16
controlFlagMask = 0x20

deleteHorizonFlagMask :: Int16
deleteHorizonFlagMask = 0x40

data CompressionType
  = CompressionTypeNone
  | CompressionTypeGzip
  | CompressionTypeSnappy
  | CompressionTypeLz4
  | CompressionTypeZstd
  deriving (Show, Eq)

instance Enum CompressionType where
  toEnum 0 = CompressionTypeNone
  toEnum 1 = CompressionTypeGzip
  toEnum 2 = CompressionTypeSnappy
  toEnum 3 = CompressionTypeLz4
  toEnum 4 = CompressionTypeZstd
  toEnum x = error $ "Unknown compression type id: " <> show x
  {-# INLINE toEnum #-}

  fromEnum CompressionTypeNone   = 0
  fromEnum CompressionTypeGzip   = 1
  fromEnum CompressionTypeSnappy = 2
  fromEnum CompressionTypeLz4    = 3
  fromEnum CompressionTypeZstd   = 4
  {-# INLINE fromEnum #-}

data TimestampType
  = TimestampTypeCreateTime
  | TimestampTypeLogAppendTime
  | TimestampTypeNone
  deriving (Show, Eq)

instance Enum TimestampType where
  toEnum 0    = TimestampTypeCreateTime
  toEnum 1    = TimestampTypeLogAppendTime
  toEnum (-1) = TimestampTypeNone
  toEnum x    = error $ "Invalid timestamp type: " <> show x
  {-# INLINE toEnum #-}

  fromEnum TimestampTypeCreateTime    = 0
  fromEnum TimestampTypeLogAppendTime = 1
  fromEnum TimestampTypeNone          = (-1)
  {-# INLINE fromEnum #-}

compressionType :: Attributes -> CompressionType
compressionType (Attributes attr) =
  toEnum $ fromIntegral (attr .&. compressionCodecMask)
{-# INLINE compressionType #-}

timestampType :: Attributes -> TimestampType
timestampType (Attributes attr) = toEnum $ fromIntegral (attr .&. timestampTypeMask)
{-# INLINE timestampType #-}

-------------------------------------------------------------------------------
-- Records

data BatchRecord
  = BatchRecordV0 RecordV0
  | BatchRecordV1 RecordV1
  | BatchRecordV2 RecordBatch1
  deriving (Show, Eq, Generic)

instance NFData BatchRecord

decodeBatchRecords :: Bool -> ByteString -> IO (Vector BatchRecord)
decodeBatchRecords shouldValidateCrc batchBs =
  fst <$> decodeBatchRecords' shouldValidateCrc batchBs
{-# INLINABLE decodeBatchRecords #-}

-- | The same as 'decodeBatchRecords', but with some extra information for
-- convenience.
--
-- Extra information: the real total number of records.
--
-- TODO: support compression
decodeBatchRecords' :: Bool -> ByteString -> IO (Vector BatchRecord, Int)
decodeBatchRecords' shouldValidateCrc batchBs = Growing.new >>= decode 0 batchBs
  where
    decode len "" !v = (, len) <$> Growing.unsafeFreeze v
    decode !len !bs !v = do
      (RecordBase{..}, bs') <- runGet' @RecordBase bs
      case magic of
        0 -> do let crc = partitionLeaderEpochOrCrc
                    messageSize = batchLength
                when (messageSize < fromIntegral minRecordSizeV0) $
                  throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid messageSize")
                when shouldValidateCrc $ do
                  -- The crc field contains the CRC32 (and not CRC-32C) of the
                  -- subsequent message bytes (i.e. from magic byte to the value).
                  --
                  -- NOTE: pass the origin inputs to validLegacyCrc, not the bs'
                  validLegacyCrc (fromIntegral batchLength) crc bs
                (RecordBodyV0{..}, remainder) <- runGet' @RecordBodyV0 bs'
                !v' <- Growing.append v (BatchRecordV0 RecordV0{..})
                decode (len + 1) remainder v'
        1 -> do let crc = partitionLeaderEpochOrCrc
                    messageSize = batchLength
                when (messageSize < fromIntegral minRecordSizeV1) $
                  throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid messageSize")
                when shouldValidateCrc $ do
                  -- The crc field contains the CRC32 (and not CRC-32C) of the
                  -- subsequent message bytes (i.e. from magic byte to the value).
                  --
                  -- NOTE: pass the origin inputs to validLegacyCrc, not the bs'
                  validLegacyCrc (fromIntegral batchLength) crc bs
                (RecordBodyV1{..}, remainder) <- runGet' @RecordBodyV1 bs'
                !v' <- Growing.append v (BatchRecordV1 RecordV1{..})
                decode (len + 1) remainder v'
        2 -> do let partitionLeaderEpoch = partitionLeaderEpochOrCrc
                -- The CRC covers the data from the attributes to the end of
                -- the batch (i.e. all the bytes that follow the CRC).
                --
                -- The CRC-32C (Castagnoli) polynomial is used for the
                -- computation.
                (crc, bs'') <- runGet' @Int32 bs'
                when shouldValidateCrc $ do
                  let crcPayload = BS.take (fromIntegral batchLength - 9) bs''
                  when (fromIntegral (crc32c crcPayload) /= crc) $
                    throwIO $ DecodeError (CORRUPT_MESSAGE, "Invalid CRC32")
                (RecordBodyV2{..}, remainder) <- runGet' @RecordBodyV2 bs''
                !v' <- Growing.append v (BatchRecordV2 RecordBatch1{..})
                let !batchLen = maybe 0 V.length (unKaArray records)
                -- Actually, there should be only one batch record here, but
                -- we don't require it.
                decode (len + batchLen) remainder v'
        _ -> throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid magic " <> show magic)
{-# INLINABLE decodeBatchRecords' #-}

encodeBatchRecordsLazy :: Vector BatchRecord -> BL.ByteString
encodeBatchRecordsLazy rs =
  let builder = V.foldl' (\s x -> s <> putBatchRecord x) mempty rs
   in toLazyByteString builder
{-# INLINABLE encodeBatchRecordsLazy #-}

encodeBatchRecords :: Vector BatchRecord -> ByteString
encodeBatchRecords = BL.toStrict . encodeBatchRecordsLazy
{-# INLINABLE encodeBatchRecords #-}

modifyBatchRecordsOffset
  :: (Int64 -> Int64)
  -> Vector BatchRecord
  -> Vector BatchRecord
modifyBatchRecordsOffset f rs = V.map go rs
  where
    go (BatchRecordV0 r) = BatchRecordV0 r{baseOffset = f r.baseOffset}
    go (BatchRecordV1 r) = BatchRecordV1 r{baseOffset = f r.baseOffset}
    go (BatchRecordV2 r) = BatchRecordV2 r{baseOffset = f r.baseOffset}
{-# INLINABLE modifyBatchRecordsOffset #-}

putBatchRecord :: BatchRecord -> Builder
putBatchRecord (BatchRecordV0 r) = put r
putBatchRecord (BatchRecordV1 r) = put r
putBatchRecord (BatchRecordV2 r) = put r

decodeRecordMagic :: ByteString -> IO Int8
decodeRecordMagic bs =
  let parser = do
        -- baseOffset(8) + batchLength(4) + partitionLeaderEpochOrCrc(4)
        dropBytes 16
        get @Int8
   in fst <$> runParser' parser bs
{-# INLINE decodeRecordMagic #-}

-- | Get the offset of the record from the batch bs.
--
-- Return (baseOffset + batchLen) which is the (offset + 1) of the last complete
-- record.
decodeNextRecordOffset :: ByteString -> IO (Maybe Int64)
decodeNextRecordOffset bs = fst <$> runParser' parser' bs
  where
    parser' = parser (fromIntegral $ BS.length bs) Nothing

    parser !remainingLen !r = if remainingLen <= 12{- baseOffset, batchLength -}
      -- FailFast: batch is incomplete
      then pure r
      else do
        baseOffset <- get @Int64
        batchLength <- get @Int32
        let remainingLen' = remainingLen - 12 - batchLength
        -- batch is incomplete
        if remainingLen' < 0 then pure r else do
          dropBytes 4 -- partitionLeaderEpoch: int32
          magic <- get @Int8
          case magic of
            2 -> do
              -- crc: int32 + attributes: int16 + lastOffsetDelta: int32 +
              -- baseTimestamp: int64 + maxTimestamp: int64 +
              -- producerId: int64 + producerEpoch: int16 +
              -- baseSequence: int32
              dropBytes 40
              batchRecordsLen <- get @Int32
              -- Note here we don't minus 1
              let r' = baseOffset + fromIntegral batchRecordsLen
              dropBytes (fromIntegral batchLength - 49)
              parser remainingLen' (Just r')
            1 -> do
              dropBytes $ fromIntegral batchLength - 5
              parser remainingLen' (Just $ baseOffset + 1)
            0 -> do
              dropBytes $ fromIntegral batchLength - 5
              parser remainingLen' (Just $ baseOffset + 1)
            _ -> fail $ "Invalid magic " <> show magic
{-# INLINE decodeNextRecordOffset #-}

validLegacyCrc :: Int -> Int32 -> ByteString -> IO ()
validLegacyCrc batchLength crc bs = do
  crcPayload <- getLegacyCrcPayload batchLength bs
  when (fromIntegral (crc32 crcPayload) /= crc) $
    throwIO $ DecodeError (CORRUPT_MESSAGE, "Invalid CRC32")
{-# INLINE validLegacyCrc #-}

getLegacyCrcPayload :: Int -> ByteString -> IO ByteString
getLegacyCrcPayload msgSize bs =
  let parser = do void $ takeBytes 16  -- [offset(8) message_size(4) crc(4) ...]
                  takeBytes (msgSize - 4)
   in fst <$> runParser' parser bs
{-# INLINE getLegacyCrcPayload #-}

-------------------------------------------------------------------------------
-- For Handler

-- FIXME: support magic 0 and 1 are incomplete, donot use it.
decodeBatchRecordsForProduce :: Bool -> ByteString -> IO (Int, [Int])
decodeBatchRecordsForProduce shouldValidateCrc = decode 0 0 []
  where
    decode len _consumed offsetOffsets ""  = pure (len, offsetOffsets)
    decode !len !consumed !offsetOffsets !bs = do
      (RecordBase{..}, bs') <- runGet' @RecordBase bs
      case magic of
        2 -> do let partitionLeaderEpoch = partitionLeaderEpochOrCrc
                -- The CRC covers the data from the attributes to the end of
                -- the batch (i.e. all the bytes that follow the CRC).
                --
                -- The CRC-32C (Castagnoli) polynomial is used for the
                -- computation.
                (crc, bs'') <- runGet' @Int32 bs'
                when shouldValidateCrc $ do
                  let crcPayload = BS.take (fromIntegral batchLength - 9) bs''
                  when (fromIntegral (crc32c crcPayload) /= crc) $
                    throwIO $ DecodeError (CORRUPT_MESSAGE, "Invalid CRC32")
                (batchRecordsLen, remainder) <- runParser'
                  (do batchRecordsLen <- unsafePeekInt32At 36
                                   -- 36: attributes(2) + lastOffsetDelta(4)
                                   --   + baseTimestamp(8) + maxTimestamp(8)
                                   --   + producerId(8) + producerEpoch(2)
                                   --   + baseSequence(4)
                      dropBytes (fromIntegral batchLength - 9)
                      pure batchRecordsLen
                  ) bs''
                let batchRecordsLen' = if batchRecordsLen >= 0
                                          then fromIntegral batchRecordsLen
                                          else 0
                -- Actually, there should be only one batch record here, but
                -- we don't require it.
                decode (len + batchRecordsLen')
                       (consumed + fromIntegral batchLength + 12)
                       (consumed:offsetOffsets)
                       remainder
        0 -> do let crc = partitionLeaderEpochOrCrc
                    messageSize = batchLength
                when (messageSize < fromIntegral minRecordSizeV0) $
                  throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid messageSize")
                when shouldValidateCrc $ do
                  -- The crc field contains the CRC32 (and not CRC-32C) of the
                  -- subsequent message bytes (i.e. from magic byte to the value).
                  --
                  -- NOTE: pass the origin inputs to validLegacyCrc, not the bs'
                  validLegacyCrc (fromIntegral batchLength) crc bs
                let totalSize = fromIntegral $ messageSize + 12
                remainder <- snd <$> runParser' (dropBytes totalSize) bs
                decode (len + 1) (consumed + totalSize)
                       (consumed:offsetOffsets) remainder
        1 -> do let crc = partitionLeaderEpochOrCrc
                    messageSize = batchLength
                when (messageSize < fromIntegral minRecordSizeV1) $
                  throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid messageSize")
                when shouldValidateCrc $ do
                  -- The crc field contains the CRC32 (and not CRC-32C) of the
                  -- subsequent message bytes (i.e. from magic byte to the value).
                  --
                  -- NOTE: pass the origin inputs to validLegacyCrc, not the bs'
                  validLegacyCrc (fromIntegral batchLength) crc bs
                let totalSize = fromIntegral $ messageSize + 12
                remainder <- snd <$> runParser' (dropBytes totalSize) bs
                decode (len + 1) (consumed + totalSize)
                       (consumed:offsetOffsets) remainder
        _ -> throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Invalid magic " <> show magic)
{-# INLINABLE decodeBatchRecordsForProduce #-}

unsafeAlterBatchRecordsBsForProduce
  :: (Int64 -> Int64) -- Update baseOffsets
  -> [Int]            -- All bytes offsets of baseOffset
  -> ByteString
  -> IO ()
unsafeAlterBatchRecordsBsForProduce boof boos bs@(BS fp len) = do
  unsafeWithForeignPtr fp $ \p -> do
    forM_ boos $ \boo -> do
      -- FIXME improvement: does ghc provide a modifyPtr function?
      origin <- fromIntegral <$> peek64BE (p `plusPtr` boo)
      poke64BE (p `plusPtr` boo) (fromIntegral $ boof origin)

-------------------------------------------------------------------------------
-- LegacyRecord(MessageSet): v0-1
--
-- https://kafka.apache.org/documentation/#messageset
--
-- In versions prior to Kafka 0.10, the only supported message format version
-- (which is indicated in the magic value) was 0. Message format version 1 was
-- introduced with timestamp support in version 0.10.

-- Internal type to help parse RecordV0
--
-- RecordV0 = RecordBase + RecordBodyV0
data RecordBodyV0 = RecordBodyV0
  { attributes :: {-# UNPACK #-} !Int8
  , key        :: !NullableBytes
  , value      :: !NullableBytes
  } deriving (Generic, Show, Eq)

instance Serializable RecordBodyV0

data RecordV0 = RecordV0
  { baseOffset  :: {-# UNPACK #-} !Int64
  , messageSize :: {-# UNPACK #-} !Int32
  , crc         :: {-# UNPACK #-} !Int32
  , magic       :: {-# UNPACK #-} !Int8
  , attributes  :: {-# UNPACK #-} !Int8
  , key         :: !NullableBytes
  , value       :: !NullableBytes
  } deriving (Generic, Show, Eq)

instance Serializable RecordV0
instance NFData RecordV0

minRecordSizeV0 :: Int
minRecordSizeV0 =
  4{- crc -} + 1{- magic -} + 1{- attributes -} + 4{- key -} + 4{- value -}

-- Internal type to help parse all RecordV1
--
-- RecordV1 = RecordBase + RecordBodyV1
data RecordBodyV1 = RecordBodyV1
  { attributes :: {-# UNPACK #-} !Int8
  , timestamp  :: {-# UNPACK #-} !Int64
  , key        :: !NullableBytes
  , value      :: !NullableBytes
  } deriving (Generic, Show, Eq)

instance Serializable RecordBodyV1

data RecordV1 = RecordV1
  { baseOffset  :: {-# UNPACK #-} !Int64
  , messageSize :: {-# UNPACK #-} !Int32
  , crc         :: {-# UNPACK #-} !Int32
  , magic       :: {-# UNPACK #-} !Int8
  , attributes  :: {-# UNPACK #-} !Int8
  , timestamp   :: {-# UNPACK #-} !Int64
  , key         :: !NullableBytes
  , value       :: !NullableBytes
  } deriving (Generic, Show, Eq)

instance Serializable RecordV1
instance NFData RecordV1

minRecordSizeV1 :: Int
minRecordSizeV1 =
    4{- crc -} + 1{- magic -} + 1{- attributes -}
  + 8{- timestamp -} + 4{- key -} + 4{- value -}

class Serializable a => LegacyRecord a where

instance LegacyRecord RecordV0
instance LegacyRecord RecordV1

-- Note that although message sets are represented as an array, they are not
-- preceded by an int32 array size like other array elements in the protocol.
decodeLegacyRecordBatch :: LegacyRecord a => ByteString -> IO (Vector a)
decodeLegacyRecordBatch batchBs = Growing.new >>= decode batchBs
  where
    decode "" !v = Growing.unsafeFreeze v
    decode !bs !v = do
      (r, l) <- runGet' bs
      !v' <- Growing.append v r
      decode l v'

-- | Alter batchLength and crc of a LegacyRecordBatch(MessageSet) bytes.
--
-- The bytes must be a valid MessageSet bytes which only contains one message.
unsafeAlterMessageSetBs :: ByteString -> IO ()
unsafeAlterMessageSetBs (BS fp len) = unsafeWithForeignPtr fp $ \p -> do
  poke32BE (p `plusPtr` 8) (fromIntegral len - 12)
  -- 16: 8 + 4 + 4
  crc <- crc32 <$> BSU.unsafePackCStringLen ((p `plusPtr` 16), (len - 16))
  poke32BE (p `plusPtr` 12) crc

-------------------------------------------------------------------------------
-- RecordBatch: v2 (TODO: Outdated)

-- TODO: Outdated type, use RecordBatch instead.
data RecordBatch1 = RecordBatch1
  { baseOffset           :: {-# UNPACK #-} !Int64
  , batchLength          :: {-# UNPACK #-} !Int32
  , partitionLeaderEpoch :: {-# UNPACK #-} !Int32
  , magic                :: {-# UNPACK #-} !Int8
  , crc                  :: {-# UNPACK #-} !Int32
  , attributes           :: {-# UNPACK #-} !Int16
  , lastOffsetDelta      :: {-# UNPACK #-} !Int32
  , baseTimestamp        :: {-# UNPACK #-} !Int64
  , maxTimestamp         :: {-# UNPACK #-} !Int64
  , producerId           :: {-# UNPACK #-} !Int64
  , producerEpoch        :: {-# UNPACK #-} !Int16
  , baseSequence         :: {-# UNPACK #-} !Int32
  , records              :: !(KaArray RecordV2)
  } deriving (Generic, Show, Eq)

instance Serializable RecordBatch1
instance NFData RecordBatch1

-- Internal type to help parse all RecordBatch
--
-- RecordBatch = RecordBase + CRC32 + RecordBodyV2
data RecordBodyV2 = RecordBodyV2
  { attributes      :: {-# UNPACK #-} !Int16
  , lastOffsetDelta :: {-# UNPACK #-} !Int32
  , baseTimestamp   :: {-# UNPACK #-} !Int64
  , maxTimestamp    :: {-# UNPACK #-} !Int64
  , producerId      :: {-# UNPACK #-} !Int64
  , producerEpoch   :: {-# UNPACK #-} !Int16
  , baseSequence    :: {-# UNPACK #-} !Int32
  , records         :: !(KaArray RecordV2)
  } deriving (Generic, Show, Eq)

instance Serializable RecordBodyV2

type RecordHeader = (RecordHeaderKey, RecordHeaderValue)

data RecordV2 = RecordV2
  { length         :: {-# UNPACK #-} !VarInt32
  , attributes     :: {-# UNPACK #-} !Int8
  , timestampDelta :: {-# UNPACK #-} !VarInt64
  , offsetDelta    :: {-# UNPACK #-} !VarInt32
  , key            :: !RecordKey
  , value          :: !RecordValue
  , headers        :: !(RecordArray RecordHeader)
  } deriving (Generic, Show, Eq)

instance Serializable RecordV2
instance NFData RecordV2

-- | The same as 'RecordV2' but without length.
--
-- This may useful for constructing 'RecordV2' bytes.
--
-- @
-- let r = RecordV2_ ...
--     rb = put r
--     len = builderLength rb
--     bb = put @VarInt32 len <> rb
--     lbs = toLazyByteString bb
-- @
data RecordV2_ = RecordV2_
  { attributes     :: {-# UNPACK #-} !Int8
  , timestampDelta :: {-# UNPACK #-} !VarInt64
  , offsetDelta    :: {-# UNPACK #-} !VarInt32
  , key            :: !RecordKey
  , value          :: !RecordValue
  , headers        :: !(RecordArray RecordHeader)
  } deriving (Generic, Show, Eq)

instance Serializable RecordV2_
instance NFData RecordV2_

-- | The same as 'RecordBatch' but without records.
--
-- This may useful for constructing 'RecordBatch' bytes.
data RecordBatch_ = RecordBatch_
  { baseOffset           :: {-# UNPACK #-} !Int64
  , batchLength          :: {-# UNPACK #-} !Int32
  , partitionLeaderEpoch :: {-# UNPACK #-} !Int32
  , magic                :: {-# UNPACK #-} !Int8
  , crc                  :: {-# UNPACK #-} !Int32
  , attributes           :: {-# UNPACK #-} !Int16
  , lastOffsetDelta      :: {-# UNPACK #-} !Int32
  , baseTimestamp        :: {-# UNPACK #-} !Int64
  , maxTimestamp         :: {-# UNPACK #-} !Int64
  , producerId           :: {-# UNPACK #-} !Int64
  , producerEpoch        :: {-# UNPACK #-} !Int16
  , baseSequence         :: {-# UNPACK #-} !Int32
  } deriving (Generic, Show, Eq)

instance Serializable RecordBatch_
instance NFData RecordBatch_

-- | Alter batchLength and crc of a 'RecordBatch' bytes.
--
-- The bytes must be a valid 'RecordBatch' bytes.
unsafeAlterRecordBatchBs :: ByteString -> IO ()
unsafeAlterRecordBatchBs (BS fp len) = unsafeWithForeignPtr fp $ \p -> do
  poke32BE (p `plusPtr` 8) (fromIntegral len - 12)
  -- 21: 8 + 4 + 4 + 1 + 4
  crc <- crc32c <$> BSU.unsafePackCStringLen ((p `plusPtr` 21), (len - 21))
  poke32BE (p `plusPtr` 17) crc

-------------------------------------------------------------------------------
-- Misc

-- for non-nullable array
pattern NonNullKaArray :: Vector a -> KaArray a
pattern NonNullKaArray vec <- (unNonNullKaArray -> vec) where
  NonNullKaArray vec = KaArray (Just vec)
{-# COMPLETE NonNullKaArray #-}

unNonNullKaArray :: KaArray a -> Vector a
unNonNullKaArray =
  fromMaybe (error "non-nullable field was serialized as null") . unKaArray

kaArrayToCompact :: KaArray a -> CompactKaArray a
kaArrayToCompact = CompactKaArray . unKaArray
{-# INLINE kaArrayToCompact #-}

kaArrayFromCompact :: CompactKaArray a -> KaArray a
kaArrayFromCompact = KaArray . unCompactKaArray
{-# INLINE kaArrayFromCompact #-}
