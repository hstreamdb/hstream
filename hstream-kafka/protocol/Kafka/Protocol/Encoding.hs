{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DefaultSignatures     #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE MultiWayIf            #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE ViewPatterns          #-}
-- As of GHC 8.8.1, GHC started complaining about -optP--cpp when profling
-- is enabled. See https://gitlab.haskell.org/ghc/ghc/issues/17185.
{-# OPTIONS_GHC -pgmP "hpp --cpp -P" #-}

module Kafka.Protocol.Encoding
  ( Serializable (..)
  , putEither, getEither
  , putMaybe, getMaybe
  , runGet
  , runGet'
  , runPut
  , runPutLazy
  , DecodeError (..)
    -- * Defined types
  , VarInt32 (..)
  , VarInt64 (..)
  , NullableString
  , CompactString (..)
  , CompactNullableString (..)
  , NullableBytes
  , CompactBytes (..)
  , CompactNullableBytes (..)
  , TaggedFields (EmptyTaggedFields)  -- TODO
  , KaArray (..)
  , CompactKaArray (..)
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
  , RecordBatch (..)
  , RecordBatch_ (..)
  , unsafeAlterRecordBatchBs
  , unsafeAlterMessageSetBs
  , RecordKey (..)
  , RecordValue (..)
  , RecordArray (..)
  , RecordHeader
  , RecordHeaderKey (..)
  , RecordHeaderValue (..)
    -- ** Helpers
  , decodeLegacyRecordBatch
  , decodeRecordMagic
  , decodeNextRecordOffset
    -- ** Misc
  , pattern NonNullKaArray
  , unNonNullKaArray
  , kaArrayToCompact
  , kaArrayFromCompact
  , decodeBatchRecordsForProduce
  , unsafeAlterBatchRecordsBsForProduce
    -- * Internals
    -- ** Parser
  , Parser
  , runParser
  , runParser'
  , Result (..)
  , takeBytes
    -- ** Builder
  , Builder
  , builderLength
  , toLazyByteString
  ) where

import           Control.DeepSeq                  (NFData)
import           Control.Exception
import           Control.Monad
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as BS
import           Data.ByteString.Internal         (ByteString (BS), w2c)
import qualified Data.ByteString.Lazy             as BL
import qualified Data.ByteString.Unsafe           as BSU
import           Data.Digest.CRC32                (crc32)
import           Data.Digest.CRC32C               (crc32c)
import           Data.Int
import           Data.Maybe
import           Data.String                      (IsString)
import           Data.Text                        (Text)
import           Data.Typeable                    (Typeable, showsTypeRep,
                                                   typeOf)
import           Data.Vector                      (Vector)
import qualified Data.Vector                      as V
import           Data.Word
import           Foreign.Ptr                      (plusPtr)
import           GHC.ForeignPtr                   (unsafeWithForeignPtr)
import           GHC.Generics

import qualified HStream.Base.Growing             as Growing
import           Kafka.Protocol.Encoding.Encode
import           Kafka.Protocol.Encoding.Internal
import           Kafka.Protocol.Encoding.Parser

-------------------------------------------------------------------------------

class Typeable a => Serializable a where
  get :: Parser a

  default get :: (Generic a, GSerializable (Rep a)) => Parser a
  get = to <$> gget

  put :: a -> Builder

  default put :: (Generic a, GSerializable (Rep a)) => a -> Builder
  put a = gput (from a)

class GSerializable f where
  gget :: Parser (f a)
  gput :: f a -> Builder

-- | Unit: used for constructors without arguments
instance GSerializable U1 where
  gget = pure U1
  gput U1 = mempty

-- | Products: encode multiple arguments to constructors
instance (GSerializable a, GSerializable b) => GSerializable (a :*: b) where
  gget = do
    !a <- gget
    !b <- gget
    pure $ a :*: b
  gput (a :*: b) = gput a <> gput b

-- | Meta-information
instance (GSerializable a) => GSerializable (M1 i c a) where
  gget = M1 <$> gget
  gput (M1 x) = gput x

instance (Serializable a) => GSerializable (K1 i a) where
  gget = K1 <$> get
  gput (K1 x) = put x

-- There is no easy way to support Sum types for Generic instance.
--
-- So here we give a special case for Either
putEither :: (Serializable a, Serializable b) => Either a b -> Builder
putEither (Left x)  = put x
putEither (Right x) = put x
{-# INLINE putEither #-}

-- There is no way to support Sum types for Generic instance.
--
-- So here we give a special case for Either
getEither
  :: (Serializable a, Serializable b)
  => Bool  -- ^ True for Right, False for Left
  -> Parser (Either a b)
getEither True  = Right <$> get
getEither False = Left <$> get
{-# INLINE getEither #-}

putMaybe :: (Serializable a) => Maybe a -> Builder
putMaybe (Just x) = put x
putMaybe Nothing  = mempty
{-# INLINE putMaybe #-}

getMaybe
  :: (Serializable a)
  => Bool  -- ^ True for Just, False for Nothing
  -> Parser (Maybe a)
getMaybe True  = Just <$> get
getMaybe False = pure Nothing
{-# INLINE getMaybe #-}

-------------------------------------------------------------------------------

newtype DecodeError = DecodeError String
  deriving (Show)

instance Exception DecodeError

runParser' :: Typeable r => Parser r -> ByteString -> IO (r, ByteString)
runParser' parser bs = do
  result <- runParser parser bs
  case result of
    Done l r   -> pure (r, l)
    Fail _ err -> throwIO $ DecodeError $ "Fail, " <> err
    More _     -> throwIO $ DecodeError $ showsTypeRep (typeOf parser) ", need more"
{-# INLINE runParser' #-}

runGet :: Serializable a => ByteString -> IO a
runGet bs = do
  (r, l) <- runParser' get bs
  if BS.null l then pure r
               else throwIO $ DecodeError $
                      "runGet done, but left " <> map w2c (BS.unpack l)
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
-- Extra Primitive Types

newtype VarInt32 = VarInt32 { unVarInt32 :: Int32 }
  deriving newtype (Show, Num, Integral, Real, Enum, Ord, Eq, Bounded, NFData)

newtype VarInt64 = VarInt64 { unVarInt64 :: Int64 }
  deriving newtype (Show, Num, Integral, Real, Enum, Ord, Eq, Bounded, NFData)

type NullableString = Maybe Text

newtype CompactString = CompactString { unCompactString :: Text }
  deriving newtype (Show, Eq, Ord, IsString, Monoid, Semigroup)

newtype CompactNullableString = CompactNullableString
  { unCompactNullableString :: Maybe Text }
  deriving newtype (Show, Eq, Ord)

type NullableBytes = Maybe ByteString

newtype CompactBytes = CompactBytes { unCompactBytes :: ByteString }
  deriving newtype (Show, Eq, Ord, IsString, Monoid, Semigroup)

newtype CompactNullableBytes = CompactNullableBytes
  { unCompactNullableBytes :: Maybe ByteString }
  deriving newtype (Show, Eq, Ord)

-- TODO: Currently we just ignore the tagged fields
data TaggedFields = EmptyTaggedFields
  deriving (Show, Eq)

newtype KaArray a = KaArray
  { unKaArray :: Maybe (Vector a) }
  deriving newtype (Show, Eq, Ord, NFData)

instance Functor KaArray where
  fmap f (KaArray xs) = KaArray $ fmap f <$> xs

newtype CompactKaArray a = CompactKaArray
  { unCompactKaArray :: Maybe (Vector a) }
  deriving newtype (Show, Eq, Ord)

instance Functor CompactKaArray where
  fmap f (CompactKaArray xs) = CompactKaArray $ fmap f <$> xs

newtype RecordKey = RecordKey { unRecordKey :: Maybe ByteString }
  deriving newtype (Show, Eq, Ord, NFData)

newtype RecordValue = RecordValue { unRecordValue :: Maybe ByteString }
  deriving newtype (Show, Eq, Ord, NFData)

newtype RecordArray a = RecordArray { unRecordArray :: Vector a }
  deriving newtype (Show, Eq, Ord, NFData)

newtype RecordHeaderKey = RecordHeaderKey { unRecordHeaderKey :: Text }
  deriving newtype (Show, Eq, Ord, IsString, Monoid, Semigroup, NFData)

newtype RecordHeaderValue = RecordHeaderValue
  { unRecordHeaderValue :: Maybe ByteString }
  deriving newtype (Show, Eq, Ord, NFData)

-------------------------------------------------------------------------------
-- Instances

#define INSTANCE(ty, n, getfun, patmt, pat) \
instance Serializable ty where \
  get = getfun get##n; \
  {-# INLINE get #-}; \
  put patmt = put##n pat; \
  {-# INLINE put #-}

#define INSTANCE_BUILTIN(t) INSTANCE(t, t, , , )
#define INSTANCE_BUILTIN_1(t, n) INSTANCE(t, n, , , )
#define INSTANCE_NEWTYPE(t) INSTANCE(t, t, t <$>, (t x), x)
#define INSTANCE_NEWTYPE_1(t, n) INSTANCE(t, n, t <$>, (t x), x)

INSTANCE_BUILTIN(Bool)
INSTANCE_BUILTIN(Int8)
INSTANCE_BUILTIN(Int16)
INSTANCE_BUILTIN(Int32)
INSTANCE_BUILTIN(Int64)
INSTANCE_BUILTIN(Word32)
INSTANCE_BUILTIN(Double)
INSTANCE_BUILTIN(NullableString)
INSTANCE_BUILTIN(NullableBytes)
INSTANCE_BUILTIN_1(Text, String)
INSTANCE_BUILTIN_1(ByteString, Bytes)

INSTANCE_NEWTYPE(VarInt32)
INSTANCE_NEWTYPE(VarInt64)
INSTANCE_NEWTYPE(CompactString)
INSTANCE_NEWTYPE(CompactNullableString)
INSTANCE_NEWTYPE(CompactBytes)
INSTANCE_NEWTYPE(CompactNullableBytes)

INSTANCE_NEWTYPE_1(RecordKey, RecordNullableBytes)
INSTANCE_NEWTYPE_1(RecordValue, RecordNullableBytes)
INSTANCE_NEWTYPE_1(RecordHeaderKey, RecordString)
INSTANCE_NEWTYPE_1(RecordHeaderValue, RecordNullableBytes)

instance Serializable TaggedFields where
  get = do !n <- fromIntegral <$> getVarWord32
           replicateM_ n $ do
             tag <- getVarWord32
             dataLen <- getVarWord32
             val <- takeBytes (fromIntegral dataLen)
             pure (tag, val)
           pure EmptyTaggedFields
  {-# INLINE get #-}

  put _ = putVarWord32 0
  {-# INLINE put #-}

instance Serializable a => Serializable (KaArray a) where
  get = getArray
  {-# INLINE get #-}
  put = putArray
  {-# INLINE put #-}

instance Serializable a => Serializable (CompactKaArray a) where
  get = CompactKaArray <$> getCompactArray
  {-# INLINE get #-}
  put (CompactKaArray xs) = putCompactArray xs
  {-# INLINE put #-}

instance Serializable a => Serializable (RecordArray a) where
  get = RecordArray <$> getRecordArray
  {-# INLINE get #-}
  put (RecordArray xs) = putRecordArray xs
  {-# INLINE put #-}

instance
  ( Serializable a
  , Serializable b
  ) => Serializable (a, b)
instance
  ( Serializable a
  , Serializable b
  , Serializable c
  ) => Serializable (a, b, c)
instance
  ( Serializable a
  , Serializable b
  , Serializable c
  , Serializable d
  ) => Serializable (a, b, c, d)
instance
  ( Serializable a
  , Serializable b
  , Serializable c
  , Serializable d
  , Serializable e
  ) => Serializable (a, b, c, d, e)

-------------------------------------------------------------------------------
-- Records

data BatchRecord
  = BatchRecordV0 RecordV0
  | BatchRecordV1 RecordV1
  | BatchRecordV2 RecordBatch
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
                  throwIO $ DecodeError $ "Invalid messageSize"
                when shouldValidateCrc $ do
                  -- NOTE: pass the origin inputs to validLegacyCrc, not the bs'
                  --
                  -- The crc field contains the CRC32 (and not CRC-32C) of the
                  -- subsequent message bytes (i.e. from magic byte to the value).
                  validLegacyCrc (fromIntegral batchLength) crc bs
                (RecordBodyV0{..}, remainder) <- runGet' @RecordBodyV0 bs'
                !v' <- Growing.append v (BatchRecordV0 RecordV0{..})
                decode (len + 1) remainder v'
        1 -> do let crc = partitionLeaderEpochOrCrc
                    messageSize = batchLength
                when (messageSize < fromIntegral minRecordSizeV1) $
                  throwIO $ DecodeError $ "Invalid messageSize"
                when shouldValidateCrc $ do
                  -- NOTE: pass the origin inputs to validLegacyCrc, not the bs'
                  --
                  -- The crc field contains the CRC32 (and not CRC-32C) of the
                  -- subsequent message bytes (i.e. from magic byte to the value).
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
                    throwIO $ DecodeError "Invalid CRC32"
                (RecordBodyV2{..}, remainder) <- runGet' @RecordBodyV2 bs''
                !v' <- Growing.append v (BatchRecordV2 RecordBatch{..})
                let !batchLen = maybe 0 V.length (unKaArray records)
                -- Actually, there should be only one batch record here, but
                -- we don't require it.
                decode (len + batchLen) remainder v'
        _ -> throwIO $ DecodeError $ "Invalid magic " <> show magic
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

-- Internal type to help parse all Record version.
--
-- Common Record base for all versions.
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

-- Internal type to help parse all Record version.
--
-- RecordV0 = RecordBase + RecordBodyV0
data RecordBodyV0 = RecordBodyV0
  { attributes :: {-# UNPACK #-} !Int8
  , key        :: !NullableBytes
  , value      :: !NullableBytes
  } deriving (Generic, Show, Eq)

instance Serializable RecordBodyV0

-- Internal type to help parse all Record version.
--
-- RecordV1 = RecordBase + RecordBodyV1
data RecordBodyV1 = RecordBodyV1
  { attributes :: {-# UNPACK #-} !Int8
  , timestamp  :: {-# UNPACK #-} !Int64
  , key        :: !NullableBytes
  , value      :: !NullableBytes
  } deriving (Generic, Show, Eq)

instance Serializable RecordBodyV1

-- Internal type to help parse all Record version.
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

validLegacyCrc :: Int -> Int32 -> ByteString -> IO ()
validLegacyCrc batchLength crc bs = do
  crcPayload <- getLegacyCrcPayload batchLength bs
  when (fromIntegral (crc32 crcPayload) /= crc) $
    throwIO $ DecodeError "Invalid CRC32"
{-# INLINE validLegacyCrc #-}

getLegacyCrcPayload :: Int -> ByteString -> IO ByteString
getLegacyCrcPayload msgSize bs =
  let parser = do void $ takeBytes 16  -- [offset(8) message_size(4) crc(4) ...]
                  takeBytes (msgSize - 4)
   in fst <$> runParser' parser bs
{-# INLINE getLegacyCrcPayload #-}

-------------------------------------------------------------------------------
-- LegacyRecord(MessageSet): v0-1
--
-- https://kafka.apache.org/documentation/#messageset
--
-- In versions prior to Kafka 0.10, the only supported message format version
-- (which is indicated in the magic value) was 0. Message format version 1 was
-- introduced with timestamp support in version 0.10.

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
-- RecordBatch: v2
--
-- Ref: https://kafka.apache.org/documentation/#recordbatch
--
-- Introduced in Kafka 0.11.0

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

data RecordBatch = RecordBatch
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

instance Serializable RecordBatch
instance NFData RecordBatch

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

-- Currently this is used for Produce handler.
decodeBatchRecordsForProduce :: Bool -> ByteString -> IO (Int, [Int])
decodeBatchRecordsForProduce shouldValidateCrc = decode 0 0 []
  where
    decode len _consumed offsetOffsets ""  = pure (len, offsetOffsets)
    decode !len !consumed !offsetOffsets !bs = do
      (RecordBase{..}, bs') <- runGet' @RecordBase bs
      case magic of
        0 -> do let crc = partitionLeaderEpochOrCrc
                    messageSize = batchLength
                when (messageSize < fromIntegral minRecordSizeV0) $
                  throwIO $ DecodeError $ "Invalid messageSize"
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
                  throwIO $ DecodeError $ "Invalid messageSize"
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
                    throwIO $ DecodeError "Invalid CRC32"
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
        _ -> throwIO $ DecodeError $ "Invalid magic " <> show magic
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
-- Internals

-- | Represents a sequence of objects of a given type T.
--
-- Type T can be either a primitive type (e.g. STRING) or a structure.
-- First, the length N is given as an INT32. Then N instances of type T follow.
-- A null array is represented with a length of -1. In protocol documentation
-- an array of T instances is referred to as [T].
getArray :: Serializable a => Parser (KaArray a)
getArray = do
  !n <- getInt32
  if n >= 0
     then KaArray . Just <$!> V.replicateM (fromIntegral n) get
     else do
       if n == (-1)
          then pure $ KaArray Nothing
          else fail $! "Length of null array must be -1 " <> show n

-- | Represents a sequence of objects of a given type T.
--
-- Type T can be either a primitive type (e.g. STRING) or a structure. First,
-- the length N + 1 is given as an UNSIGNED_VARINT. Then N instances of type T
-- follow. A null array is represented with a length of 0. In protocol
-- documentation an array of T instances is referred to as [T].
getCompactArray :: Serializable a => Parser (Maybe (Vector a))
getCompactArray = do
  !n_1 <- fromIntegral <$> getVarWord32
  let !n = n_1 - 1
  if n >= 0
     then Just <$!> V.replicateM n get
     else do
       if n == (-1)
          then pure Nothing
          else fail $! "Length of null compact array must be -1 " <> show n

putArray :: Serializable a => KaArray a -> Builder
putArray (KaArray (Just xs)) =
  let !len = V.length xs
      put_len = putInt32 (fromIntegral len)
   in put_len <> V.foldl' (\s x -> s <> put x) mempty xs
putArray (KaArray Nothing) = putInt32 (-1)

putCompactArray :: Serializable a => Maybe (Vector a) -> Builder
putCompactArray (Just xs) =
  let !len = V.length xs
      put_len = putVarWord32 (fromIntegral len + 1)
   in put_len <> V.foldl' (\s x -> s <> put x) mempty xs
putCompactArray Nothing = putVarWord32 0

getRecordArray :: Serializable a => Parser (Vector a)
getRecordArray = do
  !n <- fromIntegral <$> getVarInt32
  if | n > 0 -> V.replicateM n get
     | n == 0 -> pure V.empty
     | otherwise -> fail $! "Length of RecordArray must not be negative " <> show n

putRecordArray :: Serializable a => Vector a -> Builder
putRecordArray xs =
  let !len = V.length xs
      put_len = putVarInt32 (fromIntegral len)
   in put_len <> V.foldl' (\s x -> s <> put x) mempty xs
