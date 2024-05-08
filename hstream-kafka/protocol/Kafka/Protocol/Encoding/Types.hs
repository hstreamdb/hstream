{-# LANGUAGE CPP               #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE MultiWayIf        #-}
-- As of GHC 8.8.1, GHC started complaining about -optP--cpp when profling
-- is enabled. See https://gitlab.haskell.org/ghc/ghc/issues/17185.
{-# OPTIONS_GHC -pgmP "hpp --cpp -P" #-}

module Kafka.Protocol.Encoding.Types
  ( Serializable (..)
  , putEither, getEither
  , putMaybe, getMaybe
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
  , RecordKey (..)
  , RecordValue (..)
  , RecordHeaderKey (..)
  , RecordArray (..)
  , RecordHeaderValue (..)
  ) where

import           Control.DeepSeq                (NFData)
import           Control.Monad
import           Data.ByteString                (ByteString)
import           Data.Int
import           Data.String                    (IsString)
import           Data.Text                      (Text)
import           Data.Typeable                  (Typeable)
import           Data.Vector                    (Vector)
import qualified Data.Vector                    as V
import           Data.Word
import           GHC.Generics

import           Kafka.Protocol.Encoding.Encode
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

INSTANCE_NEWTYPE_1(RecordKey, VarBytesInRecord)
INSTANCE_NEWTYPE_1(RecordValue, VarBytesInRecord)
INSTANCE_NEWTYPE_1(RecordHeaderKey, VarStringInRecord)
INSTANCE_NEWTYPE_1(RecordHeaderValue, VarBytesInRecord)

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
