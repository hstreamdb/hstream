{-# LANGUAGE CPP               #-}
{-# LANGUAGE DefaultSignatures #-}
-- As of GHC 8.8.1, GHC started complaining about -optP--cpp when profling
-- is enabled. See https://gitlab.haskell.org/ghc/ghc/issues/17185.
{-# OPTIONS_GHC -pgmP "hpp --cpp -P" #-}

module Kafka.Protocol.Encoding
  ( Serializable (..)
  , runGet
  , runPut
  , runPutLazy
  , DecodeError (..)
    -- * User defined types
  , VarInt32 (..)
  , VarInt64 (..)
  , NullableString
  , CompactString (..)
  , CompactNullableString (..)
  , NullableBytes
  , CompactBytes (..)
  , CompactNullableBytes (..)
  , KaArray
  , CompactKaArray (..)
    -- * Internals
  , Parser
  , runParser
  , Result (..)
  , Builder
  , toLazyByteString
  ) where

import           Control.Exception
import           Control.Monad
import           Data.ByteString                (ByteString)
import qualified Data.ByteString.Lazy           as BL
import           Data.Int
import           Data.String                    (IsString)
import           Data.Text                      (Text)
import           Data.Vector                    (Vector)
import qualified Data.Vector                    as V
import           Data.Word
import           GHC.Generics

import           Kafka.Protocol.Encoding.Encode
import           Kafka.Protocol.Encoding.Parser

-------------------------------------------------------------------------------

class Serializable a where
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

-------------------------------------------------------------------------------

newtype DecodeError = DecodeError String
  deriving (Show)

instance Exception DecodeError

runGet :: Serializable a => ByteString -> IO a
runGet bs = do
  result <- runParser get bs
  case result of
    Done "" r  -> pure r
    Done l _   -> throwIO $ DecodeError $ "Done, but left " <> show l
    Fail _ err -> throwIO $ DecodeError $ "Fail, " <> err
    More _     -> throwIO $ DecodeError "Need more"
{-# INLINE runGet #-}

runPutLazy :: Serializable a => a -> BL.ByteString
runPutLazy = toLazyByteString . put
{-# INLINE runPutLazy #-}

runPut :: Serializable a => a -> ByteString
runPut = BL.toStrict . toLazyByteString . put
{-# INLINE runPut #-}

newtype VarInt32 = VarInt32 { unVarInt32 :: Int32 }
  deriving newtype (Show, Num, Integral, Real, Enum, Ord, Eq, Bounded)

newtype VarInt64 = VarInt64 { unVarInt64 :: Int64 }
  deriving newtype (Show, Num, Integral, Real, Enum, Ord, Eq, Bounded)

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

type KaArray a = Maybe (Vector a)

newtype CompactKaArray a = CompactKaArray
  { unCompactKaArray :: Maybe (Vector a) }
  deriving newtype (Show, Eq, Ord)

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

-------------------------------------------------------------------------------
-- Internal: Array

-- | Represents a sequence of objects of a given type T.
--
-- Type T can be either a primitive type (e.g. STRING) or a structure.
-- First, the length N is given as an INT32. Then N instances of type T follow.
-- A null array is represented with a length of -1. In protocol documentation
-- an array of T instances is referred to as [T].
getArray :: Serializable a => Parser (Maybe (Vector a))
getArray = do
  !n <- getInt32
  if n >= 0
     then Just <$!> V.replicateM (fromIntegral n) get
     else do
       if n == (-1)
          then pure Nothing
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

putArray :: Serializable a => Maybe (Vector a) -> Builder
putArray (Just xs) =
  let !len = V.length xs
      put_len = putInt32 (fromIntegral len)
   in put_len <> V.foldl' (\s x -> s <> put x) mempty xs
putArray Nothing = putInt32 (-1)

putCompactArray :: Serializable a => Maybe (Vector a) -> Builder
putCompactArray (Just xs) =
  let !len = V.length xs
      put_len = putVarWord32 (fromIntegral len + 1)
   in put_len <> V.foldl' (\s x -> s <> put x) mempty xs
putCompactArray Nothing = putVarWord32 0
