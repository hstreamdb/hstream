module Kafka.Protocol.Encoding.Encode
  ( Builder
  , builderLength
  , toLazyByteString
    -- * Builders
  , putBool
  , putInt8
  , putInt16
  , putInt32
  , putInt64
  , putWord8
  , putWord32
  , putVarInt32
  , putVarInt64
  , putVarWord32
  , putVarWord64
  -- TODO
  --, putUUID
  , putDouble
  , putString
  , putCompactString
  , putNullableString
  , putCompactNullableString
  , putBytes
  , putCompactBytes
  , putNullableBytes
  , putCompactNullableBytes
  , putVarBytesInRecord
  , putVarStringInRecord
  ) where

import           Data.Bits
import           Data.ByteString               (ByteString)
import qualified Data.ByteString               as BS
import qualified Data.ByteString.Builder       as BB
import qualified Data.ByteString.Builder.Extra as BB
import qualified Data.ByteString.Lazy          as BL
import           Data.Char                     (ord)
import           Data.Int
import           Data.Monoid
import           Data.String                   (IsString (..))
import           Data.Text                     (Text)
import qualified Data.Text.Encoding            as Text
import           Data.Word

-------------------------------------------------------------------------------

putBool :: Bool -> Builder
putBool True  = putWord8 1
putBool False = putWord8 0

putInt8 :: Int8 -> Builder
putInt8 i = Builder (Sum 1) (BB.int8 i)
{-# INLINE putInt8 #-}

putInt16 :: Int16 -> Builder
putInt16 i = Builder (Sum 2) (BB.int16BE i)
{-# INLINE putInt16 #-}

putInt32 :: Int32 -> Builder
putInt32 i = Builder (Sum 4) (BB.int32BE i)
{-# INLINE putInt32 #-}

putInt64 :: Int64 -> Builder
putInt64 i = Builder (Sum 8) (BB.int64BE i)
{-# INLINE putInt64 #-}

putWord8 :: Word8 -> Builder
putWord8 w = Builder (Sum 1) (BB.word8 w)
{-# INLINE putWord8 #-}

putWord32 :: Word32 -> Builder
putWord32 w = Builder (Sum 4) (BB.word32BE w)
{-# INLINE putWord32 #-}

putVarInt32 :: Int32 -> Builder
putVarInt32 i =
  let i' = (i `shiftL` 1) `xor` (i `shiftR` 31)  -- encode zigzag
   in putVarWord32 (fromIntegral i')

putVarInt64 :: Int64 -> Builder
putVarInt64 i =
  let i' = (i `shiftL` 1) `xor` (i `shiftR` 63)  -- encode zigzag
   in putVarWord64 (fromIntegral i')

putVarWord32 :: Word32 -> Builder
putVarWord32 = go mempty
  where
    go !ret !value =
      if value .&. 0xffffff80 /= 0
         then
           let ret' = ret <> putWord8 ((fromIntegral value .&. 0x7f) .|. 0x80)
               value' = value `shiftR` 7
            in go ret' value'
         else ret <> putWord8 (fromIntegral value)
{-# INLINE putVarWord32 #-}

putVarWord64 :: Word64 -> Builder
putVarWord64 = go mempty
  where
    go !ret !value =
      if value .&. 0xffffffffffffff80 /= 0
         then
           let ret' = ret <> putWord8 ((fromIntegral value .&. 0x7f) .|. 0x80)
               value' = value `shiftR` 7
            in go ret' value'
         else ret <> putWord8 (fromIntegral value)
{-# INLINE putVarWord64 #-}

-- TODO
--putUUID = undefined

putDouble :: Double -> Builder
putDouble x = Builder (Sum 8) (BB.doubleBE x)
{-# INLINE putDouble #-}

-- FIXME: do not copy for large bs(whats the length for this?)
putString :: Text -> Builder
putString x =
  let !bs = Text.encodeUtf8 x
      !l = BS.length bs
      b = Builder (Sum (fromIntegral l)) (BB.byteStringCopy bs)
   in putInt16 (fromIntegral l) <> b
{-# INLINE putString #-}

-- FIXME: do not copy for large bs(whats the length for this?)
putCompactString :: Text -> Builder
putCompactString x =
  let !bs = Text.encodeUtf8 x
      !l = BS.length bs
      b = Builder (Sum (fromIntegral l)) (BB.byteStringCopy bs)
   in putVarWord32 (fromIntegral l + 1) <> b
{-# INLINE putCompactString #-}

putNullableString :: Maybe Text -> Builder
putNullableString Nothing  = putInt16 (-1)
putNullableString (Just x) = putString x

putCompactNullableString :: Maybe Text -> Builder
putCompactNullableString Nothing  = putVarWord32 0
putCompactNullableString (Just x) = putCompactString x

-- FIXME: do not copy for large bs(whats the length for this?)
putBytes :: ByteString -> Builder
putBytes bs =
  let !l = BS.length bs
      b = Builder (Sum (fromIntegral l)) (BB.byteStringCopy bs)
   in putInt32 (fromIntegral l) <> b
{-# INLINE putBytes #-}

-- FIXME: do not copy for large bs(whats the length for this?)
putCompactBytes :: ByteString -> Builder
putCompactBytes bs =
  let !l = BS.length bs
      b = Builder (Sum (fromIntegral l)) (BB.byteStringCopy bs)
   in putVarWord32 (fromIntegral l + 1) <> b
{-# INLINE putCompactBytes #-}

putNullableBytes :: Maybe ByteString -> Builder
putNullableBytes Nothing  = putInt32 (-1)
putNullableBytes (Just x) = putBytes x

putCompactNullableBytes :: Maybe ByteString -> Builder
putCompactNullableBytes Nothing  = putVarWord32 0
putCompactNullableBytes (Just x) = putCompactBytes x

-- | Record key or value
--
-- ref: https://kafka.apache.org/documentation/#record
putVarBytesInRecord :: Maybe ByteString -> Builder
putVarBytesInRecord Nothing = putVarInt32 (-1)
putVarBytesInRecord (Just bs) =
  let !l = BS.length bs
      b = Builder (Sum (fromIntegral l)) (BB.byteStringCopy bs)
   in putVarInt32 (fromIntegral l) <> b

-- | Record header key
--
-- ref: https://kafka.apache.org/documentation/#record
putVarStringInRecord :: Text -> Builder
putVarStringInRecord x =
  let !bs = Text.encodeUtf8 x
      !l = BS.length bs
      b = Builder (Sum (fromIntegral l)) (BB.byteStringCopy bs)
   in putVarInt32 (fromIntegral l) <> b

-------------------------------------------------------------------------------

-- Fork from: https://github.com/awakesecurity/proto3-wire

data Builder = Builder
  { builderLen :: {-# UNPACK #-} !(Sum Word)
  , _builderBS :: !BB.Builder
  }

builderLength :: Builder -> Word
builderLength = getSum . builderLen

instance Semigroup Builder where
  {-# INLINE (<>) #-}
  Builder s1 b1 <> Builder s2 b2 = Builder (s1 <> s2) (b1 <> b2)

instance Monoid Builder where
  {-# INLINE mempty #-}
  mempty = Builder mempty mempty
  {-# INLINE mappend #-}
  mappend = (<>)

instance Show Builder where
  show = show . toLazyByteString

instance IsString Builder where
  fromString = stringUtf8

-- | Create a lazy `BL.ByteString` from a `Builder`
toLazyByteString :: Builder -> BL.ByteString
toLazyByteString (Builder (Sum len) bb) =
    BB.toLazyByteStringWith strat BL.empty bb
  where
    -- If the supplied length is accurate then we will perform just
    -- one allocation.  An inaccurate length would indicate a bug
    -- in one of the primitives that produces a 'Builder'.
    strat = BB.safeStrategy (fromIntegral len) BB.defaultChunkSize
-- NOINLINE to avoid bloating caller; see docs for 'BB.toLazyByteStringWith'.
{-# NOINLINE toLazyByteString #-}

stringUtf8 :: String -> Builder
stringUtf8 s = Builder (Sum (len 0 s)) (BB.stringUtf8 s)
  where
    len !n []      = n
    len !n (h : t) = len (n + utf8Width h) t
-- INLINABLE so that if the input is constant, the
-- compiler has the opportunity to precompute its length.
{-# INLINABLE stringUtf8 #-}

utf8Width :: Char -> Word
utf8Width c = case ord c of
  o | o <= 0x007F -> 1
    | o <= 0x07FF -> 2
    | o <= 0xFFFF -> 3
    | otherwise   -> 4
{-# INLINE utf8Width #-}
