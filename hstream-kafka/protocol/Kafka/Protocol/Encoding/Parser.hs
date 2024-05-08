{-# LANGUAGE CPP #-}

-- Ref: https://kafka.apache.org/protocol.html#protocol_types

module Kafka.Protocol.Encoding.Parser
  ( Parser (..)
  , Result (..)
  , runParser
    -- * Parsers
  , getBool
  , getInt8
  , getInt16
  , getInt32
  , getInt64
  , getWord32
  , getVarInt32
  , getVarInt64
  , getVarWord32
  , getVarWord64
  -- TODO
  --, getUUID
  , getDouble
  , getString
  , getCompactString
  , getNullableString
  , getCompactNullableString
  , getBytes
  , getCompactBytes
  , getNullableBytes
  , getCompactNullableBytes
  , getVarBytesInRecord
  , getVarStringInRecord
    -- * Internals
  , takeBytes
  , dropBytes
  , directDropBytes
  , unsafePeekInt32At
  , fromIO
  ) where

import           Control.Monad
import           Data.Bits
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString                  as BS
import qualified Data.ByteString.Internal         as BSI
import           Data.Int
import           Data.Text                        (Text)
import qualified Data.Text.Encoding               as Text
import           Data.Word
import           Foreign.Ptr                      (plusPtr)
import           GHC.Float                        (castWord64ToDouble)

import           Kafka.Protocol.Encoding.Internal

-------------------------------------------------------------------------------

-- | Represents a boolean value in a byte.
--
-- Values 0 and 1 are used to represent false and true respectively. When
-- reading a boolean value, any non-zero value is considered true.
getBool :: Parser Bool
getBool = (/= 0) <$!> anyWord8

-- | Represents an integer between -2^7 and 2^7-1 inclusive.
getInt8 :: Parser Int8
getInt8 = fromIntegral <$!> anyWord8

-- | Represents an integer between -2^15 and 2^15-1 inclusive.
--
-- The values are encoded using two bytes in network byte order (big-endian).
getInt16 :: Parser Int16
getInt16 = do
  BSI.BS fp _ <- takeBytes 2
  fromIO $! fromIntegral <$!> BSI.unsafeWithForeignPtr fp peek16BE

-- | Represents an integer between -2^31 and 2^31-1 inclusive.
--
-- The values are encoded using four bytes in network byte order (big-endian).
--
-- > runParser getInt32 "\NUL\NUL\NUL "
-- Done 32
getInt32 :: Parser Int32
getInt32 = do
  BSI.BS fp _ <- takeBytes 4
  fromIO $! fromIntegral <$!> BSI.unsafeWithForeignPtr fp peek32BE

-- | Represents an integer between -2^63 and 2^63-1 inclusive.
--
-- The values are encoded using eight bytes in network byte order (big-endian).
getInt64 :: Parser Int64
getInt64 = do
  BSI.BS fp _ <- takeBytes 8
  fromIO $! fromIntegral <$!> BSI.unsafeWithForeignPtr fp peek64BE

-- | Represents an integer between 0 and 2^32-1 inclusive.
--
-- The values are encoded using four bytes in network byte order (big-endian).
getWord32 :: Parser Word32
getWord32 = do
  BSI.BS fp _ <- takeBytes 4
  fromIO $! BSI.unsafeWithForeignPtr fp peek32BE

-- | Represents an integer between -2^31 and 2^31-1 inclusive.
--
-- Encoding follows the variable-length zig-zag encoding from Google Protocol
-- Buffers.
getVarInt32 :: Parser Int32
getVarInt32 = do
  v <- getVarWord32
  -- decode zigzag
  pure $! fromIntegral $ (v `shiftR` 1) `xor` negate (v .&. 1)

-- | Represents an integer between -2^63 and 2^63-1 inclusive.
--
-- Encoding follows the variable-length zig-zag encoding from Google Protocol
-- Buffers.
getVarInt64 :: Parser Int64
getVarInt64 = do
  v <- getVarWord64
  -- decode zigzag
  pure $! fromIntegral $ (v `shiftR` 1) `xor` negate (v .&. 1)

-- | Represents an integer between 0 and 2^32-1 inclusive.
--
-- Encoding follows the variable-length zig-zag encoding from Google Protocol
-- Buffers.
getVarWord32 :: Parser Word32
getVarWord32 = loop 0 0
  where
    loop :: Word32 -> Int -> Parser Word32
    loop !v !i = do
      b <- anyWord8
      if testMsb b
         then
           let v' = v .|. (fromIntegral (b .&. 0x7f) `shiftL` i)
               i' = i + 7
            in if i' > 28
                  then fail $ "getVarWord32 failed " <> show v'
                  else loop v' i'
         else pure $ v .|. (fromIntegral b `shiftL` i)
{-# INLINE getVarWord32 #-}

getVarWord64 :: Parser Word64
getVarWord64 = loop 0 0
  where
    loop !v !i = do
      b <- anyWord8
      if testMsb b
         then
           let v' = v .|. ((fromIntegral b .&. 0x7f) `shiftL` i)
               i' = i + 7
            in if i' > 63
                  then fail $ "getVarWord64 failed " <> show v'
                  else loop v' i'
         else pure $ v .|. (fromIntegral b `shiftL` i)
{-# INLINE getVarWord64 #-}

-- | Represents a type 4 immutable universally unique identifier (UUID).
--
-- The values are encoded using sixteen bytes in network byte order (big-endian).
--getUUID = undefined

-- | Represents a double-precision 64-bit format IEEE 754 value.
--
-- The values are encoded using eight bytes in network byte order (big-endian).
getDouble :: Parser Double
getDouble = do
  BSI.BS fp _ <- takeBytes 8
  fromIO $! castWord64ToDouble <$!> BSI.unsafeWithForeignPtr fp peek64BE

-- | Represents a sequence of characters.
--
-- First the length N is given as an INT16. Then N bytes follow which are the
-- UTF-8 encoding of the character sequence. Length must not be negative.
getString :: Parser Text
getString = do
  !n <- getInt16
  if n >= 0
     then decodeUtf8 $! takeBytes (fromIntegral n)
     else fail $! "Length of string must not be negative " <> show n

-- | Represents a sequence of characters.
--
-- First the length N + 1 is given as an UNSIGNED_VARINT. Then N bytes follow
-- which are the UTF-8 encoding of the character sequence.
getCompactString :: Parser Text
getCompactString = do
  n_1 <- fromIntegral <$> getVarWord32
  let n = n_1 - 1
  if n >= 0
     then decodeUtf8 $! takeBytes n
     else fail $! "Length of compact string must not be negative " <> show n

-- | Represents a sequence of characters or null.
--
-- For non-null strings, first the length N is given as an INT16. Then N bytes
-- follow which are the UTF-8 encoding of the character sequence. A null value
-- is encoded with length of -1 and there are no following bytes.
getNullableString :: Parser (Maybe Text)
getNullableString = do
  !n <- getInt16
  if n >= 0
     then decodeUtf8Just $! takeBytes (fromIntegral n)
     else do
       if n == (-1)
          then pure Nothing
          else fail $! "Length of empty nullable string must be -1 " <> show n

-- | Represents a sequence of characters.
--
-- First the length N + 1 is given as an UNSIGNED_VARINT. Then N bytes follow
-- which are the UTF-8 encoding of the character sequence. A null string is
-- represented with a length of 0.
getCompactNullableString :: Parser (Maybe Text)
getCompactNullableString = do
  n_1 <- fromIntegral <$> getVarWord32
  let n = n_1 - 1
  if n >= 0
     then decodeUtf8Just $! takeBytes n
     else do
       if n == (-1)
          then pure Nothing
          else fail $! "Length of empty nullable compact string must be -1 " <> show n

-- | Represents a raw sequence of bytes.
--
-- First the length N is given as an INT32. Then N bytes follow.
getBytes :: Parser ByteString
getBytes = do
  !n <- getInt32
  if n >= 0
     then takeBytes (fromIntegral n)
     else fail $! "Length of bytes must not be negative " <> show n

-- | Represents a raw sequence of bytes.
--
-- First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow.
getCompactBytes :: Parser ByteString
getCompactBytes = do
  n <- fromIntegral <$> getVarWord32
  let n' = n - 1
  if n' >= 0
     then takeBytes n'
     else fail $! "Length of compact bytes must not be negative " <> show n'

-- | Represents a raw sequence of bytes or null.
--
-- For non-null values, first the length N is given as an INT32. Then N bytes
-- follow. A null value is encoded with length of -1 and there are no following
-- bytes.
getNullableBytes :: Parser (Maybe ByteString)
getNullableBytes = do
  !n <- getInt32
  if n >= 0
     then Just <$> takeBytes (fromIntegral n)
     else do
       if n == (-1)
          then pure Nothing
          else fail $! "Length of empty nullable bytes must be -1 " <> show n

-- | Represents a raw sequence of bytes.
--
-- First the length N+1 is given as an UNSIGNED_VARINT.Then N bytes follow.
-- A null object is represented with a length of 0.
getCompactNullableBytes :: Parser (Maybe ByteString)
getCompactNullableBytes = do
  n_1 <- fromIntegral <$> getVarWord32
  let n = n_1 - 1
  if n >= 0
     then Just <$!> takeBytes n
     else do
       if n == (-1)
          then pure Nothing
          else fail $! "Length of empty nullable compact bytes must be -1 " <> show n

-- | Record key or value
--
-- ref: https://kafka.apache.org/documentation/#record
getVarBytesInRecord :: Parser (Maybe ByteString)
getVarBytesInRecord = do
  !n <- fromIntegral <$!> getVarInt32
  if n >= 0
     then Just <$> takeBytes n
     else pure Nothing
{-# INLINE getVarBytesInRecord #-}

-- | Record header key
--
-- ref: https://kafka.apache.org/documentation/#record
getVarStringInRecord :: Parser Text
getVarStringInRecord = do
  n <- fromIntegral <$> getVarInt32
  if n >= 0
     then decodeUtf8 $! takeBytes n
     else fail $! "Length of RecordString must be -1 " <> show n

-- | Peek a 32-bit integer at the specified offset without consuming the input.
--
-- Warning: this does not check the length of input.
unsafePeekInt32At :: Int -> Parser Int32
unsafePeekInt32At offset = Parser $ \bs next -> do
  let BSI.BS fp _ = bs
  r <- fromIntegral <$!> BSI.unsafeWithForeignPtr fp (peek32BE . (`plusPtr` offset))
  next bs r
{-# INLINE unsafePeekInt32At #-}

-------------------------------------------------------------------------------

-- Fork from: https://github.com/Yuras/scanner

-- TODO Bench: Use State# instead
newtype Parser a = Parser
  { run :: forall r. ByteString -> Next a r -> IO (Result r) }

instance Functor Parser where
  {-# INLINE fmap #-}
  fmap f (Parser s) = Parser $ \bs next ->
    s bs $ \bs' a -> next bs' (f a)

instance Applicative Parser where
  {-# INLINE pure #-}
  pure a = Parser $ \bs next -> next bs a
  {-# INLINE (<*>) #-}
  (<*>) = ap

  {-# INLINE (<*) #-}
  s1 <* s2 = s1 >>= \a -> s2 >> return a

instance Monad Parser where
  {-# INLINE (>>=) #-}
  s1 >>= s2 = Parser $ \bs next ->
    run s1 bs $ \bs' a -> run (s2 a) bs' next

instance MonadFail Parser where
  {-# INLINE fail #-}
  fail err = Parser $ \bs _ -> pure $! Fail bs err

-- | Run parser with the input
runParser :: Parser r -> ByteString -> IO (Result r)
runParser s bs = run s bs (\b r -> pure $! Done b r)
{-# INLINE runParser #-}

-- | Parser continuation
type Next a r = ByteString -> a -> IO (Result r)

-- | Parser result
data Result r
  = Done ByteString r
  -- ^ Successful result with the rest of input
  | Fail ByteString String
  -- ^ Parser failed with rest of input and error message
  | More (ByteString -> IO (Result r))
  -- ^ Need more input

instance (Show r) => Show (Result r) where
  show (Done bs r)  = "Done " <> show r <> if BS.null bs then "" else "," <> show bs
  show (Fail _ err) = "Fail " <> err
  show (More _)     = "More"

instance Functor Result where
  fmap f (Done bs r) = Done bs (f r)
  fmap f (More k)    = More (fmap (fmap f) . k)
  fmap _ (Fail e v)  = Fail e v
  {-# INLINE fmap #-}

-- | Consume the next word
--
-- It fails if end of input
anyWord8 :: Parser Word8
anyWord8 = Parser $ \bs next ->
  case BS.uncons bs of
    Just (c, bs') -> next bs' c
    _             -> pure $! More $ \bs' -> slowPath bs' next
  where
    slowPath bs next =
      case BS.uncons bs of
        Just (c, bs') -> next bs' c
        _             -> pure $! Fail BS.empty "No more input"
{-# INLINE anyWord8 #-}

-- | Take the specified number of bytes
takeBytes :: Int -> Parser ByteString
takeBytes 0 = pure ""
takeBytes n = Parser $ \bs next ->
  let len = BS.length bs
   in if len >= n
         then let (l, r) = BS.splitAt n bs  -- O(1), note that n can be 0,
                                            -- BS.splitAt 0 "xx" will get ("","xx")
               in next r l
         else pure $ More $ \bs' ->
           if BS.null bs'
              then pure $! Fail BS.empty "No more input"
              else run (slowPath bs len) bs' next
  where
    slowPath bs len = go [bs] (n - len)
    go res 0 = return . BS.concat . reverse $ res
    go res i = Parser $ \bs next ->
      let len = BS.length bs
       in if len >= i
             then let (l, r) = BS.splitAt i bs
                   in next r (BS.concat . reverse $ (l : res))
             else pure $ More $ \bs' ->
               if BS.null bs'
                  then pure $ Fail BS.empty "No more input"
                  else run (go (bs : res) (i - len)) bs' next
{-# INLINE takeBytes #-}

dropBytes :: Int -> Parser ()
dropBytes = void . takeBytes
{-# INLINE dropBytes #-}

-- | Drop the specified number of bytes without checking the length.
--
-- The number must <= the length of input.
directDropBytes :: Int -> Parser ()
directDropBytes n = Parser $ \bs next -> next (BS.drop n bs) ()
{-# INLINE directDropBytes #-}

fromIO :: IO a -> Parser a
fromIO f = Parser $ \bs next -> do
  !r <- f
  next bs r
{-# INLINE fromIO #-}

decodeUtf8 :: Parser ByteString -> Parser Text
decodeUtf8 p = do
  !bs <- p
  case Text.decodeUtf8' bs of
    Left e  -> fail $ "Decode utf8 error: " <> show e
    Right r -> pure r
{-# INLINE decodeUtf8 #-}

decodeUtf8Just :: Parser ByteString -> Parser (Maybe Text)
decodeUtf8Just p = do
  !bs <- p
  case Text.decodeUtf8' bs of
    Left e  -> fail $ "Decode utf8 error: " <> show e
    Right r -> pure $ Just r
{-# INLINE decodeUtf8Just #-}

testMsb :: Word8 -> Bool
testMsb b = (b .&. 0x80) /= 0
{-# INLINE testMsb #-}
