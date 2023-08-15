{-# LANGUAGE CPP #-}

module Kafka.Protocol.Encoding.Internal
  ( peek16BE, poke16BE
  , peek32BE, poke32BE
  , peek64BE, poke64BE
  , peek16LE, poke16LE
  , peek32LE, poke32LE
  , peek64LE, poke64LE
  ) where

import           Data.Bits
import           Data.Word
import           Foreign.Ptr
import           Foreign.Storable

-------------------------------------------------------------------------------
-- Basic number peek & poke
--
-- Stolen from: https://github.com/minad/persist

-- From GHC
#include <MachDeps.h>

poke16LE :: Ptr Word8 -> Word16 -> IO ()
poke32LE :: Ptr Word8 -> Word32 -> IO ()
poke64LE :: Ptr Word8 -> Word64 -> IO ()
{-# INLINE poke16LE #-}
{-# INLINE poke32LE #-}
{-# INLINE poke64LE #-}

poke16BE :: Ptr Word8 -> Word16 -> IO ()
poke32BE :: Ptr Word8 -> Word32 -> IO ()
poke64BE :: Ptr Word8 -> Word64 -> IO ()
{-# INLINE poke16BE #-}
{-# INLINE poke32BE #-}
{-# INLINE poke64BE #-}

peek16LE :: Ptr Word8 -> IO Word16
peek32LE :: Ptr Word8 -> IO Word32
peek64LE :: Ptr Word8 -> IO Word64
{-# INLINE peek16LE #-}
{-# INLINE peek32LE #-}
{-# INLINE peek64LE #-}

peek16BE :: Ptr Word8 -> IO Word16
peek32BE :: Ptr Word8 -> IO Word32
peek64BE :: Ptr Word8 -> IO Word64
{-# INLINE peek16BE #-}
{-# INLINE peek32BE #-}
{-# INLINE peek64BE #-}

#ifndef UNALIGNED_MEMORY
pokeByte :: (Integral a) => Ptr Word8 -> a -> IO ()
pokeByte p x = poke p (fromIntegral x)
{-# INLINE pokeByte #-}

peekByte :: (Integral a) => Ptr Word8 -> IO a
peekByte p = do
  !b <- peek p
  return $! fromIntegral b
{-# INLINE peekByte #-}

poke16LE p y = do
  pokeByte p $ y
  pokeByte (p `plusPtr` 1) $ y `unsafeShiftR` 8

poke16BE p y = do
  pokeByte p $ y `unsafeShiftR` 8
  pokeByte (p `plusPtr` 1) $ y

poke32LE p y = do
  pokeByte p $ y
  pokeByte (p `plusPtr` 1) $ y `unsafeShiftR` 8
  pokeByte (p `plusPtr` 2) $ y `unsafeShiftR` 16
  pokeByte (p `plusPtr` 3) $ y `unsafeShiftR` 24

poke32BE p y = do
  pokeByte p $ y `unsafeShiftR` 24
  pokeByte (p `plusPtr` 1) $ y `unsafeShiftR` 16
  pokeByte (p `plusPtr` 2) $ y `unsafeShiftR` 8
  pokeByte (p `plusPtr` 3) $ y

poke64LE p y = do
  pokeByte p $ y
  pokeByte (p `plusPtr` 1) $ y `unsafeShiftR` 8
  pokeByte (p `plusPtr` 2) $ y `unsafeShiftR` 16
  pokeByte (p `plusPtr` 3) $ y `unsafeShiftR` 24
  pokeByte (p `plusPtr` 4) $ y `unsafeShiftR` 32
  pokeByte (p `plusPtr` 5) $ y `unsafeShiftR` 40
  pokeByte (p `plusPtr` 6) $ y `unsafeShiftR` 48
  pokeByte (p `plusPtr` 7) $ y `unsafeShiftR` 56

poke64BE p y = do
  pokeByte p $ y `unsafeShiftR` 56
  pokeByte (p `plusPtr` 1) $ y `unsafeShiftR` 48
  pokeByte (p `plusPtr` 2) $ y `unsafeShiftR` 40
  pokeByte (p `plusPtr` 3) $ y `unsafeShiftR` 32
  pokeByte (p `plusPtr` 4) $ y `unsafeShiftR` 24
  pokeByte (p `plusPtr` 5) $ y `unsafeShiftR` 16
  pokeByte (p `plusPtr` 6) $ y `unsafeShiftR` 8
  pokeByte (p `plusPtr` 7) $ y

peek16LE p = do
  !x0 <- peekByte @Word16 p
  !x1 <- peekByte @Word16 (p `plusPtr` 1)
  return $ x1 `unsafeShiftL` 8
    .|. x0

peek16BE p = do
  !x0 <- peekByte @Word16 p
  !x1 <- peekByte @Word16 (p `plusPtr` 1)
  return $ x0 `unsafeShiftL` 8
    .|. x1

peek32LE p = do
  !x0 <- peekByte @Word32 p
  !x1 <- peekByte @Word32 (p `plusPtr` 1)
  !x2 <- peekByte @Word32 (p `plusPtr` 2)
  !x3 <- peekByte @Word32 (p `plusPtr` 3)
  return $ x3 `unsafeShiftL` 24
    .|. x2 `unsafeShiftL` 16
    .|. x1 `unsafeShiftL` 8
    .|. x0

peek32BE p = do
  !x0 <- peekByte @Word32 p
  !x1 <- peekByte @Word32 (p `plusPtr` 1)
  !x2 <- peekByte @Word32 (p `plusPtr` 2)
  !x3 <- peekByte @Word32 (p `plusPtr` 3)
  return $ x0 `unsafeShiftL` 24
    .|. x1 `unsafeShiftL` 16
    .|. x2 `unsafeShiftL` 8
    .|. x3

peek64LE p = do
  !x0 <- peekByte @Word64 p
  !x1 <- peekByte @Word64 (p `plusPtr` 1)
  !x2 <- peekByte @Word64 (p `plusPtr` 2)
  !x3 <- peekByte @Word64 (p `plusPtr` 3)
  !x4 <- peekByte @Word64 (p `plusPtr` 4)
  !x5 <- peekByte @Word64 (p `plusPtr` 5)
  !x6 <- peekByte @Word64 (p `plusPtr` 6)
  !x7 <- peekByte @Word64 (p `plusPtr` 7)
  return $ x7 `unsafeShiftL` 56
    .|. x6 `unsafeShiftL` 48
    .|. x5 `unsafeShiftL` 40
    .|. x4 `unsafeShiftL` 32
    .|. x3 `unsafeShiftL` 24
    .|. x2 `unsafeShiftL` 16
    .|. x1 `unsafeShiftL` 8
    .|. x0

peek64BE p = do
  !x0 <- peekByte @Word64 p
  !x1 <- peekByte @Word64 (p `plusPtr` 1)
  !x2 <- peekByte @Word64 (p `plusPtr` 2)
  !x3 <- peekByte @Word64 (p `plusPtr` 3)
  !x4 <- peekByte @Word64 (p `plusPtr` 4)
  !x5 <- peekByte @Word64 (p `plusPtr` 5)
  !x6 <- peekByte @Word64 (p `plusPtr` 6)
  !x7 <- peekByte @Word64 (p `plusPtr` 7)
  return $ x0 `unsafeShiftL` 56
    .|. x1 `unsafeShiftL` 48
    .|. x2 `unsafeShiftL` 40
    .|. x3 `unsafeShiftL` 32
    .|. x4 `unsafeShiftL` 24
    .|. x5 `unsafeShiftL` 16
    .|. x6 `unsafeShiftL` 8
    .|. x7

-- UNALIGNED_MEMORY
#else
fromLE16 :: Word16 -> Word16
fromLE32 :: Word32 -> Word32
fromLE64 :: Word64 -> Word64
{-# INLINE fromLE16 #-}
{-# INLINE fromLE32 #-}
{-# INLINE fromLE64 #-}

fromBE16 :: Word16 -> Word16
fromBE32 :: Word32 -> Word32
fromBE64 :: Word64 -> Word64
{-# INLINE fromBE16 #-}
{-# INLINE fromBE32 #-}
{-# INLINE fromBE64 #-}

toLE16 :: Word16 -> Word16
toLE32 :: Word32 -> Word32
toLE64 :: Word64 -> Word64
{-# INLINE toLE16 #-}
{-# INLINE toLE32 #-}
{-# INLINE toLE64 #-}

toBE16 :: Word16 -> Word16
toBE32 :: Word32 -> Word32
toBE64 :: Word64 -> Word64
{-# INLINE toBE16 #-}
{-# INLINE toBE32 #-}
{-# INLINE toBE64 #-}

#ifdef WORDS_BIGENDIAN
fromBE16 = id
fromBE32 = id
fromBE64 = id
toBE16 = id
toBE32 = id
toBE64 = id
fromLE16 = byteSwap16
fromLE32 = byteSwap32
fromLE64 = byteSwap64
toLE16 = byteSwap16
toLE32 = byteSwap32
toLE64 = byteSwap64
#else
fromLE16 = id
fromLE32 = id
fromLE64 = id
toLE16 = id
toLE32 = id
toLE64 = id
fromBE16 = byteSwap16
fromBE32 = byteSwap32
fromBE64 = byteSwap64
toBE16 = byteSwap16
toBE32 = byteSwap32
toBE64 = byteSwap64
#endif

poke16LE p = poke (castPtr @_ @Word16 p) . toLE16
poke32LE p = poke (castPtr @_ @Word32 p) . toLE32
poke64LE p = poke (castPtr @_ @Word64 p) . toLE64

poke16BE p = poke (castPtr @_ @Word16 p) . toBE16
poke32BE p = poke (castPtr @_ @Word32 p) . toBE32
poke64BE p = poke (castPtr @_ @Word64 p) . toBE64

peek16LE p = fromLE16 <$!> peek (castPtr @_ @Word16 p)
peek32LE p = fromLE32 <$!> peek (castPtr @_ @Word32 p)
peek64LE p = fromLE64 <$!> peek (castPtr @_ @Word64 p)

peek16BE p = fromBE16 <$!> peek (castPtr @_ @Word16 p)
peek32BE p = fromBE32 <$!> peek (castPtr @_ @Word32 p)
peek64BE p = fromBE64 <$!> peek (castPtr @_ @Word64 p)

-- end UNALIGNED_MEMORY
#endif
