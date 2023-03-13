{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP          #-}
{-# LANGUAGE MagicHash    #-}

module HStream.Base.Bytes
  ( cbytes2sbs
  , cbytes2sbsUnsafe
  , sbs2cbytes
  , sbs2cbytesUnsafe
  ) where

import           Control.Monad.ST               (runST)
import           Data.ByteString.Short          (ShortByteString)
import qualified Data.ByteString.Short          as BShort
import           Data.Primitive.PrimArray
import qualified Z.Data.CBytes                  as CBytes
import           Z.Data.CBytes                  (CBytes)

#if !MIN_VERSION_bytestring(0, 11, 0)
import qualified Data.ByteString.Short.Internal as BShort
#endif

cbytes2sbs :: CBytes -> ShortByteString
cbytes2sbs cbytes =
  -- CBytes is null-terminated, so trim it
  let pa = CBytes.rawPrimArray cbytes
      size = sizeofPrimArray pa
   in runST $ do mpa <- thawPrimArray pa 0 (size - 1)
                 !(PrimArray ba#) <- unsafeFreezePrimArray mpa
                 return $ BShort.SBS ba#
{-# INLINABLE cbytes2sbs #-}

-- The original cbytes should NOT be used after the conversion.
cbytes2sbsUnsafe :: CBytes -> ShortByteString
cbytes2sbsUnsafe cbytes =
  -- CBytes is null-terminated, so trim it
  let pa = CBytes.rawPrimArray cbytes
      size = sizeofPrimArray pa
   in runST $ do mpa <- unsafeThawPrimArray pa
                 shrinkMutablePrimArray mpa (size - 1)
                 !(PrimArray ba#) <- unsafeFreezePrimArray mpa
                 return $ BShort.SBS ba#
{-# INLINABLE cbytes2sbsUnsafe #-}

-- Be care to deal with SBS contains "\\NUL".
-- Result will be shrinked to first @\\NUL@ byte.
sbs2cbytes :: ShortByteString -> CBytes
sbs2cbytes (BShort.SBS ba#) = CBytes.fromPrimArray (PrimArray ba#)
{-# INLINABLE sbs2cbytes #-}

-- The original ShortByteString should NOT be used after the conversion.
sbs2cbytesUnsafe :: ShortByteString -> CBytes
sbs2cbytesUnsafe (BShort.SBS ba#) = runST $ do
  mpa <- unsafeThawPrimArray (PrimArray ba#)
  CBytes.fromMutablePrimArray mpa
{-# INLINABLE sbs2cbytesUnsafe #-}
