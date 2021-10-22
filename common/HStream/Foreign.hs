{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Foreign
  ( PeekNFun
  , peekN
  , BA# (..)
  , MBA# (..)

  -- * StdString
  , Z.StdString
  , peekStdStringToCBytesN
  , peekStdStringToCBytesIdx

  -- * Vector
  , StdVector
  , FollySmallVector
  , peekFollySmallVectorDoubleN
  , peekFollySmallVectorDouble

  -- * Map
  , PeekMapFun
  , peekCppMap

  -- * Misc
  , c_delete_string
  , c_delete_vector_of_string
  , c_delete_vector_of_int
  , c_delete_vector_of_int64
  , c_delete_std_vec_of_folly_small_vec_of_double
  , cal_offset_std_string
  ) where

import           Control.Exception        (finally)
import           Control.Monad            (forM)
import           Data.Int                 (Int64)
import qualified Data.Map.Strict          as Map
import           Data.Primitive.ByteArray
import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.Storable
import           GHC.Prim
import           Z.Data.CBytes            (CBytes)
import qualified Z.Data.CBytes            as CBytes
import qualified Z.Foreign                as Z

-------------------------------------------------------------------------------

type PeekNFun a b = Int -> Ptr a -> IO [b]
type DeleteFun a = Ptr a -> IO ()

peekN :: Storable a => Int -> Ptr a -> IO [a]
peekN len ptr
  | len <= 0 || ptr == nullPtr = return []
  | otherwise = forM [0..len-1] (peekElemOff ptr)

newtype BA# a = BA# ByteArray#
newtype MBA# a = MBA# (MutableByteArray# RealWorld)

-------------------------------------------------------------------------------

peekStdStringToCBytesN :: Int -> Ptr Z.StdString -> IO [CBytes]
peekStdStringToCBytesN len ptr
  | len <= 0 || ptr == nullPtr = return []
  | otherwise = forM [0..len-1] (peekStdStringToCBytesIdx ptr)

peekStdStringToCBytesIdx :: Ptr Z.StdString -> Int -> IO CBytes
peekStdStringToCBytesIdx p offset = do
  ptr <- cal_offset_std_string p offset
  siz :: Int <- Z.hs_std_string_size ptr
  let !siz' = siz + 1
  (mpa@(Z.MutablePrimArray mba#) :: Z.MutablePrimArray Z.RealWorld a) <- Z.newPrimArray siz'
  !_ <- Z.hs_copy_std_string ptr siz mba#
  Z.writePrimArray mpa siz 0
  CBytes.fromMutablePrimArray mpa
{-# INLINE peekStdStringToCBytesIdx #-}

-------------------------------------------------------------------------------

data StdVector a

data FollySmallVector a

peekFollySmallVectorDoubleN :: Int -> Ptr (FollySmallVector Double) -> IO [[Double]]
peekFollySmallVectorDoubleN len ptr
  | len <= 0 || ptr == nullPtr = return []
  | otherwise = forM [0..len-1] (peekFollySmallVectorDouble ptr)

-- TODO: use Vector or Array as returned value, so that we optimise(remove) the
-- "peekN" function to copy the memory twice.
peekFollySmallVectorDouble :: Ptr (FollySmallVector Double) -> Int -> IO [Double]
peekFollySmallVectorDouble ptr offset = do
  ptr' <- c_cal_offset_vec_of_folly_small_vec_of_double ptr offset
  size <- c_get_size_folly_small_vec_of_double ptr'
  fp <- mallocForeignPtrBytes size
  withForeignPtr fp $ \data' -> do
    c_peek_folly_small_vec_of_double ptr' size data'
    peekN size data'

#define HS_CPP_VEC_SIZE(CFUN, HSOBJ) \
  foreign import ccall unsafe "hs_cpp_lib.h CFUN" \
    c_##CFUN :: Ptr HSOBJ -> IO Int

#define HS_CPP_VEC_PEEK(CFUN, HSOBJ, VAL_TYPE) \
  foreign import ccall unsafe "hs_cpp_lib.h CFUN" \
    c_##CFUN :: Ptr HSOBJ -> Int -> Ptr VAL_TYPE -> IO ()

#define HS_CPP_VEC_OFFSET(CFUN, HSOBJ) \
  foreign import ccall unsafe "hs_cpp_lib.h CFUN" \
    c_##CFUN :: Ptr HSOBJ -> Int -> IO (Ptr HSOBJ)

HS_CPP_VEC_SIZE(get_size_folly_small_vec_of_double, (FollySmallVector Double))
HS_CPP_VEC_PEEK(peek_folly_small_vec_of_double, (FollySmallVector Double), Double)
HS_CPP_VEC_OFFSET(cal_offset_vec_of_folly_small_vec_of_double, (FollySmallVector Double))

-------------------------------------------------------------------------------

type PeekMapFun dk dv ck cv
  = MBA# Int
 -- ^ returned map size
 -> MBA# (Ptr dk) -> MBA# (Ptr dv)
 -> MBA# (Ptr ck) -> MBA# (Ptr cv)
 -> IO ()

peekCppMap
  :: forall dk dv ck cv k v. Ord k
  => PeekMapFun dk dv ck cv
  -> PeekNFun dk k -> DeleteFun ck
  -> PeekNFun dv v -> DeleteFun cv
  -> IO (Map.Map k v)
peekCppMap f peekKey delKey peekVal delVal = do
  (len, (keys_ptr, (values_ptr, (keys_vec, (values_vec, _))))) <-
    Z.withPrimUnsafe (0 :: Int) $ \len ->
    Z.withPrimUnsafe nullPtr $ \keys ->
    Z.withPrimUnsafe nullPtr $ \values ->
    Z.withPrimUnsafe nullPtr $ \keys_vec ->
    Z.withPrimUnsafe nullPtr $ \values_vec ->
      f (MBA# len) (MBA# keys) (MBA# values) (MBA# keys_vec) (MBA# values_vec)
  finally
    (buildExtras len keys_ptr values_ptr)
    (delKey keys_vec <> delVal values_vec)
  where
    buildExtras len keys_ptr values_ptr = do
      keys <- peekKey len keys_ptr
      values <- peekVal len values_ptr
      return . Map.fromList $ zip keys values

-------------------------------------------------------------------------------

#define HS_CPP_DELETE(CFUN, HSOBJ) \
  foreign import ccall unsafe "hs_cpp_lib.h CFUN" \
    c_##CFUN :: Ptr HSOBJ -> IO ()

HS_CPP_DELETE(delete_string, Z.StdString)
HS_CPP_DELETE(delete_vector_of_string, (StdVector Z.StdString))
HS_CPP_DELETE(delete_vector_of_int, (StdVector CInt))
HS_CPP_DELETE(delete_vector_of_int64, (StdVector Int64))
HS_CPP_DELETE(delete_std_vec_of_folly_small_vec_of_double, (StdVector (FollySmallVector Double)))

#define HS_CPP_CAL_OFFSET(CFUN, HSOBJ) \
  foreign import ccall unsafe "hs_cpp_lib.h CFUN" \
    CFUN :: Ptr HSOBJ -> Int -> IO (Ptr HSOBJ)

HS_CPP_CAL_OFFSET(cal_offset_std_string, Z.StdString)
