{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -pgmPcpphs -optP--cpp #-}

module HStream.Foreign
  ( PeekNFun
  , DeleteFun
  , peekN
  , BA# (..)
  , MBA# (..)
  , BAArray# (..)

  -- * StdString
  , Z.StdString
  , peekStdStringToCBytesN
  , peekStdStringToCBytesIdx
  , withStdStringUnsafe

  -- * Optional
  , withAllocMaybePrim
  , withAllocMaybePrim2

  -- * List
  , StdVector
  , FollySmallVector
  , peekStdVectorWord64
  , peekStdVectorWord64Off
  , peekStdVectorWord64N
  , peekFollySmallVectorDouble
  , peekFollySmallVectorDoubleOff
  , peekFollySmallVectorDoubleN

  -- * Map
  , PeekMapFun
  , peekCppMap
  , withHsCBytesMapUnsafe
  , withPrimListPairUnsafe

  -- * Misc
  , c_delete_string
  , c_delete_vector_of_string
  , c_delete_vector_of_int
  , c_delete_vector_of_int64
  , c_delete_vector_of_uint64
  , c_delete_std_vec_of_folly_small_vec_of_double
  , c_cal_offset_std_string
  , bool2cbool
  ) where

import           Control.Exception  (assert, finally)
import           Control.Monad      (forM)
import           Data.Int           (Int64)
import           Data.Map.Strict    (Map)
import qualified Data.Map.Strict    as Map
import           Data.Primitive
import           Data.Word
import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.Storable
import           GHC.Exts
import qualified Z.Data.Array       as Z
import qualified Z.Data.CBytes      as CBytes
import           Z.Data.CBytes      (CBytes)
import qualified Z.Foreign          as Z
import           Z.Foreign          (StdString)


-------------------------------------------------------------------------------

type PeekNFun a b = Int -> Ptr a -> IO [b]
type DeleteFun a = Ptr a -> IO ()

peekN :: Storable a => Int -> Ptr a -> IO [a]
peekN len ptr
  | len <= 0 || ptr == nullPtr = return []
  | otherwise = forM [0..len-1] (peekElemOff ptr)

newtype BA# a = BA# ByteArray#
newtype MBA# a = MBA# (MutableByteArray# RealWorld)
-- TODO: ghc-9.4+ deprecates ArrayArray#, consider using Array# instead
newtype BAArray# a = BAArray# ArrayArray#

-------------------------------------------------------------------------------

#define HS_CPP_GET_SIZE(CFUN, HSOBJ) \
  foreign import ccall unsafe "get_size_##CFUN" \
    c_get_size_##CFUN :: Ptr HSOBJ -> IO Int

#define HS_CPP_PEEK(CFUN, HSOBJ, VAL_TYPE) \
  foreign import ccall unsafe "peek_##CFUN" \
    c_peek_##CFUN :: Ptr HSOBJ -> Int -> Ptr VAL_TYPE -> IO ()

#define HS_CPP_CAL_OFFSET(CFUN, HSOBJ) \
  foreign import ccall unsafe "cal_offset_##CFUN" \
    c_cal_offset_##CFUN :: Ptr HSOBJ -> Int -> IO (Ptr HSOBJ)

-------------------------------------------------------------------------------
-- Optional

withAllocMaybePrim :: forall a b c. Prim a
                    => (c -> a) -> Maybe c -> (Ptr a -> IO b) -> IO b
withAllocMaybePrim t (Just x) f = do
  buf <- newAlignedPinnedPrimArray 1
  writePrimArray buf 0 (t x)
  !b <- Z.withMutablePrimArrayContents buf f
  return b
withAllocMaybePrim _ Nothing f = f nullPtr
{-# INLINABLE withAllocMaybePrim #-}

withAllocMaybePrim2
  :: forall a b c. Prim a
  => (c -> a)
  -> Maybe (Maybe c)
  -> (Bool -> Ptr a -> IO b)
  -> IO b
withAllocMaybePrim2 t (Just (Just x)) f = do
  buf <- newAlignedPinnedPrimArray 1
  writePrimArray buf 0 (t x)
  !b <- Z.withMutablePrimArrayContents buf $ \ptr -> f True ptr
  return b
withAllocMaybePrim2 _ (Just Nothing) f = f True nullPtr
withAllocMaybePrim2 _ Nothing f = f False nullPtr
{-# INLINABLE withAllocMaybePrim2 #-}

-------------------------------------------------------------------------------
-- StdString

peekStdStringToCBytesN :: Int -> Ptr Z.StdString -> IO [CBytes]
peekStdStringToCBytesN len ptr
  | len <= 0 || ptr == nullPtr = return []
  | otherwise = forM [0..len-1] (peekStdStringToCBytesIdx ptr)

peekStdStringToCBytesIdx :: Ptr Z.StdString -> Int -> IO CBytes
peekStdStringToCBytesIdx p offset = do
  ptr <- c_cal_offset_std_string p offset
  siz :: Int <- Z.hs_std_string_size ptr
  let !siz' = siz + 1
  (mpa@(Z.MutablePrimArray mba#) :: Z.MutablePrimArray Z.RealWorld a) <- Z.newPrimArray siz'
  !_ <- Z.hs_copy_std_string ptr siz mba#
  Z.writePrimArray mpa siz 0
  CBytes.fromMutablePrimArray mpa
{-# INLINE peekStdStringToCBytesIdx #-}

withStdStringUnsafe :: (MBA# (Ptr StdString) -> IO a) -> IO (CBytes, a)
withStdStringUnsafe f = do
  (ptr', ret) <- Z.withPrimUnsafe nullPtr $ \ptr -> f (MBA# ptr)
  if ptr' == nullPtr
     then pure ("", ret)
     else do str <- finally (peekStdStringToCBytesIdx ptr' 0) (c_delete_string ptr')
             pure (str, ret)

HS_CPP_CAL_OFFSET(std_string, StdString)

-------------------------------------------------------------------------------

data StdVector a
data FollySmallVector a

-- TODO: use Vector or Array as returned value, so that we optimise(remove) the
-- "peekN" function to copy the memory twice.
#define HS_PEEK(ty, a, cfun) \
  peek##ty##a##Off :: Ptr (ty a) -> Int -> IO [a];                  \
  peek##ty##a##Off ptr offset = (do                                 \
    ptr' <- c_cal_offset_##cfun ptr offset;                         \
    size <- c_get_size_##cfun ptr';                                 \
    fp <- mallocForeignPtrBytes size;                               \
    withForeignPtr fp (\data' -> (do c_peek_##cfun ptr' size data'; \
                                     peekN size data')) );          \
                                                                    \
  peek##ty##a :: Ptr (ty a) -> IO [a];                              \
  peek##ty##a ptr = peek##ty##a##Off ptr 0;                         \
                                                                    \
  peek##ty##a##N :: Int -> Ptr (ty a) -> IO [[a]];                  \
  peek##ty##a##N len ptr                                            \
    | len <= 0 || ptr == nullPtr = return []                        \
    | otherwise = forM [0..len-1] (peek##ty##a##Off ptr);

HS_PEEK(StdVector, Word64, vec_of_uint64)
HS_PEEK(FollySmallVector, Double, folly_small_vec_of_double)

HS_CPP_GET_SIZE(vec_of_uint64, (StdVector Word64))
HS_CPP_GET_SIZE(folly_small_vec_of_double, (FollySmallVector Double))

HS_CPP_CAL_OFFSET(vec_of_uint64, (StdVector Word64))
HS_CPP_CAL_OFFSET(folly_small_vec_of_double, (FollySmallVector Double))

HS_CPP_PEEK(vec_of_uint64, (StdVector Word64), Word64)
HS_CPP_PEEK(folly_small_vec_of_double, (FollySmallVector Double), Double)

-------------------------------------------------------------------------------

type PeekMapFun a pk pv dk dv
  = MBA# Int
 -- ^ returned map size
 -> MBA# (Ptr pk) -> MBA# (Ptr pv)
 -> MBA# (Ptr dk) -> MBA# (Ptr dv)
 -> IO a

peekCppMap
  :: forall a pk pv dk dv k v. Ord k
  => PeekMapFun a pk pv dk dv
  -> PeekNFun pk k -> DeleteFun dk
  -> PeekNFun pv v -> DeleteFun dv
  -> IO (a, Map.Map k v)
peekCppMap f peekKey delKey peekVal delVal = do
  (len, (keys_ptr, (values_ptr, (keys_vec, (values_vec, ret))))) <-
    Z.withPrimUnsafe (0 :: Int) $ \len ->
    Z.withPrimUnsafe nullPtr $ \keys ->
    Z.withPrimUnsafe nullPtr $ \values ->
    Z.withPrimUnsafe nullPtr $ \keys_vec ->
    Z.withPrimUnsafe nullPtr $ \values_vec ->
      f (MBA# len) (MBA# keys) (MBA# values) (MBA# keys_vec) (MBA# values_vec)
  finally
    (buildMap ret len keys_ptr values_ptr)
    (delKey keys_vec <> delVal values_vec)
  where
    buildMap ret len keys_ptr values_ptr = do
      keys <- peekKey len keys_ptr
      values <- peekVal len values_ptr
      return (ret, Map.fromList $ zip keys values)

withHsCBytesMapUnsafe
  :: Map CBytes CBytes
  -> (Int -> BAArray# a -> BAArray# a -> IO b)
  -> IO b
withHsCBytesMapUnsafe hsmap f = do
  let hsmap' = Map.toList hsmap
      ks = map (CBytes.rawPrimArray . fst) hsmap'
      vs = map (CBytes.rawPrimArray . snd) hsmap'
  Z.withPrimArrayListUnsafe ks $ \ks' l ->
    Z.withPrimArrayListUnsafe vs $ \vs' _ -> f l (BAArray# ks') (BAArray# vs')

withPrimListPairUnsafe :: (Prim a, Prim b)
                       => [(a, b)] -> (Int -> BA# a -> BA# b -> IO c) -> IO c
withPrimListPairUnsafe pairs f = do
  let keys = primArrayFromList $ map fst pairs
      vals = primArrayFromList $ map snd pairs
  Z.withPrimArrayUnsafe keys $ \ba la -> do
    Z.withPrimArrayUnsafe vals $ \bb lb -> assert (la == lb) $ f la (BA# ba) (BA# bb)

-------------------------------------------------------------------------------

#define HS_CPP_DELETE(CFUN, HSOBJ) \
  foreign import ccall unsafe "hs_cpp_lib.h CFUN" \
    c_##CFUN :: Ptr HSOBJ -> IO ()

HS_CPP_DELETE(delete_string, StdString)
HS_CPP_DELETE(delete_vector_of_string, (StdVector StdString))
HS_CPP_DELETE(delete_vector_of_int, (StdVector CInt))
HS_CPP_DELETE(delete_vector_of_int64, (StdVector Int64))
HS_CPP_DELETE(delete_vector_of_uint64, (StdVector Int64))
HS_CPP_DELETE(delete_std_vec_of_folly_small_vec_of_double, (StdVector (FollySmallVector Double)))

bool2cbool :: Bool -> CBool
bool2cbool True  = 1
bool2cbool False = 0
