{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE RankNTypes    #-}
{-# LANGUAGE UnboxedTuples #-}

module HStream.Store.Internal.Foreign where

import           Control.Concurrent           (newEmptyMVar, takeMVar)
import           Control.Exception            (mask_, onException)
import           Control.Monad
import           Control.Monad.Primitive
import           Data.Primitive
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Exts
import           GHC.Stack
import           Z.Data.CBytes                (CBytes)
import qualified Z.Data.CBytes                as CBytes
import           Z.Foreign                    (BA#, MBA#)
import qualified Z.Foreign                    as Z

import qualified HStream.Store.Exception      as E
import           HStream.Store.Internal.Types

cbool2bool :: CBool -> Bool
cbool2bool = (/= 0)
{-# INLINE cbool2bool #-}

unsafeFreezeBA# :: MBA# a -> BA# a
unsafeFreezeBA# mba# =
  case unsafeFreezeByteArray# mba# realWorld# of
    (# _, ba# #) -> ba#

withAsyncPrimUnsafe
  :: (Prim a)
  => a -> (StablePtr PrimMVar -> Int -> MBA# a -> IO b)
  -> IO (a, b)
withAsyncPrimUnsafe a f = withAsyncPrimUnsafe' a f pure
{-# INLINE withAsyncPrimUnsafe #-}

withAsyncPrimUnsafe'
  :: (Prim a)
  => a -> (StablePtr PrimMVar -> Int -> MBA# a -> IO b)
  -> (b -> IO c)
  -> IO (a, c)
withAsyncPrimUnsafe' a f g = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  withPrimSafe' a $ \a' -> do
    (cap, _) <- threadCapability =<< myThreadId
    c <- g =<< f sp cap a'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch# a'))
    return c
{-# INLINE withAsyncPrimUnsafe' #-}

withAsyncPrimUnsafe2
  :: (Prim a, Prim b)
  => a -> b -> (StablePtr PrimMVar -> Int -> MBA# Word8 -> MBA# Word8 -> IO c)
  -> IO (a, b, c)
withAsyncPrimUnsafe2 a b f = withAsyncPrimUnsafe2' a b f pure
{-# INLINE withAsyncPrimUnsafe2 #-}

withAsyncPrimUnsafe2'
  :: (Prim a, Prim b)
  => a -> b -> (StablePtr PrimMVar -> Int -> MBA# Word8 -> MBA# Word8 -> IO c)
  -> (c -> IO d)
  -> IO (a, b, d)
withAsyncPrimUnsafe2' a b f g = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  (a_, (b_, d_)) <- withPrimSafe' a $ \a' -> do
    withPrimSafe' b $ \b' -> do
      (cap, _) <- threadCapability =<< myThreadId
      d <- g =<< f sp cap a' b'
      takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch# a'); primitive_ (touch# b'))
      return d
  return (a_, b_, d_)
{-# INLINE withAsyncPrimUnsafe2' #-}

withAsync :: HasCallStack
          => Int -> (Ptr a -> IO a)
          -> (StablePtr PrimMVar -> Int -> Ptr a -> IO ErrorCode)
          -> IO a
withAsync size peek_data f = fst <$> withAsync' size peek_data E.throwStreamErrorIfNotOK' f
{-# INLINE withAsync #-}

withAsync'
  :: HasCallStack
  => Int -> (Ptr a -> IO a)
  -> (HasCallStack => b -> IO b)
  -> (StablePtr PrimMVar -> Int -> Ptr a -> IO b)
  -> IO (a, b)
withAsync' size peek_data g f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  fp <- mallocForeignPtrBytes size
  withForeignPtr fp $ \data' -> do
    (cap, _) <- threadCapability =<< myThreadId
    b <- g =<< f sp cap data'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
    a <- peek_data data'
    return (a, b)
{-# INLINE withAsync' #-}

retryWhileAgain :: HasCallStack => IO (ErrorCode, a) -> Int -> IO a
retryWhileAgain f retries = do
  (errno, r) <- f
  case errno of
    C_OK -> return r
    C_AGAIN
      | retries == 0 -> E.throwStreamError errno callStack
      | retries < 0 -> threadDelay 5000 >> (retryWhileAgain f $! (-1))
      | retries > 0 -> threadDelay 5000 >> (retryWhileAgain f $! retries - 1)
    _ -> E.throwStreamError errno callStack
{-# INLINE retryWhileAgain #-}

-------------------------------------------------------------------------------

withPrimSafe' :: forall a b. Prim a => a -> (MBA# a -> IO b) -> IO (a, b)
withPrimSafe' v f = do
    mpa@(MutablePrimArray mba#) <- newAlignedPinnedPrimArray 1
    writePrimArray mpa 0 v
    !b <- f mba#
    !a <- readPrimArray mpa 0
    return (a, b)
{-# INLINE withPrimSafe' #-}

peekStdStringToCBytesN :: Int -> Ptr Z.StdString -> IO [CBytes]
peekStdStringToCBytesN len ptr = forM [0..len-1] (peekStdStringToCBytesIdx ptr)

peekStdStringToCBytesIdx :: Ptr Z.StdString -> Int -> IO CBytes
peekStdStringToCBytesIdx p offset = do
  ptr <- hs_cal_std_string_off p offset
  siz :: Int <- Z.hs_std_string_size ptr
  let !siz' = siz + 1
  (mpa@(MutablePrimArray mba#) :: MutablePrimArray RealWorld a) <- newPrimArray siz'
  !_ <- Z.hs_copy_std_string ptr siz mba#
  Z.writePrimArray mpa siz 0
  CBytes.fromMutablePrimArray mpa
{-# INLINE peekStdStringToCBytesIdx #-}

foreign import ccall unsafe "hs_logdevice.h hs_cal_std_string_off"
  hs_cal_std_string_off :: Ptr Z.StdString -> Int -> IO (Ptr Z.StdString)
