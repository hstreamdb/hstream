{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE RankNTypes    #-}
{-# LANGUAGE UnboxedTuples #-}

module HStream.Store.Internal.Foreign where

import           Control.Concurrent           (newEmptyMVar, takeMVar)
import           Control.Exception            (mask_, onException)
import           Control.Monad.Primitive
import           Data.Primitive
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Exts
import           GHC.Stack                    (HasCallStack)
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
withAsyncPrimUnsafe a f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  Z.withPrimUnsafe a $ \a' -> do
    (cap, _) <- threadCapability =<< myThreadId
    b <- f sp cap a'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch# a'))
    return b
{-# INLINE withAsyncPrimUnsafe #-}

withAsyncPrimUnsafe2
  :: (Prim a, Prim b)
  => a -> b -> (StablePtr PrimMVar -> Int -> MBA# Word8 -> MBA# Word8 -> IO c)
  -> IO (a, b, c)
withAsyncPrimUnsafe2 a b f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  (a_, (b_, c_)) <- Z.withPrimUnsafe a $ \a' -> do
    Z.withPrimUnsafe b $ \b' -> do
      (cap, _) <- threadCapability =<< myThreadId
      c <- f sp cap a' b'
      takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch# a'); primitive_ (touch# b'))
      return c
  return (a_, b_, c_)
{-# INLINE withAsyncPrimUnsafe2 #-}

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
