{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE RankNTypes    #-}
{-# LANGUAGE UnboxedTuples #-}

module HStream.Store.Internal.Foreign where

import           Control.Concurrent           (newEmptyMVar, takeMVar)
import           Control.Exception            (finally, mask_, onException)
import           Control.Monad.Primitive
import           Data.Primitive
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Exts
import           GHC.Stack
import           Z.Data.CBytes                (CBytes)
import qualified Z.Foreign                    as Z
import           Z.Foreign                    (BA#, MBA#)

-- TODO: Use HStream.Foreign.BA# instead
import           HStream.Foreign              hiding (BA#, MBA#)
import qualified HStream.Logger               as Log
import qualified HStream.Store.Exception      as E
import           HStream.Store.Internal.Types

cbool2bool :: CBool -> Bool
cbool2bool = (/= 0)
{-# INLINE cbool2bool #-}

unsafeFreezeBA# :: MBA# a -> BA# a
unsafeFreezeBA# mba# =
  case unsafeFreezeByteArray# mba# realWorld# of
    (# _, ba# #) -> ba#

-- Actually, these unsafe functions can be used for both unsafe & safe ffi(?).
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
  -- PinnedPrimArray is required, because even the f is an unsafe ffi, we asume
  -- it'll return immediately. The callback is runned on other cpp thread.
  withPrimSafe' a $ \a' -> do
    (cap, _) <- threadCapability =<< myThreadId
    c <- g =<< f sp cap a'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch# a'))
    return c
{-# INLINE withAsyncPrimUnsafe' #-}

withAsyncPrimUnsafe2
  :: (Prim a, Prim b)
  => a -> b -> (StablePtr PrimMVar -> Int -> MBA# a -> MBA# b -> IO c)
  -> IO (a, b, c)
withAsyncPrimUnsafe2 a b f = withAsyncPrimUnsafe2' a b f pure
{-# INLINE withAsyncPrimUnsafe2 #-}

withAsyncPrimUnsafe2'
  :: (Prim a, Prim b)
  => a -> b -> (StablePtr PrimMVar -> Int -> MBA# a -> MBA# b -> IO c)
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

withAsyncPrimUnsafe3'
  :: (Prim a, Prim b, Prim c)
  => a -> b -> c
  -> (StablePtr PrimMVar -> Int -> MBA# a -> MBA# b -> MBA# c -> IO d)
  -> (d -> IO e)
  -> IO (a, b, c, e)
withAsyncPrimUnsafe3' a b c f g = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  (a_, (b_, (c_ , e_))) <-
    withPrimSafe' a $ \a' ->
    withPrimSafe' b $ \b' ->
    withPrimSafe' c $ \c' -> do
      (cap, _) <- threadCapability =<< myThreadId
      e <- g =<< f sp cap a' b' c'
      takeMVar mvar `onException` forkIO (do takeMVar mvar
                                             primitive_ (touch# a')
                                             primitive_ (touch# b')
                                             primitive_ (touch# c'))
      return e
  return (a_, b_, c_, e_)

-- Similar to HStream.Foreign.PeekMapFun
--
-- TODO: Use HStream.Foreign.PeekMapFun instead
type MapFun a pk pv dk dv
  = MBA# Int
    -- ^ returned map size
 -> MBA# (Ptr pk) -> MBA# (Ptr pv)
    -- ^ pointer to peek
 -> MBA# (Ptr dk) -> MBA# (Ptr dv)
    -- ^ pointer to delete
 -> IO a

withAsyncPrimMapUnsafe
  :: (Prim p)
  => p
  -> PeekNFun pk k -> DeleteFun dk
  -> PeekNFun pv v -> DeleteFun dv
  -> (StablePtr PrimMVar -> Int -> MBA# p -> MapFun a k v vk vv)
  -> IO (a, p, [(k, v)])
withAsyncPrimMapUnsafe p peekk delk peekv delv f =
  withAsyncPrimMapUnsafe' p peekk delk peekv delv f pure

withAsyncPrimMapUnsafe'
  :: (Prim p)
  => p
  -> PeekNFun pk k -> DeleteFun dk
  -> PeekNFun pv v -> DeleteFun dv
  -> (StablePtr PrimMVar -> Int -> MBA# p -> MapFun a pk pv dk dv)
  -> (a -> IO b)
  -> IO (b, p, [(k, v)])
withAsyncPrimMapUnsafe' p peekk delk peekv delv f g = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  (p_, (len_, (keys_, (values_, (keys_vec_, (values_vec_, b_)))))) <-
    withPrimSafe' p $ \p' ->
    withPrimSafe' (0 :: Int) $ \len ->
    withPrimSafe' nullPtr $ \keys ->
    withPrimSafe' nullPtr $ \values ->
    withPrimSafe' nullPtr $ \keys_vec ->
    withPrimSafe' nullPtr $ \values_vec -> do
      (cap, _) <- threadCapability =<< myThreadId
      b <- g =<< f sp cap p' len keys values keys_vec values_vec
      takeMVar mvar `onException` forkIO (do takeMVar mvar
                                             primitive_ (touch# p')
                                             primitive_ (touch# len)
                                             primitive_ (touch# keys)
                                             primitive_ (touch# values)
                                             primitive_ (touch# keys_vec)
                                             primitive_ (touch# values_vec))
      return b
  finally
    (do ret_keys <- peekk len_ keys_
        ret_values <- peekv len_ values_
        return (b_, p_, zip ret_keys ret_values))
    (delk keys_vec_ <> delv values_vec_)  -- delete a nullptr is OK

withAsync :: HasCallStack
          => Int -> (Ptr a -> IO a)
          -> (StablePtr PrimMVar -> Int -> Ptr a -> IO ErrorCode)
          -> IO a
withAsync size peek_data f = fst <$> withAsync' size peek_data E.throwStreamErrorIfNotOK' f
{-# INLINE withAsync #-}

withAsyncVoid
  :: HasCallStack
  => Int -> (Ptr a -> IO a)
  -> (StablePtr PrimMVar -> Int -> Ptr a -> IO b)
  -> IO a
withAsyncVoid size peek_data f = fst <$> withAsync' size peek_data pure f
{-# INLINE withAsyncVoid #-}

retryWhileAgain :: HasCallStack => IO (ErrorCode, a) -> Int -> IO a
retryWhileAgain f retries = do
  (errno, r) <- f
  case errno of
    C_OK -> return r
    C_AGAIN
      | retries == 0 -> Log.warning "Run out of retries!" >> E.throwStreamError errno callStack
      | retries < 0 -> do Log.warning $ "Again: remain retries " <> Log.build retries
                          threadDelay 500000 >> (retryWhileAgain f $! (-1))
      | retries > 0 -> do Log.warning $ "Again: remain retries " <> Log.build retries
                          threadDelay 500000 >> (retryWhileAgain f $! retries - 1)
    _ -> E.throwStreamError errno callStack

withForeignPtrList :: [ForeignPtr a] -> (Ptr (Ptr a) -> Int -> IO b) -> IO b
withForeignPtrList fptrs f = do
  let l = length fptrs
  ptrs <- newPinnedPrimArray l
  go ptrs 0 fptrs
  where
    go ptrs !_ [] = do
      pa <- unsafeFreezePrimArray ptrs
      Z.withPrimArraySafe pa f
    go ptrs !i (fp:fps) = do
      withForeignPtr fp $ \p -> do
        writePrimArray ptrs i p
        go ptrs (i+1) fps

-------------------------------------------------------------------------------

withPrimSafe' :: forall a b. Prim a => a -> (MBA# a -> IO b) -> IO (a, b)
withPrimSafe' v f = do
    mpa@(MutablePrimArray mba#) <- newAlignedPinnedPrimArray 1
    writePrimArray mpa 0 v
    !b <- f mba#
    !a <- readPrimArray mpa 0
    return (a, b)
{-# INLINE withPrimSafe' #-}

peekVectorStringToCBytes :: Ptr (StdVector Z.StdString) -> IO [CBytes]
peekVectorStringToCBytes ptr = do
  size <- get_vector_of_string_size ptr
  ptr' <- get_vector_of_string_data ptr
  peekStdStringToCBytesN size ptr'

foreign import ccall unsafe "hs_logdevice.h hs_cal_std_string_off"
  hs_cal_std_string_off :: Ptr Z.StdString -> Int -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h delete_vector_of_string"
  delete_vector_of_string :: Ptr (StdVector Z.StdString) -> IO ()

foreign import ccall unsafe "hs_logdevice.h delete_vector_of_cint"
  delete_vector_of_cint :: Ptr (StdVector CInt) -> IO ()

foreign import ccall unsafe "hs_logdevice.h get_vector_of_string_size"
  get_vector_of_string_size :: Ptr (StdVector Z.StdString) -> IO Int

foreign import ccall unsafe "hs_logdevice.h get_vector_of_string_data"
  get_vector_of_string_data :: Ptr (StdVector Z.StdString) -> IO (Ptr Z.StdString)

-------------------------------------------------------------------------------
-- Internal helpers

-- Do NOT use this function unless you really know what you are doing.
-- Use 'withAsync' instead.
withAsync'
  :: HasCallStack
  => Int -> (Ptr a -> IO a)
  -> (HasCallStack => b -> IO c)
  -> (StablePtr PrimMVar -> Int -> Ptr a -> IO b)
  -> IO (a, c)
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
