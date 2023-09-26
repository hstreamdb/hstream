{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice.Checkpoint where

import           Control.Monad                  (void)
import           Data.Int                       (Int64)
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import qualified Data.Vector.Primitive          as VP
import           Data.Word
import           Foreign.ForeignPtr             (newForeignPtr, withForeignPtr)
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack                      (HasCallStack)
import qualified Z.Data.CBytes                  as ZC
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Foreign                      as Z
import           Z.Foreign                      (BA#, MBA#)

import qualified HStream.Foreign                as F
import qualified HStream.Store.Exception        as E
import qualified HStream.Store.Internal.Foreign as FFI
import           HStream.Store.Internal.Types

-------------------------------------------------------------------------------

-- | Create a file based checkpoint store.
--
-- Note: it's not safe to have multiple FileBasedVersionedConfigStore
-- objects created from the `root_path' accessing configs with the same
-- `key' concurrently. For the best practice, use only one
-- FileBasedVersionedConfigStore instance for one `root_path'.
newFileBasedCheckpointStore :: CBytes -> IO LDCheckpointStore
newFileBasedCheckpointStore root_path =
  ZC.withCBytesUnsafe root_path $ \path' -> do
    i <- c_new_file_based_checkpoint_store path'
    newForeignPtr c_free_checkpoint_store_fun i

newRSMBasedCheckpointStore
  :: LDClient
  -> C_LogID
  -> Int64
  -- ^ Timeout for the RSM to stop after calling shutdown, in milliseconds.
  -> IO LDCheckpointStore
newRSMBasedCheckpointStore client log_id stop_timeout =
  withForeignPtr client $ \client' -> do
    i <- c_new_rsm_based_checkpoint_store client' log_id stop_timeout
    newForeignPtr c_free_checkpoint_store_fun i

-- TODO: remove
-- | Creates a zookeeper based CheckpointStore.
--
-- zk path: "/logdevice/{}/checkpoints/"
newZookeeperBasedCheckpointStore :: LDClient -> IO LDCheckpointStore
newZookeeperBasedCheckpointStore client =
  withForeignPtr client $ \client' -> do
    i <- c_new_zookeeper_based_checkpoint_store client'
    newForeignPtr c_free_checkpoint_store_fun i

ckpStoreGetLSN :: LDCheckpointStore -> CBytes -> C_LogID -> IO LSN
ckpStoreGetLSN store customid logid =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (errno, lsn, _) <- FFI.withAsyncPrimUnsafe2 (0 :: ErrorCode) LSN_INVALID $
      c_checkpoint_store_get_lsn store' customid' logid
    _ <- E.throwStreamErrorIfNotOK' errno
    return lsn

ckpStoreGetAllCheckpoints' :: LDCheckpointStore -> CBytes -> IO [(C_LogID, LSN)]
ckpStoreGetAllCheckpoints' store customid =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (_, errno, ret) <- FFI.withAsyncPrimMapUnsafe
      C_OK
      F.peekN F.c_delete_vector_of_uint64
      F.peekN F.c_delete_vector_of_uint64
      (checkpoint_store_get_all_checkpoints store' customid')
    _ <- E.throwStreamErrorIfNotOK' errno
    pure ret

ckpStoreGetAllCheckpoints :: LDCheckpointStore -> CBytes -> IO (Map C_LogID LSN)
ckpStoreGetAllCheckpoints store customid =
  Map.fromList <$> ckpStoreGetAllCheckpoints' store customid

ckpStoreUpdateLSN :: LDCheckpointStore -> CBytes -> C_LogID -> LSN -> IO ()
ckpStoreUpdateLSN = ckpStoreUpdateLSN' (-1)

ckpStoreUpdateLSN' :: Int -> LDCheckpointStore -> CBytes -> C_LogID -> LSN -> IO ()
ckpStoreUpdateLSN' retries store customid logid sn =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    let f = FFI.withAsyncPrimUnsafe (0 :: ErrorCode) $ c_checkpoint_store_update_lsn store' customid' logid sn
    void $ FFI.retryWhileAgain f retries

ckpStoreUpdateMultiLSN
  :: LDCheckpointStore -> CBytes -> Map C_LogID LSN -> IO ()
ckpStoreUpdateMultiLSN = ckpStoreUpdateMultiLSN' (-1)

ckpStoreUpdateMultiLSN'
  :: Int -> LDCheckpointStore -> CBytes -> Map C_LogID LSN -> IO ()
ckpStoreUpdateMultiLSN' retries store customid sns =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    let xs = Map.toList sns
        ka = Z.primArrayFromList $ map fst xs
        va = Z.primArrayFromList $ map snd xs
    let f =
          Z.withPrimArrayUnsafe ka $ \ks' len ->
          Z.withPrimArrayUnsafe va $ \vs' _len ->
            FFI.withAsyncPrimUnsafe (0 :: ErrorCode) $
              checkpoint_store_update_multi_lsn store' customid' ks' vs' (fromIntegral len)
    void $ FFI.retryWhileAgain f retries

ckpStoreRemoveCheckpoints
  :: HasCallStack
  => LDCheckpointStore -> CBytes -> VP.Vector C_LogID -> IO ()
ckpStoreRemoveCheckpoints store customid (VP.Vector offset len (Z.ByteArray ba#)) =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (errno, _) <- FFI.withAsyncPrimUnsafe (0 :: ErrorCode) $
      checkpoint_store_remove_checkpoints store' customid' ba# offset len
    void $ E.throwStreamErrorIfNotOK' errno

ckpStoreRemoveAllCheckpoints :: HasCallStack => LDCheckpointStore -> CBytes -> IO ()
ckpStoreRemoveAllCheckpoints store customid =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (errno, _) <- FFI.withAsyncPrimUnsafe (0 :: ErrorCode) $
      checkpoint_store_remove_all_checkpoints store' customid'
    void $ E.throwStreamErrorIfNotOK' errno

foreign import ccall unsafe "hs_logdevice.h new_file_based_checkpoint_store"
  c_new_file_based_checkpoint_store :: BA# Word8 -> IO (Ptr LogDeviceCheckpointStore)

foreign import ccall unsafe "hs_logdevice.h new_rsm_based_checkpoint_store"
  c_new_rsm_based_checkpoint_store
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Int64
    -> IO (Ptr LogDeviceCheckpointStore)

foreign import ccall unsafe "hs_logdevice.h new_zookeeper_based_checkpoint_store"
  c_new_zookeeper_based_checkpoint_store
    :: Ptr LogDeviceClient
    -> IO (Ptr LogDeviceCheckpointStore)

foreign import ccall safe "hs_logdevice.h free_checkpoint_store"
  c_free_checkpoint_store :: Ptr LogDeviceCheckpointStore -> IO ()

-- VersionedConfigStore::shutdown may block
foreign import ccall safe "hs_logdevice.h &free_checkpoint_store"
  c_free_checkpoint_store_fun :: FunPtr (Ptr LogDeviceCheckpointStore -> IO ())

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_get_lsn"
  c_checkpoint_store_get_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8     -- ^ customer_id
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode  -- ^ value out: error code
    -> MBA# LSN        -- ^ value out: lsn
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_get_all_checkpoints"
  checkpoint_store_get_all_checkpoints
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8     -- ^ customer_id
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode  -- ^ value out: error code
    -> MBA# Int -> MBA# (Ptr C_LogID) -> MBA# (Ptr LSN)
    -> MBA# (Ptr (F.StdVector C_LogID)) -> MBA# (Ptr (F.StdVector LSN))
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_update_lsn"
  c_checkpoint_store_update_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8    -- ^ customer_id
    -> C_LogID
    -> LSN
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_update_multi_lsn"
  checkpoint_store_update_multi_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8    -- ^ customer_id
    -> BA# C_LogID -> BA# LSN -> Word  -- ^ map of (logid, lsn)
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_remove_checkpoints"
  checkpoint_store_remove_checkpoints
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8    -- ^ customer_id
    -> BA# C_LogID -> Int -> Int  -- ^ (bytearray, offset, length)
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_remove_all_checkpoints"
  checkpoint_store_remove_all_checkpoints
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8    -- ^ customer_id
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

-------------------------------------------------------------------------------
-- DEPRECATED

{-# DEPRECATED ckpStoreGetLSNSync "Use ckpStoreGetLSN instead" #-}
ckpStoreGetLSNSync :: LDCheckpointStore -> CBytes -> C_LogID -> IO LSN
ckpStoreGetLSNSync store customid logid =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (ret_lsn, _) <- Z.withPrimSafe LSN_INVALID $ \sn' ->
      E.throwStreamErrorIfNotOK $ c_checkpoint_store_get_lsn_sync_safe store' customid' logid sn'
    return ret_lsn

{-# DEPRECATED ckpStoreUpdateLSNSync "Use ckpStoreUpdateLSN instead" #-}
ckpStoreUpdateLSNSync :: LDCheckpointStore -> CBytes -> C_LogID -> LSN -> IO ()
ckpStoreUpdateLSNSync store customid logid sn =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    void $ E.throwStreamErrorIfNotOK $ c_checkpoint_store_update_lsn_sync_safe store' customid' logid sn

{-# DEPRECATED updateMultiSequenceNumSync "Use ckpStoreUpdateMultiLSN instead" #-}
updateMultiSequenceNumSync
  :: LDCheckpointStore
  -> CBytes
  -> Map C_LogID LSN
  -> IO ()
updateMultiSequenceNumSync store customid sns =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    let xs = Map.toList sns
    let ka = Z.primArrayFromList $ map fst xs
        va = Z.primArrayFromList $ map snd xs
    Z.withPrimArraySafe ka $ \ks' len ->
      Z.withPrimArraySafe va $ \vs' _len -> void $ E.throwStreamErrorIfNotOK $
        c_checkpoint_store_update_multi_lsn_sync_safe store' customid' ks' vs' (fromIntegral len)

foreign import ccall safe "hs_logdevice.h checkpoint_store_get_lsn_sync"
  c_checkpoint_store_get_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> Ptr LSN    -- ^ value out
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_lsn_sync"
  c_checkpoint_store_update_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> LSN
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_multi_lsn_sync"
  c_checkpoint_store_update_multi_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8
    -> Ptr C_LogID
    -> Ptr LSN
    -> Word
    -> IO ErrorCode
