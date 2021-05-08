{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice.Checkpoint where

import           Control.Monad                  (void)
import           Data.Int                       (Int64)
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           Data.Word
import           Foreign.ForeignPtr             (newForeignPtr, withForeignPtr)
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.CBytes                  as ZC
import           Z.Foreign                      (BA#, MBA#)
import qualified Z.Foreign                      as Z

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

ckpStoreGetLSNSync :: LDCheckpointStore -> CBytes -> C_LogID -> IO LSN
ckpStoreGetLSNSync store customid logid =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (ret_lsn, _) <- Z.withPrimSafe LSN_INVALID $ \sn' ->
      E.throwStreamErrorIfNotOK $ c_checkpoint_store_get_lsn_sync_safe store' customid' logid sn'
    return ret_lsn

ckpStoreUpdateLSN :: LDCheckpointStore -> CBytes -> C_LogID -> LSN -> IO ()
ckpStoreUpdateLSN store customid logid sn =
  ZC.withCBytesUnsafe customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    (errno, _) <- FFI.withAsyncPrimUnsafe (0 :: ErrorCode) $
      c_checkpoint_store_update_lsn store' customid' logid sn
    void $ E.throwStreamErrorIfNotOK' errno

ckpStoreUpdateLSNSync :: LDCheckpointStore -> CBytes -> C_LogID -> LSN -> IO ()
ckpStoreUpdateLSNSync store customid logid sn =
  ZC.withCBytes customid $ \customid' ->
  withForeignPtr store $ \store' -> do
    void $ E.throwStreamErrorIfNotOK $ c_checkpoint_store_update_lsn_sync_safe store' customid' logid sn

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

foreign import ccall unsafe "hs_logdevice.h free_checkpoint_store"
  c_free_checkpoint_store :: Ptr LogDeviceCheckpointStore -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_checkpoint_store"
  c_free_checkpoint_store_fun :: FunPtr (Ptr LogDeviceCheckpointStore -> IO ())

foreign import ccall safe "hs_logdevice.h checkpoint_store_get_lsn_sync"
  c_checkpoint_store_get_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> Ptr LSN    -- ^ value out
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_get_lsn"
  c_checkpoint_store_get_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8     -- ^ customer_id
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> MBA# Word8     -- ^ ErrorCode
    -> MBA# Word8     -- ^ value out
    -> IO ()

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_lsn_sync"
  c_checkpoint_store_update_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> LSN
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_update_lsn"
  c_checkpoint_store_update_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA# Word8    -- ^ customer_id
    -> C_LogID
    -> LSN
    -> StablePtr PrimMVar -> Int
    -> MBA# Word8
    -> IO ()

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_multi_lsn_sync"
  c_checkpoint_store_update_multi_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8
    -> Ptr C_LogID
    -> Ptr LSN
    -> Word
    -> IO ErrorCode
