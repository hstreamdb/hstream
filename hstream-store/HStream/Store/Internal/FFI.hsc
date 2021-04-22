{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
-- Note that we need this UnboxedTuples to force ghci use -fobject-code for all
-- related modules. Or ghci will complain "panic".
--
-- Also, manual add @{-# OPTIONS_GHC -fobject-code #-}@ is possible, but need
-- to add all imported local modules. :(
--
-- Relatead ghc issues:
-- * https://gitlab.haskell.org/ghc/ghc/-/issues/19733
-- * https://gitlab.haskell.org/ghc/ghc/-/issues/15454
{-# LANGUAGE UnboxedTuples #-}

module HStream.Store.Internal.FFI where

import           Control.Concurrent           (newEmptyMVar, takeMVar)
import           Control.Exception            (mask_, onException)
import           Control.Monad                (void)
import           Control.Monad.Primitive
import           Data.Int
import           Data.Primitive
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr           (mallocForeignPtrBytes,
                                               touchForeignPtr, withForeignPtr)
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Exts
import           GHC.Stack                    (HasCallStack)
import           Z.Foreign                    (BA##, MBA##)
import qualified Z.Foreign                    as Z

import qualified HStream.Store.Exception      as E
import           HStream.Store.Internal.Types

#include "hs_logdevice.h"

-------------------------------------------------------------------------------
-- Client

-- | Create a new logdeive client
foreign import ccall unsafe "hs_logdevice.h new_logdevice_client"
  c_new_logdevice_client :: BA## Word8
                         -> MBA## (Ptr LogDeviceClient)
                         -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h free_logdevice_client"
  c_free_logdevice_client :: Ptr LogDeviceClient -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_client"
  c_free_logdevice_client_fun :: FunPtr (Ptr LogDeviceClient -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_get_max_payload_size"
  c_ld_client_get_max_payload_size :: Ptr LogDeviceClient -> IO Word

foreign import ccall unsafe "hs_logdevice.h ld_client_get_settings"
  c_ld_client_get_settings :: Ptr LogDeviceClient
                           -> BA## Word8
                           -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h ld_client_set_settings"
  c_ld_client_set_settings :: Ptr LogDeviceClient
                           -> BA## Word8
                           -> BA## Word8
                           -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_get_tail_lsn_sync"
  c_ld_client_get_tail_lsn_sync :: Ptr LogDeviceClient
                                -> C_LogID
                                -> IO C_LSN

-------------------------------------------------------------------------------
-- LogAttributes

foreign import ccall unsafe "hs_logdevice.h new_log_attributes"
  c_new_log_attributes
    :: CInt
    -> Int -> Z.BAArray## Word8 -> Z.BAArray## Word8
    -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h get_log_attrs_extra"
  c_get_log_attrs_extra
    :: Ptr LogDeviceLogAttributes
    -> BA## Word8
    -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h set_log_attrs_extra"
  c_set_log_attrs_extra
    :: Ptr LogDeviceLogAttributes
    -> BA## Word8
    -> BA## Word8
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h free_log_attributes"
  c_free_log_attributes :: Ptr LogDeviceLogAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_attributes"
  c_free_log_attributes_fun :: FunPtr (Ptr LogDeviceLogAttributes -> IO ())

-------------------------------------------------------------------------------
-- LogsConfig

-- | This waits (blocks) until this Client's local view of LogsConfig catches up
-- to the given version or higher, or until the timeout has passed.
-- Doesn't wait for config propagation to servers.
--
-- This guarantees that subsequent get*() calls (getDirectory(), getLogGroup()
-- etc) will get an up-to-date view.
-- Does *not* guarantee that subsequent append(), makeDirectory(),
-- makeLogGroup(), etc, will have an up-to-date view.
foreign import ccall safe "hs_logdevice.h ld_client_sync_logsconfig_version"
  c_ld_client_sync_logsconfig_version
    :: Ptr LogDeviceClient
    -> Word64
    -- ^ The minimum version you need to sync LogsConfig to
    -> IO ErrorCode
    -- ^ Return TIMEDOUT on timeout or OK on successful.

foreign import ccall unsafe "hs_logdevice.h ld_client_make_directory_sync"
  c_ld_client_make_directory_sync
    :: Ptr LogDeviceClient
    -> BA## Word8   -- ^ path
    -> Bool
    -> Ptr LogDeviceLogAttributes
    -> MBA## (Ptr LogDeviceLogDirectory)
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h free_logdevice_logdirectory"
  c_free_logdevice_logdirectory :: Ptr LogDeviceLogDirectory -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_logdirectory"
  c_free_logdevice_logdirectory_fun :: FunPtr (Ptr LogDeviceLogDirectory -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_get_directory_sync"
  c_ld_client_get_directory_sync :: Ptr LogDeviceClient
                                 -> BA## Word8
                                 -> MBA## (Ptr LogDeviceLogDirectory)
                                 -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_remove_directory_sync"
  c_ld_client_remove_directory_sync_safe
    :: Ptr LogDeviceClient
    -> Ptr Word8   -- ^ path
    -> Bool
    -> Ptr Word64
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_name"
  c_ld_logdirectory_get_name :: Ptr LogDeviceLogDirectory -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_version"
  c_ld_logdirectory_get_version :: Ptr LogDeviceLogDirectory -> IO Word64

foreign import ccall safe "hs_logdevice.h ld_client_make_loggroup_sync"
  c_ld_client_make_loggroup_sync
    :: Ptr LogDeviceClient
    -> BA## Word8
    -> C_LogID
    -> C_LogID
    -> Ptr LogDeviceLogAttributes
    -> Bool
    -> MBA## (Ptr LogDeviceLogGroup) -- ^ result, can be nullptr
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_get_loggroup_sync"
  c_ld_client_get_loggroup_sync :: Ptr LogDeviceClient
                                -> BA## Word8
                                -> MBA## (Ptr LogDeviceLogGroup)
                                -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup_sync"
  c_ld_client_remove_loggroup_sync :: Ptr LogDeviceClient
                                    -> BA## Word8
                                    -> Ptr Word64
                                    -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup_sync"
  c_ld_client_remove_loggroup_sync' :: Ptr LogDeviceClient
                                    -> BA## Word8
                                    -> MBA## Word64
                                    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup"
  c_ld_client_remove_loggroup :: Ptr LogDeviceClient
                              -> BA## Word8
                              -> StablePtr PrimMVar -> Int
                              -> Ptr LogsconfigStatusCbData
                              -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_range"
  c_ld_loggroup_get_range :: Ptr LogDeviceLogGroup
                          -> MBA## C_LogID    -- ^ returned value, start logid
                          -> MBA## C_LogID    -- ^ returned value, end logid
                          -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_name"
  c_ld_loggroup_get_name :: Ptr LogDeviceLogGroup -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_attrs"
  c_ld_loggroup_get_attrs :: Ptr LogDeviceLogGroup -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_version"
  c_ld_loggroup_get_version :: Ptr LogDeviceLogGroup -> IO Word64

foreign import ccall safe "hs_logdevice.h free_logdevice_loggroup"
  c_free_logdevice_loggroup :: Ptr LogDeviceLogGroup -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_logdevice_loggroup"
  c_free_logdevice_loggroup_fun :: FunPtr (Ptr LogDeviceLogGroup -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_rename"
  c_ld_client_rename :: Ptr LogDeviceClient
                     -> BA## Word8    -- ^ from_path
                     -> BA## Word8    -- ^ to_path
                     -> StablePtr PrimMVar -> Int
                     -> Ptr LogsconfigStatusCbData
                     -> IO ErrorCode

-------------------------------------------------------------------------------
-- Writer API

foreign import ccall unsafe "hs_logdevice.h logdevice_append_async"
  c_logdevice_append_async
    :: StablePtr PrimMVar
    -> Int
    -> Ptr AppendCallBackData
    -> Ptr LogDeviceClient
    -> C_LogID
    -> BA## Word8 -> Int -> Int
    -- ^ Payload pointer,offset,length
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h logdevice_append_with_attrs_async"
  c_logdevice_append_with_attrs_async
    :: StablePtr PrimMVar
    -> Int
    -> Ptr AppendCallBackData
    -> Ptr LogDeviceClient
    -> C_LogID
    -> BA## Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> BA## Word8       -- ^ attrs: optional_key
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_sync"
  c_logdevice_append_sync_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int -- ^ Payload pointer,offset,length
    -> Ptr Int64      -- ^ returned timestamp, should be NULL
    -> Ptr C_LSN      -- ^ returned value, log sequence number
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_with_attrs_sync"
  c_logdevice_append_with_attrs_sync_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> (Ptr Word8)     -- ^ attrs: optional_key
    -> Ptr Int64      -- ^ returned timestamp, should be NULL
    -> Ptr C_LSN      -- ^ returned value, log sequence number
    -> IO ErrorCode

-------------------------------------------------------------------------------
-- Checkpoint

foreign import ccall unsafe "hs_logdevice.h new_file_based_checkpoint_store"
  c_new_file_based_checkpoint_store :: BA## Word8 -> IO (Ptr LogDeviceCheckpointStore)

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
    -> Ptr C_LSN    -- ^ value out
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_get_lsn"
  c_checkpoint_store_get_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA## Word8     -- ^ customer_id
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> MBA## Word8     -- ^ ErrorCode
    -> MBA## Word8     -- ^ value out
    -> IO ()

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_lsn_sync"
  c_checkpoint_store_update_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> C_LSN
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h checkpoint_store_update_lsn"
  c_checkpoint_store_update_lsn
    :: Ptr LogDeviceCheckpointStore
    -> BA## Word8    -- ^ customer_id
    -> C_LogID
    -> C_LSN
    -> StablePtr PrimMVar -> Int
    -> MBA## Word8
    -> IO ()

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_multi_lsn_sync"
  c_checkpoint_store_update_multi_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8
    -> Ptr C_LogID
    -> Ptr C_LSN
    -> Word
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h sync_write_checkpoints"
  c_sync_write_checkpoints_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> Ptr C_LogID
    -> Ptr C_LSN
    -> Word
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h write_checkpoints"
  c_write_checkpoints
    :: Ptr LogDeviceSyncCheckpointedReader
    -> BA## C_LogID
    -> BA## C_LSN
    -> Word
    -> StablePtr PrimMVar -> Int
    -> MBA## Word8
    -> IO ()

foreign import ccall safe "hs_logdevice.h sync_write_last_read_checkpoints"
  c_sync_write_last_read_checkpoints_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> Ptr C_LogID
    -> Word
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h write_last_read_checkpoints"
  c_write_last_read_checkpoints
    :: Ptr LogDeviceSyncCheckpointedReader
    -> BA## C_LogID
    -> Word
    -> StablePtr PrimMVar -> Int
    -> MBA## Word8
    -> IO ()

-------------------------------------------------------------------------------
-- Reader API

foreign import ccall unsafe "hs_logdevice.h new_logdevice_reader"
  c_new_logdevice_reader :: Ptr LogDeviceClient
                         -> CSize
                         -> Int64
                         -> IO (Ptr LogDeviceReader)

foreign import ccall unsafe "hs_logdevice.h free_logdevice_reader"
  c_free_logdevice_reader :: Ptr LogDeviceReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_reader"
  c_free_logdevice_reader_fun :: FunPtr (Ptr LogDeviceReader -> IO ())

foreign import ccall unsafe "hs_logdevice.h new_sync_checkpointed_reader"
  c_new_logdevice_sync_checkpointed_reader
    :: BA## Word8           -- ^ Reader name
    -> Ptr LogDeviceReader
    -> Ptr LogDeviceCheckpointStore
    -> Word32               -- ^ num of retries
    -> IO (Ptr LogDeviceSyncCheckpointedReader)

foreign import ccall unsafe "hs_logdevice.h free_sync_checkpointed_reader"
  c_free_sync_checkpointed_reader :: Ptr LogDeviceSyncCheckpointedReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_sync_checkpointed_reader"
  c_free_sync_checkpointed_reader_fun
    :: FunPtr (Ptr LogDeviceSyncCheckpointedReader -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_reader_start_reading"
  c_ld_reader_start_reading :: Ptr LogDeviceReader -> C_LogID -> C_LSN -> C_LSN -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_start_reading"
  c_ld_checkpointed_reader_start_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> C_LSN -> C_LSN -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_start_reading_from_ckp"
  c_ld_checkpointed_reader_start_reading_from_ckp :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> C_LSN -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_reader_stop_reading"
  c_ld_reader_stop_reading :: Ptr LogDeviceReader -> C_LogID -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_stop_reading"
  c_ld_checkpointed_reader_stop_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_reader_is_reading"
  c_ld_reader_is_reading :: Ptr LogDeviceReader -> C_LogID -> IO CBool
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_is_reading"
  c_ld_checkpointed_reader_is_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> IO CBool

foreign import ccall unsafe "hs_logdevice.h ld_reader_is_reading_any"
  c_ld_reader_is_reading_any :: Ptr LogDeviceReader -> IO CBool
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_is_reading_any"
  c_ld_checkpointed_reader_is_reading_any :: Ptr LogDeviceSyncCheckpointedReader -> IO CBool

foreign import ccall unsafe "hs_logdevice.h ld_reader_set_timeout"
  c_ld_reader_set_timeout :: Ptr LogDeviceReader -> Int32 -> IO CInt
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_set_timeout"
  c_ld_checkpointed_reader_set_timeout :: Ptr LogDeviceSyncCheckpointedReader -> Int32 -> IO CInt

foreign import ccall safe "hs_logdevice.h logdevice_reader_read"
  c_logdevice_reader_read_safe :: Ptr LogDeviceReader -> CSize -> Ptr DataRecord -> Ptr Int -> IO ErrorCode
foreign import ccall safe "hs_logdevice.h logdevice_checkpointed_reader_read"
  c_logdevice_checkpointed_reader_read_safe :: Ptr LogDeviceSyncCheckpointedReader -> CSize -> Ptr DataRecord -> Ptr Int -> IO ErrorCode

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_logdevice.h new_logdevice_admin_async_client"
  c_new_logdevice_admin_async_client
    :: BA## Word8 -> Word16 -> Bool
    -> Word32
    -> MBA## (Ptr LogDeviceAdminAsyncClient)
    -> IO CInt

foreign import ccall unsafe "hs_logdevice.h free_logdevice_admin_async_client"
  c_free_logdevice_admin_async_client :: Ptr LogDeviceAdminAsyncClient
                                      -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_logdevice_admin_async_client"
  c_free_logdevice_admin_async_client_fun
    :: FunPtr (Ptr LogDeviceAdminAsyncClient -> IO ())

foreign import ccall unsafe "hs_logdevice.h new_thrift_rpc_options"
  c_new_thrift_rpc_options :: Int64 -> IO (Ptr ThriftRpcOptions)

foreign import ccall unsafe "hs_logdevice.h free_thrift_rpc_options"
  c_free_thrift_rpc_options :: Ptr ThriftRpcOptions -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_thrift_rpc_options"
  c_free_thrift_rpc_options_fun :: FunPtr (Ptr ThriftRpcOptions -> IO ())

foreign import ccall safe "hs_logdevice.h ld_admin_sync_getVersion"
  ld_admin_sync_getVersion :: Ptr LogDeviceAdminAsyncClient
                           -> Ptr ThriftRpcOptions
                           -> IO (Ptr Z.StdString)

foreign import ccall safe "hs_logdevice.h ld_admin_sync_getStatus"
  ld_admin_sync_getStatus :: Ptr LogDeviceAdminAsyncClient
                          -> Ptr ThriftRpcOptions
                          -> IO FB_STATUS

foreign import ccall safe "hs_logdevice.h ld_admin_sync_aliveSince"
  ld_admin_sync_aliveSince :: Ptr LogDeviceAdminAsyncClient
                           -> Ptr ThriftRpcOptions
                           -> IO Int64

foreign import ccall safe "hs_logdevice.h ld_admin_sync_getPid"
  ld_admin_sync_getPid :: Ptr LogDeviceAdminAsyncClient
                       -> Ptr ThriftRpcOptions
                       -> IO Int64

-------------------------------------------------------------------------------
-- Misc

cbool2bool :: CBool -> Bool
cbool2bool = (/= 0)

withAsyncPrimUnsafe
  :: (Prim a)
  => a -> (StablePtr PrimMVar -> Int -> MBA## Word8 -> IO b)
  -> IO (a, b)
withAsyncPrimUnsafe a f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  Z.withPrimUnsafe a $ \a' -> do
    (cap, _) <- threadCapability =<< myThreadId
    b <- f sp cap a'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch## a'))
    return b
{-# INLINE withAsyncPrimUnsafe #-}

withAsyncPrimUnsafe2
  :: (Prim a, Prim b)
  => a -> b -> (StablePtr PrimMVar -> Int -> MBA## Word8 -> MBA## Word8 -> IO c)
  -> IO (a, b, c)
withAsyncPrimUnsafe2 a b f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  (a_, (b_, c_)) <- Z.withPrimUnsafe a $ \a' -> do
    Z.withPrimUnsafe b $ \b' -> do
      (cap, _) <- threadCapability =<< myThreadId
      c <- f sp cap a' b'
      takeMVar mvar `onException` forkIO (do takeMVar mvar; primitive_ (touch## a'); primitive_ (touch## b'))
      return c
  return (a_, b_, c_)
{-# INLINE withAsyncPrimUnsafe2 #-}

withAsync :: HasCallStack
          => Int -> (Ptr a -> IO a)
          -> (StablePtr PrimMVar -> Int -> Ptr a -> IO ErrorCode)
          -> IO a
withAsync size peek_data f = mask_ $ do
  mvar <- newEmptyMVar
  sp <- newStablePtrPrimMVar mvar
  fp <- mallocForeignPtrBytes size
  withForeignPtr fp $ \data' -> do
    (cap, _) <- threadCapability =<< myThreadId
    void $ E.throwStreamErrorIfNotOK' =<< f sp cap data'
    takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
    peek_data data'
{-# INLINE withAsync #-}
