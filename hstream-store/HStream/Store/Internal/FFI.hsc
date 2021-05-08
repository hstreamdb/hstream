{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

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
