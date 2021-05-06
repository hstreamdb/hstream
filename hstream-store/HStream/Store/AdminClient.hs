{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.AdminClient
  ( newStreamAdminClient
  , newRpcOptions
  , adminSyncGetVersion
  , adminSyncGetStatus
  , adminSyncGetPid
  , adminSyncAliveSince
  , FB_STATUS
  , pattern FFI.FB_STATUS_STARTING
  , pattern FFI.FB_STATUS_ALIVE
  , pattern FFI.FB_STATUS_DEAD
  , pattern FFI.FB_STATUS_STOPPING
  , pattern FFI.FB_STATUS_STOPPED
  , pattern FFI.FB_STATUS_WARNING
  ) where

import           Data.Int                     (Int64)
import           Data.Word                    (Word16, Word32)
import           Foreign.ForeignPtr           (newForeignPtr, withForeignPtr)
import           Foreign.Ptr                  (nullPtr)
import           GHC.Stack                    (HasCallStack, callStack)
import           Z.Data.CBytes                (CBytes)
import qualified Z.Data.CBytes                as ZC
import qualified Z.Data.Text                  as ZT
import qualified Z.Foreign                    as Z

import qualified HStream.Store.Exception      as E
import qualified HStream.Store.Internal.FFI   as FFI
import           HStream.Store.Internal.Types (FB_STATUS, RpcOptions (..),
                                               StreamAdminClient (..))
import qualified HStream.Store.Internal.Types as FFI


newStreamAdminClient :: HasCallStack
                     => CBytes -> Word16 -> Bool -> Word32
                     -> IO StreamAdminClient
newStreamAdminClient host port allowNameLookup timeout =
  ZC.withCBytesUnsafe host $ \host' -> do
    (client', ret) <- Z.withPrimUnsafe nullPtr $ \client'' ->
      FFI.c_new_logdevice_admin_async_client host' port allowNameLookup timeout client''
    if ret < 0
       then E.throwStoreError "Couldn't create an admin client for node, it might mean that the node is down." callStack
       else StreamAdminClient <$> newForeignPtr FFI.c_free_logdevice_admin_async_client_fun client'

newRpcOptions :: Int64 -> IO RpcOptions
newRpcOptions timeout = do
  options' <- FFI.c_new_thrift_rpc_options timeout
  RpcOptions <$> newForeignPtr FFI.c_free_thrift_rpc_options_fun options'

adminSyncGetVersion :: StreamAdminClient -> RpcOptions -> IO ZT.Text
adminSyncGetVersion (StreamAdminClient client) (RpcOptions options) =
  withForeignPtr client $ \client' ->
  withForeignPtr options $ \options' -> do
    ZT.validate <$> Z.fromStdString (FFI.ld_admin_sync_getVersion client' options')

adminSyncGetStatus :: StreamAdminClient -> RpcOptions -> IO FB_STATUS
adminSyncGetStatus (StreamAdminClient client) (RpcOptions options) =
  withForeignPtr client $ \client' ->
  withForeignPtr options (FFI.ld_admin_sync_getStatus client')

adminSyncAliveSince :: StreamAdminClient -> RpcOptions -> IO Int64
adminSyncAliveSince (StreamAdminClient client) (RpcOptions options) =
  withForeignPtr client $ \client' ->
  withForeignPtr options (FFI.ld_admin_sync_aliveSince client')

adminSyncGetPid :: StreamAdminClient -> RpcOptions -> IO Int64
adminSyncGetPid (StreamAdminClient client) (RpcOptions options) =
  withForeignPtr client $ \client' ->
  withForeignPtr options (FFI.ld_admin_sync_getPid client')
