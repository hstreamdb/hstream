module HStream.Store.AdminClient
  ( newStreamAdminClient
  , newRpcOptions
  , adminSyncGetVersion
  ) where

import           Data.Int                   (Int64)
import           Data.Word                  (Word16, Word32)
import           Foreign.ForeignPtr         (newForeignPtr, withForeignPtr)
import           Foreign.Ptr                (nullPtr)
import           GHC.Stack                  (HasCallStack, callStack)
import           Z.Data.CBytes              (CBytes)
import qualified Z.Data.CBytes              as ZC
import qualified Z.Data.Text                as ZT
import qualified Z.Foreign                  as Z

import qualified HStream.Store.Exception    as E
import           HStream.Store.Internal.FFI (RpcOptions (..),
                                             StreamAdminClient (..))
import qualified HStream.Store.Internal.FFI as FFI

newStreamAdminClient :: HasCallStack
                     => CBytes -> Word16 -> Bool -> Word32
                     -> IO StreamAdminClient
newStreamAdminClient host port allowNameLookup timeout =
  ZC.withCBytesUnsafe host $ \host' -> do
    (client', ret) <- Z.withPrimUnsafe nullPtr $ \client'' ->
      FFI.c_new_logdevice_admin_async_client host' port allowNameLookup timeout client''
    if ret < 0
       then E.throwUserStreamError "Couldn't create an admin client for node, it might mean that the node is down." callStack
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
