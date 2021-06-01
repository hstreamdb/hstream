{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice
  ( newLDClient
  , getTailLSN
  , getMaxPayloadSize
  , setClientSetting
  , getClientSetting
  , trim

  , module HStream.Store.Internal.LogDevice.Checkpoint
  , module HStream.Store.Internal.LogDevice.LogConfigTypes
  , module HStream.Store.Internal.LogDevice.Reader
  , module HStream.Store.Internal.LogDevice.VersionedConfigStore
  , module HStream.Store.Internal.LogDevice.Writer
  ) where

import           Control.Monad
import           Data.Primitive
import           Data.Word
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack
import           Z.Data.CBytes                                         (CBytes)
import qualified Z.Data.CBytes                                         as CBytes
import           Z.Data.Vector                                         (Bytes)
import           Z.Foreign                                             (MBA#)
import qualified Z.Foreign                                             as Z

import qualified HStream.Store.Exception                               as E
import           HStream.Store.Internal.Types

import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.LogDevice.Checkpoint
import           HStream.Store.Internal.LogDevice.LogConfigTypes
import           HStream.Store.Internal.LogDevice.Reader
import           HStream.Store.Internal.LogDevice.VersionedConfigStore
import           HStream.Store.Internal.LogDevice.Writer

-------------------------------------------------------------------------------
-- Client

-- | Create a new client from config url.
newLDClient :: HasCallStack => CBytes -> IO LDClient
newLDClient config = CBytes.withCBytesUnsafe config $ \config' -> do
  (client', _) <- Z.withPrimUnsafe nullPtr $ \client'' ->
    E.throwStreamErrorIfNotOK $ c_new_logdevice_client config' client''
  newForeignPtr c_free_logdevice_client_fun client'

getTailLSN :: LDClient -> C_LogID -> IO LSN
getTailLSN client logid = withForeignPtr client $ flip c_ld_client_get_tail_lsn logid

-- | Returns the maximum permitted payload size for this client.
--
-- The default is 1MB, but this can be increased via changing the
-- max-payload-size setting.
getMaxPayloadSize :: LDClient -> IO Word
getMaxPayloadSize client = withForeignPtr client c_ld_client_get_max_payload_size

-- | Change settings for the Client.
--
-- Settings that are commonly used on the client:
--
-- connect-timeout
--    Connection timeout
--
-- handshake-timeout
--    Timeout for LogDevice protocol handshake sequence
--
-- num-workers
--    Number of worker threads on the client
--
-- client-read-buffer-size
--    Number of records to buffer while reading
--
-- max-payload-size
--    The maximum payload size that could be appended by the client
--
-- ssl-boundary
--    Enable SSL in cross-X traffic, where X is the setting. Example: if set
--    to "rack", all cross-rack traffic will be sent over SSL. Can be one of
--    "none", "node", "rack", "row", "cluster", "dc" or "region". If a value
--    other than "none" or "node" is specified, --my-location has to be
--    specified as well.
--
-- my-location
--    Specifies the location of the machine running the client. Used for
--    determining whether to use SSL based on --ssl-boundary. Format:
--    "{region}.{dc}.{cluster}.{row}.{rack}"
--
-- client-initial-redelivery-delay
--    Initial delay to use when downstream rejects a record or gap
--
-- client-max-redelivery-delay
--    Maximum delay to use when downstream rejects a record or gap
--
-- on-demand-logs-config
--    Set this to true if you want the client to get log configuration on
--    demand from the server when log configuration is not included in the
--    main config file.
--
-- enable-logsconfig-manager
--    Set this to true if you want to use the internal replicated storage for
--    logs configuration, this will ignore loading the logs section from the
--    config file.
setClientSetting :: HasCallStack => LDClient -> CBytes -> CBytes -> IO ()
setClientSetting client key val =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe key $ \key' ->
  CBytes.withCBytesUnsafe val $ \val' -> void $
    E.throwStreamErrorIfNotOK $ c_ld_client_set_settings client' key' val'

getClientSetting :: LDClient -> CBytes -> IO Bytes
getClientSetting client key =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe key $ \key' ->
    Z.fromStdString $ c_ld_client_get_settings client' key'

trim :: LDClient -> C_LogID -> LSN -> IO ()
trim client logid lsn =
  withForeignPtr client $ \client' -> do
    void $ E.throwStreamErrorIfNotOK' . fst =<<
      withAsyncPrimUnsafe' (0 :: ErrorCode)
                           (c_ld_client_trim client' logid lsn)
                           (E.throwSubmitIfNotOK callStack)

foreign import ccall unsafe "hs_logdevice.h new_logdevice_client"
  c_new_logdevice_client :: Z.BA# Word8
                         -> Z.MBA# (Ptr LogDeviceClient)
                         -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_client"
  c_free_logdevice_client_fun :: FunPtr (Ptr LogDeviceClient -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_get_max_payload_size"
  c_ld_client_get_max_payload_size :: Ptr LogDeviceClient -> IO Word

foreign import ccall unsafe "hs_logdevice.h ld_client_get_settings"
  c_ld_client_get_settings
    :: Ptr LogDeviceClient -> Z.BA# Word8 -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h ld_client_set_settings"
  c_ld_client_set_settings
    :: Ptr LogDeviceClient -> Z.BA# Word8 -> Z.BA# Word8 -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_get_tail_lsn_sync"
  c_ld_client_get_tail_lsn :: Ptr LogDeviceClient -> C_LogID -> IO LSN

foreign import ccall unsafe "hs_logdevice.h ld_client_trim"
  c_ld_client_trim :: Ptr LogDeviceClient
                   -> C_LogID
                   -> LSN
                   -> StablePtr PrimMVar -> Int
                   -> MBA# ErrorCode
                   -> IO Int
