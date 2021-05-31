{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice
  ( newLDClient
  , getTailLSN
  , getMaxPayloadSize
  , setClientSetting
  , getClientSetting
  , append
  , appendSync
  , appendSyncTS
  , trim

  , module HStream.Store.Internal.LogDevice.Checkpoint
  , module HStream.Store.Internal.LogDevice.LogConfigTypes
  , module HStream.Store.Internal.LogDevice.Reader
  , module HStream.Store.Internal.LogDevice.VersionedConfigStore
  ) where

import           Control.Concurrent
import           Control.Exception
import           Control.Monad
import           Data.Int
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
import qualified Z.Foreign                                             as Z

import qualified HStream.Store.Exception                               as E
import           HStream.Store.Internal.Types

import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.LogDevice.Checkpoint
import           HStream.Store.Internal.LogDevice.LogConfigTypes
import           HStream.Store.Internal.LogDevice.Reader
import           HStream.Store.Internal.LogDevice.VersionedConfigStore
import           Z.Foreign                                             (MBA#)

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

-------------------------------------------------------------------------------
-- Writer API

append
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> Bytes
  -> Maybe (KeyType, CBytes)
  -> IO AppendCallBackData
append client logid payload m_key_attr =
  withForeignPtr client $ \client' ->
  Z.withPrimVectorUnsafe payload $ \payload' offset len -> mask_ $ do
    mvar <- newEmptyMVar
    sp <- newStablePtrPrimMVar mvar  -- freed by hs_try_takemvar()
    fp <- mallocForeignPtrBytes appendCallBackDataSize
    result <- withForeignPtr fp $ \data' -> do
      (cap, _) <- threadCapability =<< myThreadId
      void $ E.throwStreamErrorIfNotOK $
        case m_key_attr of
          Just (keytype, keyval) -> CBytes.withCBytesUnsafe keyval $ \keyval' ->
            c_logdevice_append_with_attrs_async sp cap data' client' logid payload' offset len keytype keyval'
          Nothing ->
            c_logdevice_append_async sp cap data' client' logid payload' offset len
      takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
      peekAppendCallBackData data'
    void $ E.throwStreamErrorIfNotOK' $ appendCbRetCode result
    return result
{-# INLINABLE append #-}

appendSync
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> Bytes
  -> Maybe (KeyType, CBytes)
  -> IO LSN
appendSync client logid payload m_key_attr =
  withForeignPtr client $ \client' ->
  Z.withPrimVectorSafe payload $ \payload' len -> do
    (sn_ret, _) <- Z.withPrimSafe LSN_INVALID $ \lsn' ->
      E.throwStreamErrorIfNotOK $
        case m_key_attr of
          Just (keytype, keyval) -> do
            CBytes.withCBytes keyval $ \keyval' ->
              c_logdevice_append_with_attrs_sync_safe client' logid payload' 0 len keytype keyval' nullPtr lsn'
          Nothing -> c_logdevice_append_sync_safe client' logid payload' 0 len nullPtr lsn'
    return sn_ret

-- | The same as 'appendSync', but also return the timestamp that stored with
-- the record.
appendSyncTS
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> Bytes
  -> Maybe (KeyType, CBytes)
  -> IO (Int64, LSN)
appendSyncTS client logid payload m_key_attr =
  withForeignPtr client $ \client' ->
  Z.withPrimVectorSafe payload $ \payload' len -> do
    (sn_ret, (ts, _)) <- Z.withPrimSafe LSN_INVALID $ \lsn' ->
      Z.allocPrimSafe $ \ts' ->
        E.throwStreamErrorIfNotOK $
          case m_key_attr of
            Just (keytype, keyval) -> do
              CBytes.withCBytes keyval $ \keyval' ->
                c_logdevice_append_with_attrs_sync_safe client' logid payload' 0 len keytype keyval' ts' lsn'
            Nothing -> c_logdevice_append_sync_safe client' logid payload' 0 len ts' lsn'
    return (ts, sn_ret)

foreign import ccall unsafe "hs_logdevice.h logdevice_append_async"
  c_logdevice_append_async
    :: StablePtr PrimMVar
    -> Int
    -> Ptr AppendCallBackData
    -> Ptr LogDeviceClient
    -> C_LogID
    -> Z.BA# Word8 -> Int -> Int
    -- ^ Payload pointer,offset,length
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h logdevice_append_with_attrs_async"
  c_logdevice_append_with_attrs_async
    :: StablePtr PrimMVar
    -> Int
    -> Ptr AppendCallBackData
    -> Ptr LogDeviceClient
    -> C_LogID
    -> Z.BA# Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> Z.BA# Word8       -- ^ attrs: optional_key
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_sync"
  c_logdevice_append_sync_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int -- ^ Payload pointer,offset,length
    -> Ptr Int64      -- ^ returned timestamp, should be NULL
    -> Ptr LSN        -- ^ returned value, log sequence number
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_with_attrs_sync"
  c_logdevice_append_with_attrs_sync_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> (Ptr Word8)     -- ^ attrs: optional_key
    -> Ptr Int64      -- ^ returned timestamp, should be NULL
    -> Ptr LSN        -- ^ returned value, log sequence number
    -> IO ErrorCode
