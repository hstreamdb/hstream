{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice.Writer where

import           Data.Int
import           Data.Primitive
import           Data.Word
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.CBytes                  as CBytes
import           Z.Data.Vector                  (Bytes)
import qualified Z.Foreign                      as Z

import qualified HStream.Store.Exception        as E
import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.Types

append
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> Bytes
  -> Maybe (KeyType, CBytes)
  -> IO AppendCallBackData
append client logid payload m_key_attr = withForeignPtr client $ \client' -> do
  Z.withPrimVectorSafe payload $ \payload' len -> do
    cfun <- case m_key_attr of
              Nothing -> return $ c_logdevice_append_async client' logid payload' 0 len
              Just (keytype, keyval) -> CBytes.withCBytes keyval $ \keyval' ->
                return $ c_logdevice_append_with_attrs_async client' logid payload' 0 len keytype keyval'
    withAsync appendCallBackDataSize peekAppendCallBackData cfun
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
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int
    -- ^ Payload pointer,offset,length
    -> StablePtr PrimMVar -> Int -> Ptr AppendCallBackData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h logdevice_append_with_attrs_async"
  c_logdevice_append_with_attrs_async
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> Ptr Word8       -- ^ attrs: optional_key
    -> StablePtr PrimMVar -> Int -> Ptr AppendCallBackData
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
