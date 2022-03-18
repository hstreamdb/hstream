{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}

module HStream.Store.Internal.LogDevice.Writer where

import           Control.Monad
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Internal       as BS
import           Data.Int
import           Data.Maybe                     (fromMaybe)
import           Data.Primitive
import           Data.Word
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack
import qualified Z.Data.CBytes                  as CBytes
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.Vector                  as V
import           Z.Data.Vector                  (Bytes)
import qualified Z.Foreign                      as Z
import           Z.Foreign                      (CInt)

import qualified HStream.Store.Exception        as E
import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.Types

data AppendCompletion = AppendCompletion
  { appendCompLogID     :: {-# UNPACK #-} !C_LogID
  , appendCompLSN       :: {-# UNPACK #-} !LSN
  , appendCompTimestamp :: {-# UNPACK #-} !C_Timestamp
  } deriving (Show)

append
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> Bytes
  -> Maybe (KeyType, CBytes)
  -> IO AppendCompletion
append client logid payload m_key_attr =
  withForeignPtr client $ \client' -> do
  Z.withPrimVectorSafe payload $ \payload' len -> do
    __append__ client' logid payload' 0 len m_key_attr
{-# INLINABLE append #-}

appendBS
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> BS.ByteString
  -> Maybe (KeyType, CBytes)
  -> IO AppendCompletion
appendBS client logid (BS.PS payload offset len) m_key_attr =
  withForeignPtr client $ \client' ->
  withForeignPtr payload $ \payload' -> do
    __append__ client' logid payload' offset len m_key_attr
{-# INLINABLE appendBS #-}

__append__
  :: HasCallStack
  => Ptr LogDeviceClient
  -> C_LogID
  -> Ptr Word8 -> Int -> Int
  -> Maybe (KeyType, CBytes)
  -> IO AppendCompletion
__append__ client logid payload offset len m_key_attr = do
  cfun <- case m_key_attr of
            Nothing -> return $ c_logdevice_append_async client logid payload offset len
            Just (keytype, keyval) -> CBytes.withCBytes keyval $ \keyval' ->
              return $ c_logdevice_append_with_attrs_async client logid payload offset len keytype keyval'
  AppendCallBackData{..} <- withAsync appendCallBackDataSize peekAppendCallBackData cfun
  void $ E.throwStreamErrorIfNotOK' appendCbRetCode
  return $ AppendCompletion appendCbLogID appendCbLSN appendCbTimestamp
{-# INLINE __append__ #-}

appendBatch
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> [Bytes]
  -> Compression
  -> Maybe (KeyType, CBytes)
  -> IO AppendCompletion
appendBatch client logid payloads compression m_key_attr = withForeignPtr client $ \client' -> do
  let pa = Z.primArrayFromList (map V.length payloads)
  Z.withPrimArrayListUnsafe (map V.arrVec payloads) $ \payloads' totalLen -> do
    let (comp, lvl) = fromCompression compression
    let (keyType, keyVal) = fromMaybe (KeyTypeUndefined, "") m_key_attr
    AppendCallBackData{..} <- CBytes.withCBytes keyVal $ \keyVal' ->
      case pa of
        Z.PrimArray ba# ->
          withAsync appendCallBackDataSize peekAppendCallBackData
                    (c_logdevice_append_batch client' logid payloads' ba# totalLen comp lvl keyType keyVal')
    void $ E.throwStreamErrorIfNotOK' appendCbRetCode
    return $ AppendCompletion appendCbLogID appendCbLSN appendCbTimestamp
{-# INLINABLE appendBatch #-}

-- TODO: support vector of ByteString
appendBatchBS
  :: HasCallStack
  => LDClient
  -> C_LogID
  -> [BS.ByteString]
  -> Compression
  -> Maybe (KeyType, CBytes)
  -> IO AppendCompletion
appendBatchBS client logid payloads compression m_key_attr = withForeignPtr client $ \client' -> do
  let (fps, lens) = unzip ((\(BS.PS payload _ofs len) -> (payload, fromIntegral len)) <$> payloads)
  Z.withPrimArraySafe (Z.primArrayFromList lens) $ \lens' num -> do
    withForeignPtrList fps $ \fps' _num -> do
      let (comp, lvl) = fromCompression compression
      let (keyType, keyVal) = fromMaybe (KeyTypeUndefined, "") m_key_attr
      AppendCallBackData{..} <- CBytes.withCBytes keyVal $ \keyVal' -> do
        withAsync appendCallBackDataSize peekAppendCallBackData
          (c_logdevice_append_batch_safe client' logid fps' lens' num comp lvl keyType keyVal')
      void $ E.throwStreamErrorIfNotOK' appendCbRetCode
      return $ AppendCompletion appendCbLogID appendCbLSN appendCbTimestamp

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

foreign import ccall unsafe "hs_logdevice.h logdevice_append_batch"
  c_logdevice_append_batch
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Z.BAArray# Word8 -> Z.BA# Int -> Int
    -> Int -> Int
    -> KeyType -> Ptr Word8       -- ^ attrs: optional_key
    -> StablePtr PrimMVar -> Int -> Ptr AppendCallBackData
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_batch_safe"
  c_logdevice_append_batch_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr (Ptr Word8) -> Ptr CInt -> Int
    -> Int -> Int
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
