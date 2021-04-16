module HStream.Store.Stream.Appender
  ( FFI.AppendCallBackData (appendCbLogID, appendCbLSN, appendCbTimestamp)
  , append
  , appendSync
  , appendSyncTS
  ) where

import           Control.Concurrent           (forkIO, myThreadId, newEmptyMVar,
                                               takeMVar, threadCapability)
import           Control.Exception            (mask_, onException)
import           Control.Monad                (void)
import           Data.Int                     (Int64)
import           Foreign.ForeignPtr           (mallocForeignPtrBytes,
                                               touchForeignPtr, withForeignPtr)
import           Foreign.Ptr                  (nullPtr)
import           GHC.Conc                     (newStablePtrPrimMVar)
import           GHC.Stack                    (HasCallStack)
import           Z.Data.CBytes                (CBytes)
import qualified Z.Data.CBytes                as ZC
import           Z.Data.Vector                (Bytes)
import qualified Z.Foreign                    as Z

import qualified HStream.Store.Exception      as E
import qualified HStream.Store.Internal.FFI   as FFI
import           HStream.Store.Internal.Types (SequenceNum (..),
                                               StreamClient (..), TopicID (..))
import qualified HStream.Store.Internal.Types as FFI

-------------------------------------------------------------------------------

-- | Appends a new record to the log. Block haskell thread until operation
-- completes.
append :: HasCallStack
       => StreamClient
       -> TopicID
       -> Bytes
       -> Maybe (FFI.KeyType, CBytes)
       -> IO FFI.AppendCallBackData
append (StreamClient client) (TopicID topicid) payload m_key_attr =
  withForeignPtr client $ \client' ->
  Z.withPrimVectorUnsafe payload $ \payload' offset len -> mask_ $ do
    mvar <- newEmptyMVar
    sp <- newStablePtrPrimMVar mvar  -- freed by hs_try_takemvar()
    fp <- mallocForeignPtrBytes FFI.appendCallBackDataSize
    result <- withForeignPtr fp $ \data' -> do
      (cap, _) <- threadCapability =<< myThreadId
      void $ E.throwStreamErrorIfNotOK $
        case m_key_attr of
          Just (keytype, keyval) -> ZC.withCBytesUnsafe keyval $ \keyval' ->
            FFI.c_logdevice_append_with_attrs_async sp cap data' client' topicid payload' offset len keytype keyval'
          Nothing ->
            FFI.c_logdevice_append_async sp cap data' client' topicid payload' offset len
      takeMVar mvar `onException` forkIO (do takeMVar mvar; touchForeignPtr fp)
      FFI.peekAppendCallBackData data'
    void $ E.throwStreamErrorIfNotOK' $ FFI.appendCbRetCode result
    return result

appendSync :: StreamClient
           -> TopicID
           -> Bytes
           -> Maybe (FFI.KeyType, CBytes)
           -> IO SequenceNum
appendSync (StreamClient client) (TopicID topicid) payload m_key_attr =
  withForeignPtr client $ \client' ->
  Z.withPrimVectorSafe payload $ \payload' len -> do
    (sn_ret, _) <- Z.withPrimSafe FFI.c_lsn_invalid $ \lsn' ->
      E.throwStreamErrorIfNotOK $
        case m_key_attr of
          Just (keytype, keyval) -> do
            ZC.withCBytes keyval $ \keyval' ->
              FFI.c_logdevice_append_with_attrs_sync_safe client' topicid payload' 0 len keytype keyval' nullPtr lsn'
          Nothing -> FFI.c_logdevice_append_sync_safe client' topicid payload' 0 len nullPtr lsn'
    return $ SequenceNum sn_ret

-- | The same as 'appendSync', but also return the timestamp that stored with
-- the record.
appendSyncTS :: StreamClient
             -> TopicID
             -> Bytes
             -> Maybe (FFI.KeyType, CBytes)
             -> IO (Int64, SequenceNum)
appendSyncTS (StreamClient client) (TopicID topicid) payload m_key_attr =
  withForeignPtr client $ \client' ->
  Z.withPrimVectorSafe payload $ \payload' len -> do
    (sn_ret, (ts, _)) <- Z.withPrimSafe FFI.c_lsn_invalid $ \lsn' ->
      Z.allocPrimSafe $ \ts' ->
        E.throwStreamErrorIfNotOK $
          case m_key_attr of
            Just (keytype, keyval) -> do
              ZC.withCBytes keyval $ \keyval' ->
                FFI.c_logdevice_append_with_attrs_sync_safe client' topicid payload' 0 len keytype keyval' ts' lsn'
            Nothing -> FFI.c_logdevice_append_sync_safe client' topicid payload' 0 len ts' lsn'
    return (ts, SequenceNum sn_ret)
