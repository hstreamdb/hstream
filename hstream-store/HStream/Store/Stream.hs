{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module HStream.Store.Stream
  ( -- * Stream Client
    StreamClient
  , newStreamClient
  , setClientSettings
  , getClientSettings
  , getMaxPayloadSize

    -- ** Sequence Number
  , SequenceNum
  , FFI.sequenceNumInvalid
    -- ** Data Record
  , DataRecord (..)
    -- ** KeyType
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable

    -- * Writer
  , FFI.AppendCallBackData (appendCbLogID, appendCbLSN, appendCbTimestamp)
  , append
  , appendSync
  , appendSyncTS

    -- * Reader
  , module HStream.Store.Stream.Reader

    -- * Misc
  , getTailSequenceNum
  , setLoggerlevelError

    -- * Re-export
  , Bytes
  ) where

import           Control.Concurrent          (forkIO, myThreadId, newEmptyMVar,
                                              takeMVar, threadCapability)
import           Control.Exception           (mask_, onException)
import           Control.Monad               (void)
import           Data.Int                    (Int64)
import           Foreign.ForeignPtr          (mallocForeignPtrBytes,
                                              newForeignPtr, touchForeignPtr,
                                              withForeignPtr)
import           Foreign.Ptr                 (nullPtr)
import           GHC.Conc                    (newStablePtrPrimMVar)
import           Z.Data.CBytes               (CBytes)
import qualified Z.Data.CBytes               as ZC
import           Z.Data.Vector               (Bytes)
import qualified Z.Foreign                   as Z

import           HStream.Internal.FFI        (DataRecord (..), SequenceNum (..),
                                              StreamClient (..), TopicID (..))
import qualified HStream.Internal.FFI        as FFI
import qualified HStream.Store.Exception     as E
import           HStream.Store.Stream.Reader

-------------------------------------------------------------------------------

-- | Create a new stream client from config url.
newStreamClient :: CBytes -> IO StreamClient
newStreamClient config = ZC.withCBytesUnsafe config $ \config' -> do
  (client', _) <- Z.withPrimUnsafe nullPtr $ \client'' ->
    E.throwStreamErrorIfNotOK $ FFI.c_new_logdevice_client config' client''
  StreamClient <$> newForeignPtr FFI.c_free_logdevice_client_fun client'

-- | Returns the maximum permitted payload size for this client.
--
-- The default is 1MB, but this can be increased via changing the
-- max-payload-size setting.
getMaxPayloadSize :: StreamClient -> IO Word
getMaxPayloadSize (StreamClient client) =
  withForeignPtr client $ FFI.c_ld_client_get_max_payload_size

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
setClientSettings :: StreamClient -> CBytes -> CBytes -> IO ()
setClientSettings (StreamClient client) key val =
  withForeignPtr client $ \client' ->
  ZC.withCBytesUnsafe key $ \key' ->
  ZC.withCBytesUnsafe val $ \val' -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_client_set_settings client' key' val'

getClientSettings :: StreamClient -> CBytes -> IO Bytes
getClientSettings (StreamClient client) key =
  withForeignPtr client $ \client' ->
  ZC.withCBytesUnsafe key $ \key' ->
    Z.fromStdString $ FFI.c_ld_client_get_settings client' key'

-------------------------------------------------------------------------------

append :: StreamClient
       -> TopicID
       -> Bytes
       -> Maybe (FFI.KeyType, CBytes)
       -> (FFI.AppendCallBackData -> IO a)
       -> IO a
append (StreamClient client) (TopicID topicid) payload m_key_attr f =
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
    f result

-- | Appends a new record to the log. Blocks until operation completes.
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

-------------------------------------------------------------------------------

getTailSequenceNum :: StreamClient -> TopicID -> IO SequenceNum
getTailSequenceNum client (TopicID topicid) =
  withForeignPtr (unStreamClient client) $ \p ->
    SequenceNum <$> FFI.c_ld_client_get_tail_lsn_sync p topicid

setLoggerlevelError :: IO ()
setLoggerlevelError = FFI.c_set_dbg_level_error
