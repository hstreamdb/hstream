{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module HStream.Store.Stream
  ( -- * Stream Client
    StreamClient
  , newStreamClient
  , setClientSettings
  , getClientSettings
  , getMaxPayloadSize

    -- * Types
    -- ** Topic ID
  , TopicID
  , topicIDInvalid
  , topicIDInvalid'
  , mkTopicID
    -- ** Sequence Number
  , SequenceNum
  , FFI.sequenceNumInvalid
    -- ** Data Record
  , DataRecord (..)
    -- ** KeyType
  , FFI.KeyType
  , FFI.keyTypeFindKey
  , FFI.keyTypeFilterable

    -- * Topic Config
    -- ** Topic attributes
  , TopicAttributes
  , newTopicAttributes
  , setTopicReplicationFactor
  , setTopicReplicationFactor'
    -- ** Topic Group
  , StreamTopicGroup
  , makeTopicGroupSync
  , getTopicGroupSync
  , removeTopicGroupSync
  , removeTopicGroupSync'
  , topicGroupGetRange
  , topicGroupGetName
    -- ** Topic Directory
  , StreamTopicDirectory
  , makeTopicDirectory
  , topicDirectoryGetName

    -- * Writer
  , FFI.AppendCallBackData (appendCbLogID, appendCbLSN, appendCbTimestamp)
  , append
  , appendSync
  , appendSyncTS

    -- * Reader
  , StreamReader
  , newStreamReader
  , readerStartReading
  , readerStopReading
  , readerRead
  , tryReaderRead
  , readerIsReading
  , readerIsReadingAny

    -- * Misc
  , getTailSequenceNum
  , setLoggerlevelError

    -- * Re-export
  , Bytes
  ) where

import           Control.Concurrent      (forkIO, myThreadId, newEmptyMVar,
                                          takeMVar, threadCapability)
import           Control.Exception       (mask_, onException)
import           Control.Monad           (void)
import           Data.Int                (Int64)
import           Data.Word               (Word64)
import           Foreign.C.Types         (CSize)
import           Foreign.ForeignPtr      (ForeignPtr, mallocForeignPtrBytes,
                                          newForeignPtr, touchForeignPtr,
                                          withForeignPtr)
import           Foreign.Marshal         (allocaBytes)
import           Foreign.Ptr             (Ptr, nullPtr)
import           GHC.Conc                (newStablePtrPrimMVar)
import           Z.Data.CBytes           (CBytes)
import qualified Z.Data.CBytes           as ZC
import           Z.Data.Vector           (Bytes)
import qualified Z.Foreign               as Z

import           HStream.Internal.FFI    (DataRecord (..), SequenceNum (..),
                                          TopicID (..))
import qualified HStream.Internal.FFI    as FFI
import qualified HStream.Store.Exception as E

-------------------------------------------------------------------------------

newtype StreamClient = StreamClient
  { unStreamClient :: ForeignPtr FFI.LogDeviceClient }

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

-- TODO: assert all functions that recv TopicID as a param is a valid TopicID

-- TODO: validation
-- 1. invalid_min < topicID < invalid_max
mkTopicID :: Word64 -> TopicID
mkTopicID = TopicID

topicIDInvalid :: TopicID
topicIDInvalid = TopicID FFI.c_logid_invalid

topicIDInvalid' :: TopicID
topicIDInvalid' = TopicID FFI.c_logid_invalid2

newtype TopicAttributes = TopicAttributes
  { unTopicAttributes :: ForeignPtr FFI.LogDeviceLogAttributes }

newTopicAttributes :: IO TopicAttributes
newTopicAttributes = do
  i <- FFI.c_new_log_attributes
  TopicAttributes <$> newForeignPtr FFI.c_free_log_attributes_fun i

setTopicReplicationFactor :: TopicAttributes -> Int -> IO ()
setTopicReplicationFactor attrs val =
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
    FFI.c_log_attrs_set_replication_factor attrs' (fromIntegral val)

setTopicReplicationFactor' :: TopicAttributes -> Int -> IO TopicAttributes
setTopicReplicationFactor' attrs val =
  setTopicReplicationFactor attrs val >> return attrs

-------------------------------------------------------------------------------

newtype StreamTopicGroup = StreamTopicGroup
  { unStreamTopicGroup :: ForeignPtr FFI.LogDeviceLogGroup }

newtype StreamTopicDirectory = StreamTopicDirectory
  { unStreamTopicDirectory :: ForeignPtr FFI.LogDeviceLogDirectory }

makeTopicDirectory :: StreamClient
                   -> CBytes
                   -> TopicAttributes
                   -> Bool
                   -> IO StreamTopicDirectory
makeTopicDirectory client path attrs mkParent =
  withForeignPtr (unStreamClient client) $ \client' ->
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (dir', _) <- Z.withPrimUnsafe nullPtr $ \dir'' -> do
      void $ E.throwStreamErrorIfNotOK $
        FFI.c_ld_client_make_directory_sync client' path' mkParent attrs' dir''
    StreamTopicDirectory <$> newForeignPtr FFI.c_free_lodevice_logdirectory_fun dir'

topicDirectoryGetName :: StreamTopicDirectory -> IO CBytes
topicDirectoryGetName dir = withForeignPtr (unStreamTopicDirectory dir) $
  ZC.fromCString . FFI.c_ld_logdirectory_get_name

-- | Creates a log group under a specific directory path.
--
-- Note that, even after this method returns success, it may take some time
-- for the update to propagate to all servers, so the new log group may not
-- be usable for a few seconds (appends may fail with NOTFOUND or
-- NOTINSERVERCONFIG). Same applies to all other logs config update methods,
-- e.g. setAttributes().
makeTopicGroupSync :: StreamClient
                   -> CBytes
                   -> TopicID
                   -> TopicID
                   -> TopicAttributes
                   -> Bool
                   -> IO StreamTopicGroup
makeTopicGroupSync client path (TopicID start) (TopicID end) attrs mkParent =
  withForeignPtr (unStreamClient client) $ \client' ->
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' -> do
      void $ E.throwStreamErrorIfNotOK $
        FFI.c_ld_client_make_loggroup_sync client' path' start end attrs' mkParent group''
    StreamTopicGroup <$> newForeignPtr FFI.c_free_lodevice_loggroup_fun group'

getTopicGroupSync :: StreamClient -> CBytes -> IO StreamTopicGroup
getTopicGroupSync client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
      void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_get_loggroup_sync client' path' group''
    StreamTopicGroup <$> newForeignPtr FFI.c_free_lodevice_loggroup_fun group'

removeTopicGroupSync :: StreamClient -> CBytes -> IO ()
removeTopicGroupSync client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_remove_loggroup_sync client' path' nullPtr

-- | The same as 'removeTopicGroupSync', but return the version of the
-- logsconfig at which the topic group got removed.
removeTopicGroupSync' :: StreamClient -> CBytes -> IO Word64
removeTopicGroupSync' client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  ZC.withCBytesUnsafe path $ \path' -> do
    (version, _) <- Z.withPrimUnsafe 0 $ \version' ->
      E.throwStreamErrorIfNotOK $ FFI.c_ld_client_remove_loggroup_sync' client' path' version'
    return version

topicGroupGetRange :: StreamTopicGroup -> IO (TopicID, TopicID)
topicGroupGetRange group =
  withForeignPtr (unStreamTopicGroup group) $ \group' -> do
    (start_ret, (end_ret, _)) <- Z.withPrimUnsafe (FFI.c_logid_invalid) $ \start' -> do
      Z.withPrimUnsafe FFI.c_logid_invalid $ \end' ->
        FFI.c_ld_loggroup_get_range group' start' end'
    return (mkTopicID start_ret, mkTopicID end_ret)

topicGroupGetName :: StreamTopicGroup -> IO CBytes
topicGroupGetName group =
  withForeignPtr (unStreamTopicGroup group) $ \group' ->
    ZC.fromCString =<< FFI.c_ld_loggroup_get_name group'

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

newtype StreamReader = StreamReader
  { unStreamReader :: ForeignPtr FFI.LogDeviceReader }

newStreamReader :: StreamClient
                -> CSize
                -- ^ maximum number of logs that can be read from
                -- this Reader at the same time
                -> Int64
                -- ^ specify the read buffer size for this client, fallback
                -- to the value in settings if it is -1
                -> IO StreamReader
newStreamReader client max_logs buffer_size =
  withForeignPtr (unStreamClient client) $ \clientPtr -> do
    i <- FFI.c_new_logdevice_reader clientPtr max_logs buffer_size
    StreamReader <$> newForeignPtr FFI.c_free_logdevice_reader_fun i

-- | Start reading a log.
--
-- Any one topic can only be read once by a single Reader.  If this method is
-- called for the same topic multiple times, it restarts reading, optionally
-- at a different point.
readerStartReading :: StreamReader -> TopicID -> SequenceNum -> SequenceNum -> IO ()
readerStartReading reader (TopicID topicid) (SequenceNum startSeq) (SequenceNum untilSeq) =
  withForeignPtr (unStreamReader reader) $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_reader_start_reading ptr topicid startSeq untilSeq

readerStopReading :: StreamReader -> TopicID -> IO ()
readerStopReading reader (TopicID topicid) =
  withForeignPtr (unStreamReader reader) $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_reader_stop_reading ptr topicid

readerRead :: StreamReader -> Int -> IO ([DataRecord])
readerRead reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' -> go reader' payload'
  where
    go !rp !pp = do
      m_records <- tryReaderRead' rp pp maxlen
      case m_records of
        Just rs -> return rs
        Nothing -> go rp pp

-- | Attempts to read a batch of records synchronously.
tryReaderRead :: StreamReader -> Int -> IO (Maybe [DataRecord])
tryReaderRead reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' ->
    tryReaderRead' reader' payload' maxlen

tryReaderRead'
  :: Ptr FFI.LogDeviceReader
  -> Ptr DataRecord
  -> Int
  -> IO (Maybe [DataRecord])
tryReaderRead' readerp recordp maxlen = do
  (nread, _) <- Z.withPrimSafe 0 $ \len' -> void $ E.throwStreamErrorIfNotOK $
    FFI.c_logdevice_reader_read_safe readerp (fromIntegral maxlen) recordp len'
  hdResult recordp nread
  where
    hdResult p nread
      | nread >  0 = Just <$> FFI.peekDataRecords nread p
      | nread == 0 = return $ Just []
      | nread <  0 = return Nothing
    hdResult _ _   = error "Unexpected Error!"

readerIsReading :: StreamReader -> TopicID -> IO Bool
readerIsReading reader (TopicID topicid) =
  withForeignPtr (unStreamReader reader) $ \ptr ->
    FFI.cbool2bool <$> FFI.c_ld_reader_is_reading ptr topicid

readerIsReadingAny :: StreamReader -> IO Bool
readerIsReadingAny reader =
  withForeignPtr (unStreamReader reader) $
    fmap FFI.cbool2bool . FFI.c_ld_reader_is_reading_any

-------------------------------------------------------------------------------

getTailSequenceNum :: StreamClient -> TopicID -> IO SequenceNum
getTailSequenceNum client (TopicID topicid) =
  withForeignPtr (unStreamClient client) $ \p ->
    SequenceNum <$> FFI.c_ld_client_get_tail_lsn_sync p topicid

setLoggerlevelError :: IO ()
setLoggerlevelError = FFI.c_set_dbg_level_error
