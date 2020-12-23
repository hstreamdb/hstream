{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module HStream.Store.Stream
  ( -- * Stream Client
    StreamClient
  , newStreamClient
    -- ** Sequence Number
  , SequenceNum
    -- ** Topic
  , TopicID
  , topicIDInvalid
  , topicIDInvalid'
  , mkTopicID
  , StreamTopicGroup
  , TopicAttributes
  , newTopicAttributes
  , setTopicReplicationFactor
  , setTopicReplicationFactor'
  , makeTopicGroupSync
  , getTopicGroupSync
  , topicGroupGetRange
  , topicGroupGetName
    -- ** Reader
  , StreamReader

    -- * Writer
  , append
  , appendAndRetTimestamp

    -- * Reader
  , newStreamReader
  , startReading
  , read
  , isReading
  , isReadingAny

    -- * Misc
  , getTailSequenceNum
  , setLoggerlevelError
  ) where

import           Control.Monad        (void)
import           Data.Int             (Int64)
import           Data.Word            (Word64)
import           Foreign.C.Types      (CSize)
import           Foreign.ForeignPtr   (ForeignPtr, newForeignPtr,
                                       withForeignPtr)
import           Foreign.Marshal      (allocaBytes)
import           Foreign.Ptr          (nullPtr)
import           Foreign.Storable     (Storable)
import           Prelude              hiding (read)
import qualified Z.Data.CBytes        as Z
import qualified Z.Data.Vector        as Z
import qualified Z.Foreign            as Z

import qualified HStream.Exception    as E
import qualified HStream.Internal.FFI as FFI

-------------------------------------------------------------------------------

newtype StreamClient = StreamClient
  { unStreamClient :: ForeignPtr FFI.LogDeviceClient }

-- | Create a new stream client from config file path.
newStreamClient :: Z.CBytes -> IO StreamClient
newStreamClient configPath = Z.withCBytesUnsafe configPath $ \ba -> do
  i <- FFI.c_new_logdevice_client ba
  StreamClient <$> newForeignPtr FFI.c_free_logdevice_client_fun i

newtype StreamReader = StreamReader
  { unStreamReader :: ForeignPtr FFI.LogDeviceReader }

newtype StreamTopicGroup = StreamTopicGroup
  { unStreamTopicGroup :: ForeignPtr FFI.LogDeviceLogGroup }

-- TODO: assert all functions that recv TopicID as a param is a valid TopicID

newtype TopicID = TopicID FFI.C_LogID
  deriving (Show, Eq, Ord, Num, Storable)

instance Bounded TopicID where
  minBound = TopicID FFI.c_logid_min
  maxBound = TopicID FFI.c_logid_max

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

setTopicReplicationFactor :: TopicAttributes -> Int -> IO TopicAttributes
setTopicReplicationFactor attrs val =
  setTopicReplicationFactor' attrs val >> return attrs

setTopicReplicationFactor' :: TopicAttributes -> Int -> IO ()
setTopicReplicationFactor' attrs val =
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
    FFI.c_log_attrs_set_replication_factor attrs' (fromIntegral val)

newtype SequenceNum = SequenceNum FFI.C_LSN
  deriving (Show, Eq, Ord, Num, Storable)

instance Bounded SequenceNum where
  minBound = SequenceNum FFI.c_lsn_oldest
  maxBound = SequenceNum FFI.c_lsn_max

type DataRecord = FFI.DataRecord

-------------------------------------------------------------------------------

-- Creates a log group under a specific directory path.
--
-- Note that, even after this method returns success, it may take some time
-- for the update to propagate to all servers, so the new log group may not
-- be usable for a few seconds (appends may fail with NOTFOUND or
-- NOTINSERVERCONFIG). Same applies to all other logs config update methods,
-- e.g. setAttributes().
makeTopicGroupSync :: StreamClient
                   -> Z.CBytes
                   -> TopicID
                   -> TopicID
                   -> TopicAttributes
                   -> Bool
                   -> IO StreamTopicGroup
makeTopicGroupSync client path (TopicID start) (TopicID end) attrs mkParent =
  withForeignPtr (unStreamClient client) $ \client' ->
  withForeignPtr (unTopicAttributes attrs) $ \attrs' ->
  Z.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' -> do
      void $ E.throwStreamErrorIfNotOK $
        FFI.c_ld_client_make_loggroup_sync client' path' start end attrs' mkParent group''
    StreamTopicGroup <$> newForeignPtr FFI.c_free_lodevice_loggroup_fun group'

getTopicGroupSync :: StreamClient -> Z.CBytes -> IO StreamTopicGroup
getTopicGroupSync client path =
  withForeignPtr (unStreamClient client) $ \client' ->
  Z.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
      void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_get_loggroup_sync client' path' group''
    StreamTopicGroup <$> newForeignPtr FFI.c_free_lodevice_loggroup_fun group'

topicGroupGetRange :: StreamTopicGroup -> IO (TopicID, TopicID)
topicGroupGetRange group =
  withForeignPtr (unStreamTopicGroup group) $ \group' -> do
    (start_ret, (end_ret, _)) <- Z.withPrimUnsafe (FFI.c_logid_invalid) $ \start' -> do
      Z.withPrimUnsafe FFI.c_logid_invalid $ \end' ->
        FFI.c_ld_loggroup_get_range group' start' end'
    return (mkTopicID start_ret, mkTopicID end_ret)

topicGroupGetName :: StreamTopicGroup -> IO Z.CBytes
topicGroupGetName group =
  withForeignPtr (unStreamTopicGroup group) $ \group' ->
    Z.fromCString =<< FFI.c_ld_loggroup_get_name group'

append :: StreamClient -> TopicID -> Z.Bytes -> IO SequenceNum
append client (TopicID topicid) payload =
  withForeignPtr (unStreamClient client) $ \clientPtr ->
    Z.withPrimVectorUnsafe payload $ \ba_data offset len -> SequenceNum <$>
      FFI.c_logdevice_append_sync clientPtr topicid ba_data offset len nullPtr

appendAndRetTimestamp
  :: StreamClient
  -> TopicID
  -> Z.Bytes
  -> IO (Int64, SequenceNum)
appendAndRetTimestamp client (TopicID topicid) payload =
  withForeignPtr (unStreamClient client) $ \clientPtr ->
    Z.withPrimVectorUnsafe payload $ \ba_data offset len -> do
      (ts, num) <- Z.allocPrimUnsafe $ FFI.c_logdevice_append_sync_ts clientPtr topicid ba_data offset len
      return (ts, SequenceNum num)

-------------------------------------------------------------------------------

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
startReading :: StreamReader -> TopicID -> SequenceNum -> SequenceNum -> IO Int
startReading reader (TopicID topicid) (SequenceNum startSeq) (SequenceNum untilSeq) =
  withForeignPtr (unStreamReader reader) $ \ptr ->
    fromIntegral <$> FFI.c_logdevice_reader_start_reading ptr topicid startSeq untilSeq

-- | Attempts to read a batch of records synchronously.
read :: StreamReader -> Int -> IO (Maybe [DataRecord])
read reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' -> do
    (nread, ret) <- Z.withPrimSafe 0 $ \len' ->
      FFI.c_logdevice_reader_read_sync_safe reader' (fromIntegral maxlen) payload' len'
    hdResult ret payload' (nread :: Int)
  where
    hdResult 0 p nread
      | nread >  0 = Just <$> FFI.peekDataRecords nread p
      | nread == 0 = return $ Just []
      | nread <  0 = return Nothing
    hdResult _ _ _ = error "DATALOSS!"  -- TODO: custom exception

isReading :: StreamReader -> TopicID -> IO Bool
isReading reader (TopicID topicid) =
  withForeignPtr (unStreamReader reader) $ \ptr ->
    FFI.cbool2bool <$> FFI.c_logdevice_reader_is_reading ptr topicid

isReadingAny :: StreamReader -> IO Bool
isReadingAny reader =
  withForeignPtr (unStreamReader reader) $
    fmap FFI.cbool2bool . FFI.c_logdevice_reader_is_reading_any

-------------------------------------------------------------------------------

getTailSequenceNum :: StreamClient -> TopicID -> IO SequenceNum
getTailSequenceNum client (TopicID topicid) =
  withForeignPtr (unStreamClient client) $ \p ->
    SequenceNum <$> FFI.c_ld_client_get_tail_lsn_sync p topicid

setLoggerlevelError :: IO ()
setLoggerlevelError = FFI.c_set_dbg_level_error
