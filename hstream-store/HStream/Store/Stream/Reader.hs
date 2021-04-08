{-# LANGUAGE BangPatterns #-}

module HStream.Store.Stream.Reader
  ( -- * Reader
    StreamReader
  , newStreamReader
  , readerStartReading
  , readerStopReading
  , readerSetTimeout
  , readerRead
  , tryReaderRead
  , readerIsReading
  , readerIsReadingAny

    -- * Checkpointed Reader
  , StreamSyncCheckpointedReader
  , newStreamSyncCheckpointedReader
  , checkpointedReaderStartReading
  , checkpointedReaderStopReading
  , checkpointedReaderSetTimeout
  , checkpointedReaderRead
  , tryCheckpointedReaderRead
  , checkpointedReaderIsReading
  , checkpointedReaderIsReadingAny
  , writeCheckpointsSync
  , writeLastCheckpointsSync
  ) where

import           Control.Monad              (void)
import           Data.Int                   (Int32, Int64)
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Word                  (Word32)
import           Foreign.C.Types            (CInt, CSize)
import           Foreign.ForeignPtr         (newForeignPtr, withForeignPtr)
import           Foreign.Marshal            (allocaBytes)
import           Foreign.Ptr                (Ptr, nullPtr)
import           Z.Data.CBytes              (CBytes)
import qualified Z.Data.CBytes              as ZC
import qualified Z.Foreign                  as Z

import qualified HStream.Store.Exception    as E
import           HStream.Store.Internal.FFI (CheckpointStore (..),
                                             DataRecord (..), SequenceNum (..),
                                             StreamClient (..),
                                             StreamReader (..),
                                             StreamSyncCheckpointedReader (..),
                                             TopicID (..))
import qualified HStream.Store.Internal.FFI as FFI

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

newStreamSyncCheckpointedReader :: CBytes
                              -> StreamReader
                              -> CheckpointStore
                              -> Word32
                              -> IO StreamSyncCheckpointedReader
newStreamSyncCheckpointedReader name reader store retries =
  ZC.withCBytesUnsafe name $ \name' ->
  withForeignPtr (unStreamReader reader) $ \reader' ->
  withForeignPtr (unCheckpointStore store) $ \store' -> do
    i <- FFI.c_new_logdevice_sync_checkpointed_reader name' reader' store' retries
    StreamSyncCheckpointedReader <$> newForeignPtr FFI.c_free_sync_checkpointed_reader_fun i

-------------------------------------------------------------------------------

-- | Start reading a log.
--
-- Any one topic can only be read once by a single Reader.  If this method is
-- called for the same topic multiple times, it restarts reading, optionally
-- at a different point.
readerStartReading :: StreamReader
                   -> TopicID
                   -> SequenceNum
                   -> SequenceNum
                   -> IO ()
readerStartReading (StreamReader reader) (TopicID topicid) (SequenceNum startSeq) (SequenceNum untilSeq) =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_reader_start_reading ptr topicid startSeq untilSeq

checkpointedReaderStartReading
  :: StreamSyncCheckpointedReader
  -> TopicID
  -> SequenceNum
  -> SequenceNum
  -> IO ()
checkpointedReaderStartReading (StreamSyncCheckpointedReader reader) (TopicID topicid) (SequenceNum startSeq) (SequenceNum untilSeq) =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_checkpointed_reader_start_reading ptr topicid startSeq untilSeq

readerStopReading :: StreamReader -> TopicID -> IO ()
readerStopReading (StreamReader reader) (TopicID topicid) =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_reader_stop_reading ptr topicid

checkpointedReaderStopReading :: StreamSyncCheckpointedReader -> TopicID -> IO ()
checkpointedReaderStopReading (StreamSyncCheckpointedReader reader) (TopicID topicid) =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ FFI.c_ld_checkpointed_reader_stop_reading ptr topicid

-- | Read a batch of records synchronously until there is some data received.
--
-- NOTE that if read timeouts, you will get an empty list.
readerRead :: StreamReader -> Int -> IO [DataRecord]
readerRead reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' -> go reader' payload'
  where
    go !rp !pp = do
      m_records <- tryReaderRead' rp nullPtr pp maxlen
      case m_records of
        Just rs -> return rs
        Nothing -> go rp pp

checkpointedReaderRead :: StreamSyncCheckpointedReader -> Int -> IO [DataRecord]
checkpointedReaderRead (StreamSyncCheckpointedReader reader) maxlen =
  withForeignPtr reader $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' -> go reader' payload'
  where
    go !rp !pp = do
      m_records <- tryReaderRead' nullPtr rp pp maxlen
      case m_records of
        Just rs -> return rs
        Nothing -> go rp pp

-- | Attempts to read a batch of records synchronously.
tryReaderRead :: StreamReader -> Int -> IO (Maybe [DataRecord])
tryReaderRead reader maxlen =
  withForeignPtr (unStreamReader reader) $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' ->
    tryReaderRead' reader' nullPtr payload' maxlen

tryCheckpointedReaderRead :: StreamSyncCheckpointedReader -> Int -> IO (Maybe [DataRecord])
tryCheckpointedReaderRead (StreamSyncCheckpointedReader reader) maxlen =
  withForeignPtr reader $ \reader' ->
  allocaBytes (maxlen * FFI.dataRecordSize) $ \payload' ->
    tryReaderRead' nullPtr reader' payload' maxlen

readerIsReading :: StreamReader -> TopicID -> IO Bool
readerIsReading (StreamReader reader) (TopicID topicid) =
  withForeignPtr reader $ \ptr ->
    FFI.cbool2bool <$> FFI.c_ld_reader_is_reading ptr topicid

checkpointedReaderIsReading :: StreamSyncCheckpointedReader -> TopicID -> IO Bool
checkpointedReaderIsReading (StreamSyncCheckpointedReader reader) (TopicID topicid) =
  withForeignPtr reader $ \ptr ->
    FFI.cbool2bool <$> FFI.c_ld_checkpointed_reader_is_reading ptr topicid

readerIsReadingAny :: StreamReader -> IO Bool
readerIsReadingAny (StreamReader reader) = withForeignPtr reader $
  fmap FFI.cbool2bool . FFI.c_ld_reader_is_reading_any

checkpointedReaderIsReadingAny :: StreamSyncCheckpointedReader -> IO Bool
checkpointedReaderIsReadingAny (StreamSyncCheckpointedReader reader) = withForeignPtr reader $
    fmap FFI.cbool2bool . FFI.c_ld_checkpointed_reader_is_reading_any

-- | Sets the limit on how long 'readerRead' calls may wait for records to
-- become available.  A timeout of -1 means no limit (infinite timeout).
-- A timeout of 0 means no waiting (nonblocking reads).
--
-- The maximum timeout is 2^31-1 milliseconds (about 24 days).  If a timeout
-- larger than that is passed in, it will be capped.
readerSetTimeout :: StreamReader -> Int32 -> IO CInt
readerSetTimeout (StreamReader reader) ms =
  withForeignPtr reader $ \reader' -> FFI.c_ld_reader_set_timeout reader' ms

checkpointedReaderSetTimeout :: StreamSyncCheckpointedReader -> Int32 -> IO CInt
checkpointedReaderSetTimeout (StreamSyncCheckpointedReader reader) ms =
  withForeignPtr reader $ \reader' -> FFI.c_ld_checkpointed_reader_set_timeout reader' ms

-------------------------------------------------------------------------------

writeCheckpointsSync :: StreamSyncCheckpointedReader
                     -> Map TopicID SequenceNum
                     -> IO ()
writeCheckpointsSync (StreamSyncCheckpointedReader reader) sns =
  withForeignPtr reader $ \reader' -> do
    let xs = Map.toList sns
    let ka = Z.primArrayFromList $ map (unTopicID . fst) xs
        va = Z.primArrayFromList $ map (unSequenceNum . snd) xs
    Z.withPrimArraySafe ka $ \ks' len ->
      Z.withPrimArraySafe va $ \vs' _len -> void $ E.throwStreamErrorIfNotOK $
        FFI.c_sync_write_checkpoints_safe reader' ks' vs' (fromIntegral len)

writeLastCheckpointsSync :: StreamSyncCheckpointedReader -> [TopicID] -> IO ()
writeLastCheckpointsSync (StreamSyncCheckpointedReader reader) xs =
  withForeignPtr reader $ \reader' -> do
    let topicIDs = Z.primArrayFromList $ map unTopicID xs
    Z.withPrimArraySafe topicIDs $ \id' len -> void $ E.throwStreamErrorIfNotOK $
      FFI.c_sync_write_last_read_checkpoints_safe reader' id' (fromIntegral len)

-------------------------------------------------------------------------------

tryReaderRead'
  :: Ptr FFI.LogDeviceReader
  -> Ptr FFI.LogDeviceSyncCheckpointedReader
  -> Ptr DataRecord
  -> Int
  -> IO (Maybe [DataRecord])
tryReaderRead' reader chkReader record maxlen =
  if reader /= nullPtr
     then do (nread, _) <- Z.withPrimSafe 0 $ \len' -> void $ E.throwStreamErrorIfNotOK $
                FFI.c_logdevice_reader_read_safe reader (fromIntegral maxlen) record len'
             hdResult record nread
     else do (nread, _) <- Z.withPrimSafe 0 $ \len' -> void $ E.throwStreamErrorIfNotOK $
                FFI.c_logdevice_checkpointed_reader_read_safe chkReader (fromIntegral maxlen) record len'
             hdResult record nread
  where
    hdResult p nread
      | nread >  0 = Just <$> FFI.peekDataRecords nread p
      | nread == 0 = return $ Just []
      | nread <  0 = return Nothing
    hdResult _ _   = error "Unexpected Error!"
