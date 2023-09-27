{-# LANGUAGE BangPatterns      #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MagicHash         #-}

module HStream.Store.Internal.LogDevice.Reader where

import           Control.Monad                  (forM, unless, void)
import qualified Data.ByteString                as BS
import           Data.Int                       (Int32, Int64)
import           Data.Map.Strict                (Map)
import qualified Data.Map.Strict                as Map
import           Data.Maybe                     (fromMaybe)
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal                (allocaBytes)
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack
import qualified Z.Data.CBytes                  as ZC
import           Z.Data.CBytes                  (CBytes)
import           Z.Data.Vector.Base             (Bytes)
import qualified Z.Foreign                      as Z

import           HStream.Foreign                (BA# (..), MBA# (..))
import qualified HStream.Logger                 as Log
import qualified HStream.Store.Exception        as E
import           HStream.Store.Internal.Foreign (cbool2bool, retryWhileAgain,
                                                 withAsyncPrimUnsafe)
import           HStream.Store.Internal.Types

-------------------------------------------------------------------------------

newLDReader
  :: LDClient
  -> CSize
  -- ^ maximum number of logs that can be read from
  -- this Reader at the same time
  -> Maybe Int64
  -- ^ specify the read buffer size for this client, fallback
  -- to the value in settings if it is Nothing
  -> IO LDReader
newLDReader client max_logs m_buffer_size =
  withForeignPtr client $ \clientPtr -> do
    let buffer_size = fromMaybe (-1) m_buffer_size
    i <- c_new_logdevice_reader clientPtr max_logs buffer_size
    newForeignPtr c_free_logdevice_reader_fun i

-- NOTE:
--
-- You must NOT use the reader after you pass it to this function, since
-- it a unique_ptr, the ownship has "moved".
--
-- For CheckpointStore, you have two choices. The underlying representation is
-- either a unique_ptr or shared_ptr.
newLDSyncCkpReader
  :: CBytes
  -> LDReader
  -> LDCheckpointStore
  -> IO LDSyncCkpReader
newLDSyncCkpReader name reader store =
  ZC.withCBytesUnsafe name $ \name' ->
  withForeignPtr reader $ \reader' ->
  withForeignPtr store $ \store' -> do
    -- FIXME: The number of retries when synchronously writing checkpoints.
    -- We only use the async cpp function, so this option has no means currently
    let retries = 10
    i <- c_new_logdevice_sync_checkpointed_reader (BA# name') reader' store' retries
    newForeignPtr c_free_sync_checkpointed_reader_fun i

-- | Start reading a log.
--
-- Any one topic can only be read once by a single Reader.  If this method is
-- called for the same topic multiple times, it restarts reading, optionally
-- at a different point.
readerStartReading
  :: HasCallStack
  => LDReader
  -> C_LogID
  -> LSN -> LSN
  -> IO ()
readerStartReading reader logid startSeq untilSeq =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ ld_reader_start_reading ptr logid startSeq untilSeq

ckpReaderStartReading :: HasCallStack => LDSyncCkpReader -> C_LogID -> LSN -> LSN -> IO ()
ckpReaderStartReading reader logid startSeq untilSeq =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ ld_checkpointed_reader_start_reading ptr logid startSeq untilSeq

startReadingFromCheckpointOrStart
  :: LDSyncCkpReader
  -> C_LogID
  -> Maybe LSN
  -> LSN
  -> IO ()
startReadingFromCheckpointOrStart reader logid m_start end =
  withForeignPtr reader $ \ptr -> void $ do
    let start = fromMaybe LSN_INVALID m_start
        warnSlow = Log.warnSlow 10000000{- max_expect: 10s -}
                                2000000{- duration: 2s -}
                                "Starting to read from checkpoint is slower than expected"
    E.throwStreamErrorIfNotOK . warnSlow $
      ld_start_reading_from_ckp_or_start ptr logid start end

-- | Start reading from checkpoint
--
-- Typically, the same as
-- > startReadingFromCheckpointOrStart reader logid Nothing until
startReadingFromCheckpoint
  :: LDSyncCkpReader
  -> C_LogID
  -> LSN
  -> IO ()
startReadingFromCheckpoint reader logid untilSeq =
  withForeignPtr reader $ \ptr -> void $ do
    let warnSlow = Log.warnSlow 10000000{- max_expect: 10s -}
                                2000000{- duration: 2s -}
                                "Starting to read from checkpoint is slower than expected"
    E.throwStreamErrorIfNotOK . warnSlow $
      ld_checkpointed_reader_start_reading_from_ckp ptr logid untilSeq

-- Exceptions:
--   NOTFOUND  the log is not being read, either because readerStartReading()
--             was never called (or readerStopReading() was called), or because
--             `until` LSN was reached
--   Any exceptions from AsyncReader::stopReading
readerStopReading :: HasCallStack => LDReader -> C_LogID -> IO ()
readerStopReading reader logid =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ c_ld_reader_stop_reading ptr logid

ckpReaderStopReading :: LDSyncCkpReader -> C_LogID -> IO ()
ckpReaderStopReading reader logid =
  withForeignPtr reader $ \ptr -> void $
    E.throwStreamErrorIfNotOK $ c_ld_checkpointed_reader_stop_reading ptr logid

-- | Read a batch of records synchronously until there is some data
-- received. Gaps are ignored
--
-- If read timeouts, you will get an empty list.
--
-- The call returns when any of this is true:
--
-- * `nrecords` records have been delivered
-- * there are no more records to deliver at the moment and the timeout
--   specified by 'readerSetTimeout' has been reached
-- * there are no more records to deliver at the moment and
--   'readerSetWaitOnlyWhenNoData' was called
-- * `until` LSN for some log was reached
-- * not reading any logs, possibly because the ends of all logs have been
--   reached (returns 0 quickly)
--
-- Note that even in the case of an infinite timeout, the call may deliver
-- less than `nrecords` data records when a gap is encountered.  The next
-- call to read() will deliver the gap.
--
-- Waiting will not be interrupted if a signal is delivered to the thread.
readerRead :: DataRecordFormat a => LDReader -> Int -> IO [DataRecord a]
readerRead reader maxlen =
  withForeignPtr reader $ \reader' ->
  allocaBytes (maxlen * dataRecordSize) $ go reader'
  where
    go !rp !pp = do
      m_records <- tryReaderRead' rp nullPtr pp nullPtr maxlen
      case m_records of
        Right rs -> return rs
        Left _   -> go rp pp

ckpReaderRead :: DataRecordFormat a => LDSyncCkpReader -> Int -> IO [DataRecord a]
ckpReaderRead reader maxlen =
  withForeignPtr reader $ \reader' ->
  allocaBytes (maxlen * dataRecordSize) $ go reader'
  where
    go !rp !pp = do
      m_records <- tryReaderRead' nullPtr rp pp nullPtr maxlen
      case m_records of
        Right rs -> return rs
        Left _   -> go rp pp

-- | Attempts to read a batch of records.
--
-- The call either delivers 0 or more (up to `maxlen`) data records, or
-- one gap record.
--
-- The call returns when a gap in sequence numbers is encountered or any of the
-- situations mentioned in `readerRead` happens
readerReadAllowGap :: DataRecordFormat a => LDReader -> Int -> IO (Either GapRecord [DataRecord a])
readerReadAllowGap reader maxlen =
  withForeignPtr reader $ \reader' ->
  allocaBytes (maxlen * dataRecordSize) $ \payload' ->
  allocaBytes gapRecordSize $ \gap ->
    refineLogRecord <$> tryReaderRead' reader' nullPtr payload' gap maxlen

ckpReaderReadAllowGap :: DataRecordFormat a => LDSyncCkpReader -> Int -> IO (Either GapRecord [DataRecord a])
ckpReaderReadAllowGap reader maxlen =
  withForeignPtr reader $ \reader' ->
  allocaBytes (maxlen * dataRecordSize) $ \payload' ->
  allocaBytes gapRecordSize $ \gap ->
    refineLogRecord <$> tryReaderRead' nullPtr reader' payload' gap maxlen

refineLogRecord :: Either (Maybe GapRecord) [DataRecord a] -> Either GapRecord [DataRecord a]
refineLogRecord (Left (Just gap)) = Left gap
refineLogRecord (Left Nothing)    = error "Unexpected Error!"
refineLogRecord (Right payload)   = Right payload

-- | Checks if a log is being read. Can be used to find out if the `until`
-- LSN (passed to startReading()) was reached (for a log that was being read).
readerIsReading :: LDReader -> C_LogID -> IO Bool
readerIsReading reader logid =
  withForeignPtr reader $ \ptr ->
    cbool2bool <$> c_ld_reader_is_reading ptr logid

checkpointedReaderIsReading :: LDSyncCkpReader -> C_LogID -> IO Bool
checkpointedReaderIsReading reader logid =
  withForeignPtr reader $ \ptr ->
    cbool2bool <$> c_ld_checkpointed_reader_is_reading ptr logid

-- | Checks if any log is being read.  Can be used to find out if the end was
-- reached for *all* logs that were being read.
readerIsReadingAny :: LDReader -> IO Bool
readerIsReadingAny reader = withForeignPtr reader $
  fmap cbool2bool . c_ld_reader_is_reading_any

checkpointedReaderIsReadingAny :: LDSyncCkpReader -> IO Bool
checkpointedReaderIsReadingAny reader = withForeignPtr reader $
    fmap cbool2bool . c_ld_checkpointed_reader_is_reading_any

-- | Sets the limit on how long 'readerRead' calls may wait for records to
-- become available.  A timeout of -1 means no limit (infinite timeout).
-- A timeout of 0 means no waiting (nonblocking reads).
--
-- Default is no limit.
--
-- The maximum timeout is 2^31-1 milliseconds (about 24 days).  If a timeout
-- larger than that is passed in, it will be capped.
readerSetTimeout :: HasCallStack => LDReader -> Int32 -> IO ()
readerSetTimeout reader ms =
  withForeignPtr reader $ \reader' -> do
    -- 0 on success, -1 if the parameter was invalid
    ret <- c_ld_reader_set_timeout reader' ms
    unless (ret == 0) $ E.throwStreamError C_INVALID_PARAM callStack

-- | Sets the limit on how long 'readerRead' calls may wait for records to
-- become available.  A timeout of -1 means no limit (infinite timeout).
-- A timeout of 0 means no waiting (nonblocking reads).
--
-- The maximum timeout is 2^31-1 milliseconds (about 24 days).  If a timeout
-- larger than that is passed in, it will be capped.
ckpReaderSetTimeout :: HasCallStack => LDSyncCkpReader -> Int32 -> IO ()
ckpReaderSetTimeout reader ms =
  withForeignPtr reader $ \reader' -> do
    -- 0 on success, -1 if the parameter was invalid
    ret <- c_ld_checkpointed_reader_set_timeout reader' ms
    unless (ret == 0) $ E.throwStreamError C_INVALID_PARAM callStack

readerSetWithoutPayload :: LDReader -> IO ()
readerSetWithoutPayload reader = withForeignPtr reader c_ld_reader_without_payload

ckpReaderSetWithoutPayload :: LDSyncCkpReader -> IO ()
ckpReaderSetWithoutPayload reader = withForeignPtr reader c_ld_ckp_reader_without_payload

-- If called, data records read by this Reader will start including
-- approximate amount of data written to given log up to current record
-- once it become available to Reader.
--
-- The value itself stored in 'DataRecord.recordByteOffset. Set as
-- 'RecordByteOffsetInvalid' if unavailable to Reader yet.
--
-- Only affects subsequent startReading() calls.
readerSetIncludeByteOffset :: LDReader -> IO ()
readerSetIncludeByteOffset reader = withForeignPtr reader c_ld_reader_include_byteoffset

ckpReaderSetIncludeByteOffset :: LDSyncCkpReader -> IO ()
ckpReaderSetIncludeByteOffset reader = withForeignPtr reader c_ld_ckp_reader_include_byteoffset

readerSetWaitOnlyWhenNoData :: LDReader -> IO ()
readerSetWaitOnlyWhenNoData reader = withForeignPtr reader c_ld_reader_wait_only_when_no_data

ckpReaderSetWaitOnlyWhenNoData :: LDSyncCkpReader -> IO ()
ckpReaderSetWaitOnlyWhenNoData reader = withForeignPtr reader c_ld_ckp_reader_wait_only_when_no_data

-------------------------------------------------------------------------------

writeCheckpoints
  :: HasCallStack
  => LDSyncCkpReader -> Map C_LogID LSN -> Int -> IO ()
writeCheckpoints reader sns retries =
  withForeignPtr reader $ \reader' -> do
    let xs = Map.toList sns
    let ka = Z.primArrayFromList $ map fst xs
        va = Z.primArrayFromList $ map snd xs
    Z.withPrimArrayUnsafe ka $ \ks' len ->
      Z.withPrimArrayUnsafe va $ \vs' _ -> do
        let f = withAsyncPrimUnsafe (0 :: ErrorCode) $
                  crb_write_checkpoints reader' (BA# ks') (BA# vs') (fromIntegral len)
        retryWhileAgain f retries
{-# INLINABLE writeCheckpoints #-}

writeLastCheckpoints :: LDSyncCkpReader -> [C_LogID] -> Int -> IO ()
writeLastCheckpoints reader xs retries =
  withForeignPtr reader $ \reader' -> do
    let topicIDs = Z.primArrayFromList xs
    Z.withPrimArrayUnsafe topicIDs $ \id' len -> do
      let f = withAsyncPrimUnsafe (0 :: ErrorCode) $
                crb_write_last_read_checkpoints reader' (BA# id') (fromIntegral len)
      retryWhileAgain f retries
{-# INLINABLE writeLastCheckpoints #-}

removeCheckpoints :: HasCallStack => LDSyncCkpReader -> [C_LogID] -> IO ()
removeCheckpoints reader xs = withForeignPtr reader $ \reader' -> do
  let logids = Z.primArrayFromList xs
  Z.withPrimArrayUnsafe logids $ \id' len -> do
    let f = crb_asyncRemoveCheckpoints reader' (BA# id') (fromIntegral len)
    (err, _ret) <- withAsyncPrimUnsafe (0 :: ErrorCode) f
    void $ E.throwStreamErrorIfNotOK' err

removeAllCheckpoints :: HasCallStack => LDSyncCkpReader -> IO ()
removeAllCheckpoints reader = withForeignPtr reader $ \reader' -> do
  (err, _ret) <- withAsyncPrimUnsafe (0 :: ErrorCode) (crb_asyncRemoveAllCheckpoints reader')
  void $ E.throwStreamErrorIfNotOK' err

{-# DEPRECATED writeCheckpointsSync "Don't use these, use writeCheckpoints instead" #-}
writeCheckpointsSync :: LDSyncCkpReader
                     -> Map C_LogID LSN
                     -> IO ()
writeCheckpointsSync reader sns =
  withForeignPtr reader $ \reader' -> do
    let xs = Map.toList sns
    let ka = Z.primArrayFromList $ map fst xs
        va = Z.primArrayFromList $ map snd xs
    Z.withPrimArraySafe ka $ \ks' len ->
      Z.withPrimArraySafe va $ \vs' _len -> void $ E.throwStreamErrorIfNotOK $
        c_sync_write_checkpoints_safe reader' ks' vs' (fromIntegral len)

{-# DEPRECATED writeLastCheckpointsSync "Don't use these, use writeLastCheckpoints instead" #-}
writeLastCheckpointsSync :: LDSyncCkpReader -> [C_LogID] -> IO ()
writeLastCheckpointsSync reader xs =
  withForeignPtr reader $ \reader' -> do
    let topicIDs = Z.primArrayFromList xs
    Z.withPrimArraySafe topicIDs $ \id' len -> void $ E.throwStreamErrorIfNotOK $
      c_sync_write_last_read_checkpoints_safe reader' id' (fromIntegral len)

-------------------------------------------------------------------------------

class DataRecordFormat a where
  peekDataFromPtr :: Ptr DataRecordInternal -> Int -> IO (DataRecord a)

instance DataRecordFormat Bytes where
  peekDataFromPtr = peekDataRecord

instance DataRecordFormat BS.ByteString where
  peekDataFromPtr = peekDataRecordBS

peekDataRecords :: DataRecordFormat a => Int -> Ptr DataRecordInternal -> IO [DataRecord a]
peekDataRecords len ptr = forM [0..len-1] (peekDataFromPtr ptr)

tryReaderRead'
  :: DataRecordFormat a
  => Ptr LogDeviceReader
  -> Ptr LogDeviceSyncCheckpointedReader
  -> Ptr DataRecordInternal
  -> Ptr GapRecord
  -> Int
  -> IO (Either (Maybe GapRecord) [DataRecord a])
tryReaderRead' reader chkReader record gap maxlen =
  if reader /= nullPtr
     then do (nread, _) <- Z.withPrimSafe 0 $ \len' -> void $ E.throwStreamErrorIfNotOK $
                c_logdevice_reader_read_safe reader (fromIntegral maxlen) record gap len'
             hdResult nread
     else do (nread, _) <- Z.withPrimSafe 0 $ \len' -> void $ E.throwStreamErrorIfNotOK $
                c_logdevice_checkpointed_reader_read_safe chkReader (fromIntegral maxlen) record gap len'
             hdResult nread
  where
    hdResult nread
      | nread >  0 = Right <$> peekDataRecords nread record
      | nread == 0 = return $ Right []
      | nread <  0 = Left <$> if gap == nullPtr
                              then pure Nothing
                              else Just <$> peekGapRecord gap
    hdResult _     = error "Unexpected Error!"

-------------------------------------------------------------------------------
-- Reader C API

foreign import ccall unsafe "hs_logdevice.h new_logdevice_reader"
  c_new_logdevice_reader :: Ptr LogDeviceClient
                         -> CSize
                         -> Int64
                         -> IO (Ptr LogDeviceReader)

foreign import ccall unsafe "hs_logdevice.h free_logdevice_reader"
  c_free_logdevice_reader :: Ptr LogDeviceReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_reader"
  c_free_logdevice_reader_fun :: FunPtr (Ptr LogDeviceReader -> IO ())

foreign import ccall unsafe "hs_logdevice.h new_sync_checkpointed_reader"
  c_new_logdevice_sync_checkpointed_reader
    :: BA# Word8           -- ^ Reader name
    -> Ptr LogDeviceReader
    -> Ptr LogDeviceCheckpointStore
    -> Word32               -- ^ num of retries
    -> IO (Ptr LogDeviceSyncCheckpointedReader)

foreign import ccall unsafe "hs_logdevice.h free_sync_checkpointed_reader"
  c_free_sync_checkpointed_reader :: Ptr LogDeviceSyncCheckpointedReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_sync_checkpointed_reader"
  c_free_sync_checkpointed_reader_fun
    :: FunPtr (Ptr LogDeviceSyncCheckpointedReader -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_reader_start_reading"
  ld_reader_start_reading :: Ptr LogDeviceReader -> C_LogID -> LSN -> LSN -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_start_reading"
  ld_checkpointed_reader_start_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> LSN -> LSN -> IO ErrorCode

-- this should be safe ffi because from_ckp may block
foreign import ccall safe "hs_logdevice.h ld_checkpointed_reader_start_reading_from_ckp"
  ld_checkpointed_reader_start_reading_from_ckp :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> LSN -> IO ErrorCode
foreign import ccall safe "hs_logdevice.h ld_start_reading_from_ckp_or_start"
  ld_start_reading_from_ckp_or_start :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> LSN -> LSN -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_reader_stop_reading"
  c_ld_reader_stop_reading :: Ptr LogDeviceReader -> C_LogID -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_stop_reading"
  c_ld_checkpointed_reader_stop_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_reader_is_reading"
  c_ld_reader_is_reading :: Ptr LogDeviceReader -> C_LogID -> IO CBool
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_is_reading"
  c_ld_checkpointed_reader_is_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> IO CBool

foreign import ccall unsafe "hs_logdevice.h ld_reader_is_reading_any"
  c_ld_reader_is_reading_any :: Ptr LogDeviceReader -> IO CBool
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_is_reading_any"
  c_ld_checkpointed_reader_is_reading_any :: Ptr LogDeviceSyncCheckpointedReader -> IO CBool

foreign import ccall unsafe "hs_logdevice.h ld_reader_set_timeout"
  c_ld_reader_set_timeout :: Ptr LogDeviceReader -> Int32 -> IO CInt
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_set_timeout"
  c_ld_checkpointed_reader_set_timeout :: Ptr LogDeviceSyncCheckpointedReader -> Int32 -> IO CInt

foreign import ccall unsafe "hs_logdevice.h ld_reader_without_payload"
  c_ld_reader_without_payload :: Ptr LogDeviceReader -> IO ()
foreign import ccall unsafe "hs_logdevice.h ld_ckp_reader_without_payload"
  c_ld_ckp_reader_without_payload :: Ptr LogDeviceSyncCheckpointedReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_reader_include_byteoffset"
  c_ld_reader_include_byteoffset :: Ptr LogDeviceReader -> IO ()
foreign import ccall unsafe "hs_logdevice.h ld_ckp_reader_include_byteoffset"
  c_ld_ckp_reader_include_byteoffset :: Ptr LogDeviceSyncCheckpointedReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_reader_wait_only_when_no_data"
  c_ld_reader_wait_only_when_no_data :: Ptr LogDeviceReader -> IO ()
foreign import ccall unsafe "hs_logdevice.h ld_ckp_reader_wait_only_when_no_data"
  c_ld_ckp_reader_wait_only_when_no_data :: Ptr LogDeviceSyncCheckpointedReader -> IO ()

foreign import ccall safe "hs_logdevice.h logdevice_reader_read"
  c_logdevice_reader_read_safe
    :: Ptr LogDeviceReader
    -> CSize
    -> Ptr DataRecordInternal
    -> Ptr GapRecord
    -> Ptr Int
    -> IO ErrorCode
foreign import ccall safe "hs_logdevice.h logdevice_checkpointed_reader_read"
  c_logdevice_checkpointed_reader_read_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> CSize
    -> Ptr DataRecordInternal
    -> Ptr GapRecord
    -> Ptr Int
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h crb_write_checkpoints"
  crb_write_checkpoints
    :: Ptr LogDeviceSyncCheckpointedReader
    -> BA# C_LogID
    -> BA# LSN
    -> Word
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h crb_write_last_read_checkpoints"
  crb_write_last_read_checkpoints
    :: Ptr LogDeviceSyncCheckpointedReader
    -> BA# C_LogID
    -> Word
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h crb_asyncRemoveCheckpoints"
  crb_asyncRemoveCheckpoints
    :: Ptr LogDeviceSyncCheckpointedReader
    -> BA# C_LogID -> Word
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h crb_asyncRemoveAllCheckpoints"
  crb_asyncRemoveAllCheckpoints
    :: Ptr LogDeviceSyncCheckpointedReader
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> IO ()

foreign import ccall safe "hs_logdevice.h sync_write_last_read_checkpoints"
  c_sync_write_last_read_checkpoints_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> Ptr C_LogID
    -> Word
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h sync_write_checkpoints"
  c_sync_write_checkpoints_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> Ptr C_LogID
    -> Ptr LSN
    -> Word
    -> IO ErrorCode
