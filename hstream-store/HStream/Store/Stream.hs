{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module HStream.Store.Stream
  ( -- * Stream Client
    StreamClient
  , SequenceNum
  , TopicID
  , newStreamClient
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

import           Data.Int             (Int64)
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

import qualified HStream.Internal.FFI as FFI

-------------------------------------------------------------------------------

newtype StreamClient = StreamClient
  { unStreamClient :: ForeignPtr FFI.LogDeviceClient }

newtype StreamReader = StreamReader
  { unStreamReader :: ForeignPtr FFI.LogDeviceReader }

-- TODO: assert all functions that recv TopicID as a param is a valid TopicID

newtype TopicID = TopicID FFI.C_LogID
  deriving (Show, Eq, Num, Storable)

instance Bounded TopicID where
  minBound = TopicID FFI.c_logid_min
  maxBound = TopicID FFI.c_logid_max

newtype SequenceNum = SequenceNum FFI.C_LSN
  deriving (Show, Eq, Num, Storable)

instance Bounded SequenceNum where
  minBound = SequenceNum FFI.c_lsn_oldest
  maxBound = SequenceNum FFI.c_lsn_max

type DataRecord = FFI.DataRecord

-------------------------------------------------------------------------------

-- | Create a new stream client from config file path.
newStreamClient :: Z.CBytes -> IO StreamClient
newStreamClient configPath = Z.withCBytesUnsafe configPath $ \ba -> do
  i <- FFI.c_new_logdevice_client ba
  StreamClient <$> newForeignPtr FFI.c_free_logdevice_client_fun i

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
startReading reader (TopicID topicid) (SequenceNum startSeq) (SequenceNum untilSeq)
  | startSeq > untilSeq = return 1
  | otherwise = withForeignPtr (unStreamReader reader) $ \ptr ->
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
    SequenceNum <$> FFI.c_logdevice_get_tail_lsn_sync p topicid

setLoggerlevelError :: IO ()
setLoggerlevelError = FFI.c_set_dbg_level_error
