{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MagicHash                  #-}
{-# LANGUAGE PatternSynonyms            #-}
{-# LANGUAGE UnliftedFFITypes           #-}

module HStream.Store.Internal.FFI where

import           Control.Exception (bracket_)
import           Control.Monad     (forM)
import           Data.Int
import           Data.Word
import           Foreign
import           Foreign.C
import           GHC.Conc          (PrimMVar)
import           GHC.Generics      (Generic)
import qualified Z.Data.JSON       as JSON
import           Z.Data.Vector     (Bytes)
import           Z.Foreign         (BA##, MBA##)
import qualified Z.Foreign         as Z

#include "hs_logdevice.h"

-------------------------------------------------------------------------------

newtype StreamClient = StreamClient
  { unStreamClient :: ForeignPtr LogDeviceClient }

newtype StreamAdminClient = StreamAdminClient
  { unStreamAdminClient :: ForeignPtr LogDeviceAdminAsyncClient }

newtype RpcOptions = RpcOptions
  { unRpcOptions :: ForeignPtr ThriftRpcOptions }

newtype StreamSyncCheckpointedReader = StreamSyncCheckpointedReader
  { unStreamSyncCheckpointedReader :: ForeignPtr LogDeviceSyncCheckpointedReader }
  deriving (Show, Eq)

newtype StreamReader = StreamReader
  { unStreamReader :: ForeignPtr LogDeviceReader }
  deriving (Show, Eq)

newtype CheckpointStore = CheckpointStore
  { unCheckpointStore :: ForeignPtr LogDeviceCheckpointStore }
  deriving (Show, Eq)

newtype TopicID = TopicID { unTopicID :: C_LogID }
  deriving (Show, Eq, Ord)

instance Bounded TopicID where
  minBound = TopicID c_logid_min
  maxBound = TopicID c_logid_max

newtype SequenceNum = SequenceNum { unSequenceNum :: C_LSN }
  deriving (Show, Eq, Ord, Num, JSON.JSON, Generic)

instance Bounded SequenceNum where
  minBound = SequenceNum c_lsn_oldest
  maxBound = SequenceNum c_lsn_max

sequenceNumInvalid :: SequenceNum
sequenceNumInvalid = SequenceNum c_lsn_invalid

newtype KeyType = KeyType C_KeyType
  deriving (Eq, Ord)

instance Show KeyType where
  show t
    | t == keyTypeFindKey = "FINDKEY"
    | t == keyTypeFilterable = "FILTERABLE"
    | otherwise = "UNDEFINED"

keyTypeFindKey :: KeyType
keyTypeFindKey = KeyType c_keytype_findkey

keyTypeFilterable :: KeyType
keyTypeFilterable = KeyType c_keytype_filterable

data DataRecord = DataRecord
  { recordLogID   :: TopicID
  , recordLSN     :: SequenceNum
  , recordPayload :: Bytes
  } deriving (Show)

dataRecordSize :: Int
dataRecordSize = (#size logdevice_data_record_t)

peekDataRecords :: Int -> Ptr DataRecord -> IO [DataRecord]
peekDataRecords len ptr = forM [0..len-1] (peekDataRecord ptr)

-- | Peek data record from a pointer and an offset, then release the payload
-- ignoring exceptions.
peekDataRecord :: Ptr DataRecord -> Int -> IO DataRecord
peekDataRecord ptr offset = bracket_ (return ()) release peekData
  where
    ptr' = ptr `plusPtr` (offset * dataRecordSize)
    peekData = do
      logid <- (#peek logdevice_data_record_t, logid) ptr'
      lsn <- (#peek logdevice_data_record_t, lsn) ptr'
      len <- (#peek logdevice_data_record_t, payload_len) ptr'
      payload <- flip Z.fromPtr len =<< (#peek logdevice_data_record_t, payload) ptr'
      return $ DataRecord (TopicID logid) (SequenceNum lsn) payload
    release = do
      payload_ptr <- (#peek logdevice_data_record_t, payload) ptr'
      free payload_ptr

data AppendCallBackData = AppendCallBackData
  { appendCbRetCode   :: !ErrorCode
  , appendCbLogID     :: !C_LogID
  , appendCbLSN       :: !C_LSN
  , appendCbTimestamp :: !C_Timestamp
  }

appendCallBackDataSize :: Int
appendCallBackDataSize = (#size logdevice_append_cb_data_t)

peekAppendCallBackData :: Ptr AppendCallBackData -> IO AppendCallBackData
peekAppendCallBackData ptr = do
  retcode <- (#peek logdevice_append_cb_data_t, st) ptr
  logid <- (#peek logdevice_append_cb_data_t, logid) ptr
  lsn <- (#peek logdevice_append_cb_data_t, lsn) ptr
  ts <- (#peek logdevice_append_cb_data_t, timestamp) ptr
  return $ AppendCallBackData retcode logid lsn ts

-------------------------------------------------------------------------------

data LogDeviceClient
data LogDeviceReader
data LogDeviceSyncCheckpointedReader
data LogDeviceLogGroup
data LogDeviceLogDirectory
data LogDeviceLogAttributes
data LogDeviceCheckpointStore
data LogDeviceAdminAsyncClient
data ThriftRpcOptions

type C_Timestamp = Int64

-- | LogID
type C_LogID = Word64

c_logid_min :: C_LogID
c_logid_min = 1

c_logid_invalid :: C_LogID
c_logid_invalid = (#const C_LOGID_INVALID)

c_logid_invalid2 :: C_LogID
c_logid_invalid2 = (#const C_LOGID_INVALID2)

c_logid_max :: C_LogID
c_logid_max = (#const C_LOGID_MAX)

c_user_logid_max :: C_LogID
c_user_logid_max = (#const C_USER_LOGID_MAX)

c_logid_max_bits :: CSize
c_logid_max_bits = (#const C_LOGID_MAX_BITS)

-- | Log Sequence Number
type C_LSN = Word64

c_lsn_invalid :: C_LSN
c_lsn_invalid = (#const C_LSN_INVALID)

c_lsn_oldest :: C_LSN
c_lsn_oldest = (#const C_LSN_OLDEST)

c_lsn_max :: C_LSN
c_lsn_max = (#const C_LSN_MAX)

type C_KeyType = Word8

c_keytype_findkey :: C_KeyType
c_keytype_findkey = (#const C_KeyType_FINDKEY)

c_keytype_filterable :: C_KeyType
c_keytype_filterable = (#const C_KeyType_FILTERABLE)

foreign import ccall unsafe "hs_logdevice.h new_log_attributes"
  c_new_log_attributes :: IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h free_log_attributes"
  c_free_log_attributes :: Ptr LogDeviceLogAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_attributes"
  c_free_log_attributes_fun :: FunPtr (Ptr LogDeviceLogAttributes -> IO ())

foreign import ccall unsafe "hs_logdevice.h with_replicationFactor"
  c_log_attrs_set_replication_factor :: Ptr LogDeviceLogAttributes -> CInt -> IO ()

-------------------------------------------------------------------------------

-- | Error
type ErrorCode = Word16

foreign import ccall unsafe "hs_logdevice.h show_error_name"
  c_show_error_name :: ErrorCode -> CString

foreign import ccall unsafe "hs_logdevice.h show_error_description"
  c_show_error_description :: ErrorCode -> CString

-- | DebugLevel
type C_DBG_LEVEL = Word8

pattern C_DBG_CRITICAL :: C_DBG_LEVEL
pattern C_DBG_CRITICAL = (#const C_DBG_CRITICAL)

pattern C_DBG_ERROR :: C_DBG_LEVEL
pattern C_DBG_ERROR = (#const C_DBG_ERROR)

pattern C_DBG_WARNING :: C_DBG_LEVEL
pattern C_DBG_WARNING = (#const C_DBG_WARNING)

pattern C_DBG_NOTIFY :: C_DBG_LEVEL
pattern C_DBG_NOTIFY = (#const C_DBG_NOTIFY)

pattern C_DBG_INFO :: C_DBG_LEVEL
pattern C_DBG_INFO = (#const C_DBG_INFO)

pattern C_DBG_DEBUG :: C_DBG_LEVEL
pattern C_DBG_DEBUG = (#const C_DBG_DEBUG)

pattern C_DBG_SPEW :: C_DBG_LEVEL
pattern C_DBG_SPEW = (#const C_DBG_SPEW)

foreign import ccall unsafe "hs_logdevice.h set_dbg_level"
  c_set_dbg_level :: C_DBG_LEVEL -> IO ()

foreign import ccall unsafe "hs_logdevice.h dbg_use_fd"
  c_dbg_use_fd :: CInt -> IO CInt

-------------------------------------------------------------------------------
-- Client

-- | Create a new logdeive client
foreign import ccall unsafe "hs_logdevice.h new_logdevice_client"
  c_new_logdevice_client :: BA## Word8
                         -> MBA## (Ptr LogDeviceClient)
                         -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h free_logdevice_client"
  c_free_logdevice_client :: Ptr LogDeviceClient -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_client"
  c_free_logdevice_client_fun :: FunPtr (Ptr LogDeviceClient -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_get_max_payload_size"
  c_ld_client_get_max_payload_size :: Ptr LogDeviceClient -> IO Word

foreign import ccall unsafe "hs_logdevice.h ld_client_get_settings"
  c_ld_client_get_settings :: Ptr LogDeviceClient
                           -> BA## Word8
                           -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h ld_client_set_settings"
  c_ld_client_set_settings :: Ptr LogDeviceClient
                           -> BA## Word8
                           -> BA## Word8
                           -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_get_tail_lsn_sync"
  c_ld_client_get_tail_lsn_sync :: Ptr LogDeviceClient
                                -> C_LogID
                                -> IO C_LSN

-------------------------------------------------------------------------------

-- | This waits (blocks) until this Client's local view of LogsConfig catches up
-- to the given version or higher, or until the timeout has passed.
-- Doesn't wait for config propagation to servers.
--
-- This guarantees that subsequent get*() calls (getDirectory(), getLogGroup()
-- etc) will get an up-to-date view.
-- Does *not* guarantee that subsequent append(), makeDirectory(),
-- makeLogGroup(), etc, will have an up-to-date view.
foreign import ccall safe "hs_logdevice.h ld_client_sync_logsconfig_version"
  c_ld_client_sync_logsconfig_version_safe
    :: Ptr LogDeviceClient
    -> Word64
    -- ^ The minimum version you need to sync LogsConfig to
    -> IO ErrorCode
    -- ^ Return TIMEDOUT on timeout or OK on successful.

foreign import ccall unsafe "hs_logdevice.h ld_client_make_directory_sync"
  c_ld_client_make_directory_sync
    :: Ptr LogDeviceClient
    -> BA## Word8   -- ^ path
    -> Bool
    -> Ptr LogDeviceLogAttributes
    -> MBA## (Ptr LogDeviceLogDirectory)
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h free_logdevice_logdirectory"
  c_free_logdevice_logdirectory :: Ptr LogDeviceLogDirectory -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_logdirectory"
  c_free_logdevice_logdirectory_fun :: FunPtr (Ptr LogDeviceLogDirectory -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_get_directory_sync"
  c_ld_client_get_directory_sync :: Ptr LogDeviceClient
                                 -> BA## Word8
                                 -> MBA## (Ptr LogDeviceLogDirectory)
                                 -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_remove_directory_sync"
  c_ld_client_remove_directory_sync_safe
    :: Ptr LogDeviceClient
    -> Ptr Word8   -- ^ path
    -> Bool
    -> Ptr Word64
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_name"
  c_ld_logdirectory_get_name :: Ptr LogDeviceLogDirectory -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_version"
  c_ld_logdirectory_get_version :: Ptr LogDeviceLogDirectory -> IO Word64

foreign import ccall unsafe "hs_logdevice.h ld_client_make_loggroup_sync"
  c_ld_client_make_loggroup_sync
    :: Ptr LogDeviceClient
    -> BA## Word8
    -> C_LogID
    -> C_LogID
    -> Ptr LogDeviceLogAttributes
    -> Bool
    -> MBA## (Ptr LogDeviceLogGroup) -- ^ result, can be nullptr
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_get_loggroup_sync"
  c_ld_client_get_loggroup_sync :: Ptr LogDeviceClient
                                -> BA## Word8
                                -> MBA## (Ptr LogDeviceLogGroup)
                                -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup_sync"
  c_ld_client_remove_loggroup_sync :: Ptr LogDeviceClient
                                    -> BA## Word8
                                    -> Ptr Word64
                                    -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup_sync"
  c_ld_client_remove_loggroup_sync' :: Ptr LogDeviceClient
                                    -> BA## Word8
                                    -> MBA## Word64
                                    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_range"
  c_ld_loggroup_get_range :: Ptr LogDeviceLogGroup
                          -> MBA## C_LogID    -- ^ returned value, start logid
                          -> MBA## C_LogID    -- ^ returned value, end logid
                          -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_name"
  c_ld_loggroup_get_name :: Ptr LogDeviceLogGroup
                          -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_version"
  c_ld_loggroup_get_version :: Ptr LogDeviceLogGroup
                            -> IO Word64

foreign import ccall safe "hs_logdevice.h free_logdevice_loggroup"
  c_free_logdevice_loggroup :: Ptr LogDeviceLogGroup -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_logdevice_loggroup"
  c_free_logdevice_loggroup_fun :: FunPtr (Ptr LogDeviceLogGroup -> IO ())

-------------------------------------------------------------------------------
-- Writer API

foreign import ccall unsafe "hs_logdevice.h logdevice_append_async"
  c_logdevice_append_async
    :: StablePtr PrimMVar
    -> Int
    -> Ptr AppendCallBackData
    -> Ptr LogDeviceClient
    -> C_LogID
    -> BA## Word8 -> Int -> Int
    -- ^ Payload pointer,offset,length
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h logdevice_append_with_attrs_async"
  c_logdevice_append_with_attrs_async
    :: StablePtr PrimMVar
    -> Int
    -> Ptr AppendCallBackData
    -> Ptr LogDeviceClient
    -> C_LogID
    -> BA## Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> BA## Word8       -- ^ attrs: optional_key
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_sync"
  c_logdevice_append_sync_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int -- ^ Payload pointer,offset,length
    -> Ptr Int64      -- ^ returned timestamp, should be NULL
    -> Ptr C_LSN      -- ^ returned value, log sequence number
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h logdevice_append_with_attrs_sync"
  c_logdevice_append_with_attrs_sync_safe
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Ptr Word8 -> Int -> Int    -- ^ Payload pointer,offset,length
    -> KeyType -> (Ptr Word8)     -- ^ attrs: optional_key
    -> Ptr Int64      -- ^ returned timestamp, should be NULL
    -> Ptr C_LSN      -- ^ returned value, log sequence number
    -> IO ErrorCode

-------------------------------------------------------------------------------
-- Checkpoint

foreign import ccall unsafe "hs_logdevice.h new_file_based_checkpoint_store"
  c_new_file_based_checkpoint_store :: BA## Word8 -> IO (Ptr LogDeviceCheckpointStore)

foreign import ccall unsafe "hs_logdevice.h new_rsm_based_checkpoint_store"
  c_new_rsm_based_checkpoint_store
    :: Ptr LogDeviceClient
    -> C_LogID
    -> C_Timestamp
    -> IO (Ptr LogDeviceCheckpointStore)

foreign import ccall unsafe "hs_logdevice.h free_checkpoint_store"
  c_free_checkpoint_store :: Ptr LogDeviceCheckpointStore -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_checkpoint_store"
  c_free_checkpoint_store_fun :: FunPtr (Ptr LogDeviceCheckpointStore -> IO ())

foreign import ccall safe "hs_logdevice.h checkpoint_store_get_lsn_sync"
  c_checkpoint_store_get_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> Ptr C_LSN    -- ^ value out
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_lsn_sync"
  c_checkpoint_store_update_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8    -- ^ customer_id
    -> C_LogID
    -> C_LSN
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h checkpoint_store_update_multi_lsn_sync"
  c_checkpoint_store_update_multi_lsn_sync_safe
    :: Ptr LogDeviceCheckpointStore
    -> Ptr Word8
    -> Ptr C_LogID
    -> Ptr C_LSN
    -> Word
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h sync_write_checkpoints"
  c_sync_write_checkpoints_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> Ptr C_LogID
    -> Ptr C_LSN
    -> Word
    -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h sync_write_last_read_checkpoints"
  c_sync_write_last_read_checkpoints_safe
    :: Ptr LogDeviceSyncCheckpointedReader
    -> Ptr C_LogID
    -> Word
    -> IO ErrorCode

-------------------------------------------------------------------------------
-- Reader API

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
    :: BA## Word8           -- ^ Reader name
    -> Ptr LogDeviceReader
    -> Ptr LogDeviceCheckpointStore
    -> Word32               -- ^ num of retries
    -> IO (Ptr LogDeviceSyncCheckpointedReader)

foreign import ccall unsafe "hs_logdevice.h free_sync_checkpointed_reader"
  c_free_sync_checkpointed_reader :: Ptr LogDeviceSyncCheckpointedReader -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_sync_checkpointed_reader"
  c_free_sync_checkpointed_reader_fun
    :: FunPtr (Ptr LogDeviceSyncCheckpointedReader -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_reader_start_reading" \
  c_ld_reader_start_reading :: Ptr LogDeviceReader -> C_LogID -> C_LSN -> C_LSN -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_checkpointed_reader_start_reading"
  c_ld_checkpointed_reader_start_reading :: Ptr LogDeviceSyncCheckpointedReader -> C_LogID -> C_LSN -> C_LSN -> IO ErrorCode

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

foreign import ccall safe "hs_logdevice.h logdevice_reader_read"
  c_logdevice_reader_read_safe :: Ptr LogDeviceReader -> CSize -> Ptr DataRecord -> Ptr Int -> IO ErrorCode
foreign import ccall safe "hs_logdevice.h logdevice_checkpointed_reader_read"
  c_logdevice_checkpointed_reader_read_safe :: Ptr LogDeviceSyncCheckpointedReader -> CSize -> Ptr DataRecord -> Ptr Int -> IO ErrorCode

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_logdevice.h new_logdevice_admin_async_client"
  c_new_logdevice_admin_async_client
    :: BA## Word8 -> Word16 -> Bool
    -> Word32
    -> MBA## (Ptr LogDeviceAdminAsyncClient)
    -> IO CInt

foreign import ccall unsafe "hs_logdevice.h free_logdevice_admin_async_client"
  c_free_logdevice_admin_async_client :: Ptr LogDeviceAdminAsyncClient
                                      -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_logdevice_admin_async_client"
  c_free_logdevice_admin_async_client_fun
    :: FunPtr (Ptr LogDeviceAdminAsyncClient -> IO ())

foreign import ccall unsafe "hs_logdevice.h new_thrift_rpc_options"
  c_new_thrift_rpc_options :: Int64 -> IO (Ptr ThriftRpcOptions)

foreign import ccall unsafe "hs_logdevice.h free_thrift_rpc_options"
  c_free_thrift_rpc_options :: Ptr ThriftRpcOptions -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_thrift_rpc_options"
  c_free_thrift_rpc_options_fun :: FunPtr (Ptr ThriftRpcOptions -> IO ())

foreign import ccall safe "hs_logdevice.h ld_admin_sync_getVersion"
  ld_admin_sync_getVersion :: Ptr LogDeviceAdminAsyncClient
                           -> Ptr ThriftRpcOptions
                           -> IO (Ptr Z.StdString)

-------------------------------------------------------------------------------
-- Misc

cbool2bool :: CBool -> Bool
cbool2bool = (/= 0)
