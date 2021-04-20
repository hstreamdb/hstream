{-# LANGUAGE CPP            #-}
{-# LANGUAGE DeriveAnyClass #-}

module HStream.Store.Internal.Types where

import           Control.Exception     (bracket_)
import           Control.Monad         (forM)
import           Data.Int
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal.Alloc
import           Foreign.Ptr
import           Foreign.Storable
import           GHC.Generics          (Generic)
import qualified Z.Data.JSON           as JSON
import qualified Z.Data.MessagePack    as MP
import           Z.Data.Vector         (Bytes)
import           Z.Data.CBytes         (CBytes, fromCString)
import qualified Z.Foreign             as Z

#include "hs_logdevice.h"

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
  deriving (Generic)
  deriving newtype (Show, Eq, Ord, Num, JSON.JSON, MP.MessagePack)

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

data LogsconfigStatusCbData = LogsconfigStatusCbData
  { logsConfigCbRetCode :: !ErrorCode
  , logsConfigCbVersion :: !Word64
  , logsConfigCbFailInfo :: !CBytes
  }

logsconfigStatusCbDataSize :: Int
logsconfigStatusCbDataSize = (#size logsconfig_status_cb_data_t)

peekLogsconfigStatusCbData :: Ptr LogsconfigStatusCbData 
                           -> IO LogsconfigStatusCbData
peekLogsconfigStatusCbData ptr = do
  retcode <- (#peek logsconfig_status_cb_data_t, st) ptr
  version <- (#peek logsconfig_status_cb_data_t, version) ptr
  failinfo_ptr <- (#peek logsconfig_status_cb_data_t, failure_reason) ptr
  failinfo <- fromCString failinfo_ptr
  return $ LogsconfigStatusCbData retcode version failinfo
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

newtype FB_STATUS = FB_STATUS { unFB_STATUS :: CInt}
  deriving newtype (Eq, Num)

instance Show FB_STATUS where
  show FB_STATUS_STARTING = "FB_STATUS_STARTING"
  show FB_STATUS_ALIVE    = "FB_STATUS_ALIVE"
  show FB_STATUS_DEAD     = "FB_STATUS_DEAD"
  show FB_STATUS_STOPPING = "FB_STATUS_STOPPING"
  show FB_STATUS_STOPPED  = "FB_STATUS_STOPPED"
  show FB_STATUS_WARNING  = "FB_STATUS_WARNING"
  show _                  = "UNDEFINED_FB_STATUS"

pattern FB_STATUS_STARTING :: FB_STATUS
pattern FB_STATUS_STARTING = (#const static_cast<int>(fb_status::STARTING))

pattern FB_STATUS_ALIVE :: FB_STATUS
pattern FB_STATUS_ALIVE = (#const static_cast<int>(fb_status::ALIVE))

pattern FB_STATUS_DEAD :: FB_STATUS
pattern FB_STATUS_DEAD = (#const static_cast<int>(fb_status::DEAD))

pattern FB_STATUS_STOPPING :: FB_STATUS
pattern FB_STATUS_STOPPING = (#const static_cast<int>(fb_status::STOPPING))

pattern FB_STATUS_STOPPED :: FB_STATUS
pattern FB_STATUS_STOPPED = (#const static_cast<int>(fb_status::STOPPED))

pattern FB_STATUS_WARNING :: FB_STATUS
pattern FB_STATUS_WARNING = (#const static_cast<int>(fb_status::WARNING))
