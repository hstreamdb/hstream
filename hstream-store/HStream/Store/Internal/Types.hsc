{-# LANGUAGE CApiFFI         #-}
{-# LANGUAGE CPP             #-}
{-# LANGUAGE DeriveAnyClass  #-}
{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Internal.Types where

import           Control.Exception     (bracket, finally)
import           Control.Monad         (forM, when)
import           Data.Int
import           Data.Map.Strict       (Map)
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal.Alloc
import           Foreign.Ptr
import           Foreign.Storable
import           Z.Data.CBytes         (CBytes)
import qualified Z.Data.CBytes         as CBytes
import           Z.Data.Vector         (Bytes)
import qualified Z.Data.Vector         as Vec
import qualified Z.Foreign             as Z

#include "hs_logdevice.h"

-------------------------------------------------------------------------------

type LDClient = ForeignPtr LogDeviceClient
type LDLogGroup = ForeignPtr LogDeviceLogGroup
type LDLogAttrs = ForeignPtr LogDeviceLogAttributes
type LDLogHeadAttrs = ForeignPtr LogDeviceLogHeadAttributes
type LDLogTailAttrs = ForeignPtr LogDeviceLogTailAttributes
type LDVersionedConfigStore = ForeignPtr LogDeviceVersionedConfigStore
type LDDirectory = ForeignPtr LogDeviceLogDirectory
type LDReader = ForeignPtr LogDeviceReader
type LDSyncCkpReader = ForeignPtr LogDeviceSyncCheckpointedReader
type LDCheckpointStore = ForeignPtr LogDeviceCheckpointStore

type C_LogID = Word64
type C_LogRange = (C_LogID, C_LogID)

newtype LogID = LogID Word64
  deriving (Show, Eq, Ord)

instance Bounded LogID where
  minBound = LOGID_MIN
  maxBound = LOGID_MAX

pattern C_LOGID_MIN :: C_LogID
pattern C_LOGID_MIN = 1

pattern LOGID_MIN :: LogID
pattern LOGID_MIN = LogID C_LOGID_MIN

-- | Max valid user data logid value.
--
-- TOPIC_ID_MAX = USER_LOGID_MAX(LOGID_MAX - 1000)
pattern LOGID_MAX :: LogID
pattern LOGID_MAX = LogID (#const C_USER_LOGID_MAX)

-- 0 is not a valid logid.
pattern C_LOGID_MIN_INVALID :: Word64
pattern C_LOGID_MIN_INVALID = (#const C_LOGID_INVALID)

pattern LOGID_MIN_INVALID :: LogID
pattern LOGID_MIN_INVALID = LogID C_LOGID_MIN_INVALID

-- ~0 is not a valid logid.
pattern C_LOGID_MAX_INVALID :: Word64
pattern C_LOGID_MAX_INVALID = (#const C_LOGID_INVALID2)

pattern LOGID_MAX_INVALID :: LogID
pattern LOGID_MAX_INVALID = LogID C_LOGID_MAX_INVALID

c_logid_max :: Word64
c_logid_max = (#const C_LOGID_MAX)

c_user_logid_max :: Word64
c_user_logid_max = (#const C_USER_LOGID_MAX)

c_logid_max_bits :: CSize
c_logid_max_bits = (#const C_LOGID_MAX_BITS)

-- | Log Sequence Number
type LSN = Word64

-- | Guaranteed to be less than or equal to the smallest valid LSN possible.
-- Use this to seek to the oldest record in a log.
pattern LSN_MIN :: LSN
pattern LSN_MIN = (#const C_LSN_OLDEST)

-- | Greatest valid LSN possible plus one.
pattern LSN_MAX :: LSN
pattern LSN_MAX = (#const C_LSN_MAX)

-- | 0 is not a valid LSN.
pattern LSN_INVALID :: LSN
pattern LSN_INVALID = (#const C_LSN_INVALID)

data RecordByteOffset
  = RecordByteOffset Word64
  | RecordByteOffsetInvalid
  deriving (Show, Eq)

pattern C_BYTE_OFFSET_INVALID :: Word64
pattern C_BYTE_OFFSET_INVALID = (#const facebook::logdevice::BYTE_OFFSET_INVALID)

type C_LogsConfigVersion = Word64

data HsLogAttrs = HsLogAttrs
  { logReplicationFactor :: Int
  , logExtraAttrs        :: Map CBytes CBytes
  } deriving (Show)

data LogAttrs = LogAttrs HsLogAttrs | LogAttrsPtr LDLogAttrs

data VcsValueCallbackData = VcsValueCallbackData
  { vcsValCallbackSt  :: !ErrorCode
  , vcsValCallbackVal :: !Bytes
  }

vcsValueCallbackDataSize :: Int
vcsValueCallbackDataSize = (#size vcs_value_callback_data_t)

peekVcsValueCallbackData :: Ptr VcsValueCallbackData -> IO VcsValueCallbackData
peekVcsValueCallbackData ptr = bracket getSt release peekData
  where
    getSt = (#peek vcs_value_callback_data_t, st) ptr
    peekData st = do
      value <- if st == C_OK
                  then do len <- (#peek vcs_value_callback_data_t, val_len) ptr
                          flip Z.fromPtr len =<< (#peek vcs_value_callback_data_t, value) ptr
                  else return Vec.empty
      return $ VcsValueCallbackData st value
    release st = when (st == C_OK) $
      free =<< (#peek vcs_value_callback_data_t, value) ptr

data VcsWriteCallbackData = VcsWriteCallbackData
  { vcsWriteCallbackSt      :: !ErrorCode
  , vcsWriteCallbackVersion :: !VcsConfigVersion
  , vcsWriteCallbackValue   :: !Bytes
  }

vcsWriteCallbackDataSize :: Int
vcsWriteCallbackDataSize = (#size vcs_write_callback_data_t)

peekVcsWriteCallbackData :: Ptr VcsWriteCallbackData -> IO VcsWriteCallbackData
peekVcsWriteCallbackData ptr = bracket getSt release peekData
  where
    getSt = (#peek vcs_value_callback_data_t, st) ptr
    peekData st = do
      if (cond st)
         then do version <- (#peek vcs_write_callback_data_t, version) ptr
                 len :: Int <- (#peek vcs_write_callback_data_t, val_len) ptr
                 value <- flip Z.fromPtr len =<< (#peek vcs_write_callback_data_t, value) ptr
                 return $ VcsWriteCallbackData st version value
         else return $ VcsWriteCallbackData st C_EMPTY_VERSION Vec.empty
    release st = when (cond st) $
      free =<< (#peek vcs_write_callback_data_t, value) ptr
    cond st = st == C_OK || st == C_VERSION_MISMATCH

-------------------------------------------------------------------------------

newtype StreamAdminClient = StreamAdminClient
  { unStreamAdminClient :: ForeignPtr LogDeviceAdminAsyncClient }

newtype RpcOptions = RpcOptions
  { unRpcOptions :: ForeignPtr ThriftRpcOptions }

newtype StreamSyncCheckpointedReader = StreamSyncCheckpointedReader
  { unStreamSyncCheckpointedReader :: ForeignPtr LogDeviceSyncCheckpointedReader }
  deriving (Show, Eq)

newtype CheckpointStore = CheckpointStore
  { unCheckpointStore :: ForeignPtr LogDeviceCheckpointStore }
  deriving (Show, Eq)

data DataRecord = DataRecord
  { recordLogID       :: {-# UNPACK #-} !C_LogID
  , recordLSN         :: {-# UNPACK #-} !LSN
  , recordTimestamp   :: {-# UNPACK #-} !C_Timestamp
  , recordBatchOffset :: {-# UNPACK #-} !Int
  , recordPayload     :: !Bytes
  , recordByteOffset  :: !RecordByteOffset
  -- ^ Contains information on the amount of data written to the log
  -- (to which this record belongs) up to this record.
  -- BYTE_OFFSET will be invalid if this attribute was not requested by client
  -- (see includeByteOffset() reader option) or if it is not available to
  -- storage nodes.
  } deriving (Show, Eq)

dataRecordSize :: Int
dataRecordSize = (#size logdevice_data_record_t)

peekDataRecords :: Int -> Ptr DataRecord -> IO [DataRecord]
peekDataRecords len ptr = forM [0..len-1] (peekDataRecord ptr)

-- | Peek data record from a pointer and an offset, then release the payload
-- ignoring exceptions.
peekDataRecord :: Ptr DataRecord -> Int -> IO DataRecord
peekDataRecord ptr offset = finally peekData release
  where
    ptr' = ptr `plusPtr` (offset * dataRecordSize)
    peekData = do
      logid <- (#peek logdevice_data_record_t, logid) ptr'
      lsn <- (#peek logdevice_data_record_t, lsn) ptr'
      timestamp <- (#peek logdevice_data_record_t, timestamp) ptr'
      batchOffset <- (#peek logdevice_data_record_t, batch_offset) ptr'
      len <- (#peek logdevice_data_record_t, payload_len) ptr'
      payload <- flip Z.fromPtr len =<< (#peek logdevice_data_record_t, payload) ptr'
      byteOffset' <- (#peek logdevice_data_record_t, byte_offset) ptr'
      let byteOffset = case byteOffset' of
                         C_BYTE_OFFSET_INVALID -> RecordByteOffsetInvalid
                         x -> RecordByteOffset x
      return $ DataRecord logid lsn timestamp batchOffset payload byteOffset
    release = do
      payload_ptr <- (#peek logdevice_data_record_t, payload) ptr'
      free payload_ptr

data AppendCallBackData = AppendCallBackData
  { appendCbRetCode   :: !ErrorCode
  , appendCbLogID     :: !C_LogID
  , appendCbLSN       :: !LSN
  , appendCbTimestamp :: !C_Timestamp
  } deriving (Show)

appendCallBackDataSize :: Int
appendCallBackDataSize = (#size logdevice_append_cb_data_t)

peekAppendCallBackData :: Ptr AppendCallBackData -> IO AppendCallBackData
peekAppendCallBackData ptr = do
  retcode <- (#peek logdevice_append_cb_data_t, st) ptr
  logid <- (#peek logdevice_append_cb_data_t, logid) ptr
  lsn <- (#peek logdevice_append_cb_data_t, lsn) ptr
  ts <- (#peek logdevice_append_cb_data_t, timestamp) ptr
  return $ AppendCallBackData retcode logid lsn ts

data LogsConfigStatusCbData = LogsConfigStatusCbData
  { logsConfigCbRetCode  :: !ErrorCode
  , logsConfigCbVersion  :: !Word64
  , logsConfigCbFailInfo :: !CBytes
  }

logsConfigStatusCbDataSize :: Int
logsConfigStatusCbDataSize = (#size logsconfig_status_cb_data_t)

peekLogsConfigStatusCbData :: Ptr LogsConfigStatusCbData -> IO LogsConfigStatusCbData
peekLogsConfigStatusCbData ptr = do
  retcode <- (#peek logsconfig_status_cb_data_t, st) ptr
  version <- (#peek logsconfig_status_cb_data_t, version) ptr
  failinfo_ptr <- (#peek logsconfig_status_cb_data_t, failure_reason) ptr
  failinfo <- CBytes.fromCString failinfo_ptr
  free failinfo_ptr
  return $ LogsConfigStatusCbData retcode version failinfo

data MakeLogGroupCbData = MakeLogGroupCbData
   { makeLogGroupCbRetCode :: !ErrorCode
   , makeLogGroupCbGrpPtr  :: !(Ptr LogDeviceLogGroup)
   , makeLogGroupFailInfo  :: !CBytes
   }

makeLogGroupCbDataSize :: Int
makeLogGroupCbDataSize = (#size make_loggroup_cb_data_t)

peekMakeLogGroupCbData :: Ptr MakeLogGroupCbData -> IO MakeLogGroupCbData
peekMakeLogGroupCbData ptr = finally peekData release
  where
    peekData = do
      retcode <- (#peek make_loggroup_cb_data_t, st) ptr
      loggroup_ptr <- (#peek make_loggroup_cb_data_t, loggroup) ptr
      failinfo_ptr <- (#peek make_loggroup_cb_data_t, failure_reason) ptr
      failinfo <- CBytes.fromCString failinfo_ptr
      return $ MakeLogGroupCbData retcode loggroup_ptr failinfo
    release = free =<< (#peek make_loggroup_cb_data_t, failure_reason) ptr

data MakeDirectoryCbData = MakeDirectoryCbData
  { makeDirectoryCbRetCode :: !ErrorCode
  , makeDirectoryCbDirPtr :: !(Ptr LogDeviceLogDirectory)
  , makeDirectoryFailInfo :: !CBytes
  }

makeDirectoryCbDataSize :: Int
makeDirectoryCbDataSize = (#size make_directory_cb_data_t)

peekMakeDirectoryCbData :: Ptr MakeDirectoryCbData
                        -> IO MakeDirectoryCbData
peekMakeDirectoryCbData ptr = do
  retcode <- (#peek make_directory_cb_data_t, st) ptr
  directory_ptr <- (#peek make_directory_cb_data_t, directory) ptr
  failinfo_ptr <- (#peek make_directory_cb_data_t, failure_reason) ptr
  failinfo <- CBytes.fromCString failinfo_ptr
  free failinfo_ptr
  return $ MakeDirectoryCbData retcode directory_ptr failinfo

data LogHeadAttrsCbData = LogHeadAttrsCbData
  { logHeadAttributesCbRetCode :: !ErrorCode
  , logHeadAttributesCbAttrPtr :: !(Ptr LogDeviceLogHeadAttributes)
  }

logHeadAttrsCbDataSize :: Int
logHeadAttrsCbDataSize = (#size log_head_attributes_cb_data_t)

peekLogHeadAttrsCbData :: Ptr LogHeadAttrsCbData -> IO LogHeadAttrsCbData
peekLogHeadAttrsCbData ptr = do
  retcode <- (#peek log_head_attributes_cb_data_t, st) ptr
  head_attributes_ptr <- (#peek log_head_attributes_cb_data_t, head_attributes) ptr
  return $ LogHeadAttrsCbData retcode head_attributes_ptr

data LogTailAttrsCbData = LogTailAttrsCbData
  { logTailAttributesCbRetCode :: !ErrorCode
  , logTailAttributesCbAttrPtr :: !(Ptr LogDeviceLogTailAttributes)
  }

logTailAttrsCbDataSize :: Int
logTailAttrsCbDataSize = (#size log_tail_attributes_cb_data_t)

peekLogTailAttrsCbData :: Ptr LogTailAttrsCbData -> IO LogTailAttrsCbData
peekLogTailAttrsCbData ptr = do
  retcode <- (#peek log_tail_attributes_cb_data_t, st) ptr
  tail_attributes_ptr <- (#peek log_tail_attributes_cb_data_t, tail_attributes) ptr
  return $ LogTailAttrsCbData retcode tail_attributes_ptr

data IsLogEmptyCbData = IsLogEmptyCbData
  { isLogEmptyCbRetCode :: !ErrorCode
  , isLogEmptyCbAttrPtr :: !CBool
  }

isLogEmptyCbDataSize :: Int
isLogEmptyCbDataSize = (#size is_log_empty_cb_data_t)

peekIsLogEmptyCbData :: Ptr IsLogEmptyCbData -> IO IsLogEmptyCbData
peekIsLogEmptyCbData ptr = do
  retcode <- (#peek is_log_empty_cb_data_t, st) ptr
  empty <- (#peek is_log_empty_cb_data_t, empty) ptr
  return $ IsLogEmptyCbData retcode empty

-------------------------------------------------------------------------------

data LogDeviceClient
data LogDeviceReader
data LogDeviceSyncCheckpointedReader
data LogDeviceVersionedConfigStore
data LogDeviceLogGroup
data LogDeviceLogDirectory
data LogDeviceLogAttributes
data LogDeviceLogHeadAttributes
data LogDeviceLogTailAttributes
data LogDeviceCheckpointStore
data LogDeviceAdminAsyncClient
data ThriftRpcOptions

type C_Timestamp = Int64

pattern C_MAX_MILLISECONDS :: C_Timestamp
pattern C_MAX_MILLISECONDS = (#const MAX_MILLISECONDS)

newtype KeyType = KeyType Word8
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

c_keytype_findkey :: Word8
c_keytype_findkey = (#const C_KeyType_FINDKEY)

c_keytype_filterable :: Word8
c_keytype_filterable = (#const C_KeyType_FILTERABLE)

pattern KeyTypeUndefined :: KeyType
pattern KeyTypeUndefined = KeyType (#const static_cast<HsInt>(KeyType::UNDEFINED))

-- (compression_enum, zstd_level)
type C_Compression = (Int, Int)

data Compression
  = CompressionNone
  | CompressionZSTD Int
  | CompressionLZ4
  | CompressionLZ4HC
  deriving (Eq, Ord, Show)

fromCompression :: Compression -> C_Compression
fromCompression CompressionNone = ((#const static_cast<HsInt>(Compression::NONE)), 0)
fromCompression (CompressionZSTD lvl) = ((#const static_cast<HsWord8>(Compression::ZSTD)), lvl)
fromCompression CompressionLZ4 = ((#const static_cast<HsInt>(Compression::LZ4)), 0)
fromCompression CompressionLZ4HC = ((#const static_cast<HsInt>(Compression::LZ4_HC)), 0)

-------------------------------------------------------------------------------

type VcsConfigVersion = Word64

pattern C_EMPTY_VERSION :: VcsConfigVersion
pattern C_EMPTY_VERSION = 0

-- | Error
type ErrorCode = Word16

foreign import ccall unsafe "hs_logdevice.h show_error_name"
  c_show_error_name :: ErrorCode -> CString

foreign import ccall unsafe "hs_logdevice.h show_error_description"
  c_show_error_description :: ErrorCode -> CString

pattern
    C_OK
  , C_NOTFOUND
  , C_TIMEDOUT
  , C_NOSEQUENCER
  , C_CONNFAILED
  , C_NOTCONN
  , C_TOOBIG
  , C_TOOMANY
  , C_PREEMPTED
  , C_NOBUFS
  , C_NOMEM
  , C_INTERNAL
  , C_SYSLIMIT
  , C_TEMPLIMIT
  , C_PERMLIMIT
  , C_ACCESS
  , C_ALREADY
  , C_ISCONN
  , C_UNREACHABLE
  , C_UNROUTABLE
  , C_BADMSG
  , C_DISABLED
  , C_EXISTS
  , C_SHUTDOWN
  , C_NOTINCONFIG
  , C_PROTONOSUPPORT
  , C_PROTO
  , C_PEER_CLOSED
  , C_SEQNOBUFS
  , C_WOULDBLOCK
  , C_ABORTED
  , C_INPROGRESS
  , C_CANCELLED
  , C_NOTSTORAGE
  , C_AGAIN
  , C_PARTIAL
  , C_GAP
  , C_TRUNCATED
  , C_STALE
  , C_NOSPC
  , C_OVERLOADED
  , C_PENDING
  , C_PENDING_FULL
  , C_FAILED
  , C_SEQSYSLIMIT
  , C_REBUILDING
  , C_REDIRECTED
  , C_RETRY
  , C_BADPAYLOAD
  , C_NOSSLCONFIG
  , C_NOTREADY
  , C_DROPPED
  , C_FORWARD
  , C_NOTSUPPORTED
  , C_NOTINSERVERCONFIG
  , C_ISOLATED
  , C_SSLREQUIRED
  , C_CBREGISTERED
  , C_LOW_ON_SPC
  , C_PEER_UNAVAILABLE
  , C_NOTSUPPORTEDLOG
  , C_DATALOSS
  , C_NEVER_CONNECTED
  , C_NOTANODE
  , C_IDLE
  , C_INVALID_PARAM
  , C_INVALID_CLUSTER
  , C_INVALID_CONFIG
  , C_INVALID_THREAD
  , C_INVALID_IP
  , C_INVALID_OPERATION
  , C_UNKNOWN_SETTING
  , C_INVALID_SETTING_VALUE
  , C_UPTODATE
  , C_EMPTY
  , C_DESTINATION_MISMATCH
  , C_INVALID_ATTRIBUTES
  , C_NOTEMPTY
  , C_NOTDIR
  , C_ID_CLASH
  , C_LOGS_SECTION_MISSING
  , C_CHECKSUM_MISMATCH
  , C_COND_WRITE_NOT_READY
  , C_COND_WRITE_FAILED
  , C_FILE_OPEN
  , C_FILE_READ
  , C_LOCAL_LOG_STORE_WRITE
  , C_CAUGHT_UP
  , C_UNTIL_LSN_REACHED
  , C_WINDOW_END_REACHED
  , C_BYTE_LIMIT_REACHED
  , C_MALFORMED_RECORD
  , C_LOCAL_LOG_STORE_READ
  , C_SHADOW_DISABLED
  , C_SHADOW_UNCONFIGURED
  , C_SHADOW_FAILED
  , C_SHADOW_BUSY
  , C_SHADOW_LOADING
  , C_SHADOW_SKIP
  , C_VERSION_MISMATCH
  , C_SOURCE_STATE_MISMATCH
  , C_CONDITION_MISMATCH
  , C_MAINTENANCE_CLASH
  , C_WRITE_STREAM_UNKNOWN
  , C_WRITE_STREAM_BROKEN
  , C_WRITE_STREAM_IGNORED :: ErrorCode
pattern C_OK                    =   0
pattern C_NOTFOUND              =   1
pattern C_TIMEDOUT              =   2
pattern C_NOSEQUENCER           =   3
pattern C_CONNFAILED            =   4
pattern C_NOTCONN               =   5
pattern C_TOOBIG                =   6
pattern C_TOOMANY               =   7
pattern C_PREEMPTED             =   8
pattern C_NOBUFS                =   9
pattern C_NOMEM                 =  10
pattern C_INTERNAL              =  11
pattern C_SYSLIMIT              =  12
pattern C_TEMPLIMIT             =  13
pattern C_PERMLIMIT             =  14
pattern C_ACCESS                =  15
pattern C_ALREADY               =  16
pattern C_ISCONN                =  17
pattern C_UNREACHABLE           =  18
pattern C_UNROUTABLE            =  19
pattern C_BADMSG                =  20
pattern C_DISABLED              =  21
pattern C_EXISTS                =  22
pattern C_SHUTDOWN              =  23
pattern C_NOTINCONFIG           =  24
pattern C_PROTONOSUPPORT        =  25
pattern C_PROTO                 =  26
pattern C_PEER_CLOSED           =  27
pattern C_SEQNOBUFS             =  28
pattern C_WOULDBLOCK            =  29
pattern C_ABORTED               =  30
pattern C_INPROGRESS            =  31
pattern C_CANCELLED             =  32
pattern C_NOTSTORAGE            =  33
pattern C_AGAIN                 =  34
pattern C_PARTIAL               =  35
pattern C_GAP                   =  36
pattern C_TRUNCATED             =  37
pattern C_STALE                 =  38
pattern C_NOSPC                 =  39
pattern C_OVERLOADED            =  40
pattern C_PENDING               =  41
pattern C_PENDING_FULL          =  42
pattern C_FAILED                =  43
pattern C_SEQSYSLIMIT           =  44
pattern C_REBUILDING            =  45
pattern C_REDIRECTED            =  46
pattern C_RETRY                 =  47
pattern C_BADPAYLOAD            =  48
pattern C_NOSSLCONFIG           =  49
pattern C_NOTREADY              =  50
pattern C_DROPPED               =  51
pattern C_FORWARD               =  52
pattern C_NOTSUPPORTED          =  53
pattern C_NOTINSERVERCONFIG     =  54
pattern C_ISOLATED              =  55
pattern C_SSLREQUIRED           =  56
pattern C_CBREGISTERED          =  57
pattern C_LOW_ON_SPC            =  58
pattern C_PEER_UNAVAILABLE      =  59
pattern C_NOTSUPPORTEDLOG       =  60
pattern C_DATALOSS              =  61
pattern C_NEVER_CONNECTED       =  62
pattern C_NOTANODE              =  63
pattern C_IDLE                  =  64
pattern C_INVALID_PARAM         = 100
pattern C_INVALID_CLUSTER       = 101
pattern C_INVALID_CONFIG        = 102
pattern C_INVALID_THREAD        = 103
pattern C_INVALID_IP            = 104
pattern C_INVALID_OPERATION     = 105
pattern C_UNKNOWN_SETTING       = 106
pattern C_INVALID_SETTING_VALUE = 107
pattern C_UPTODATE              = 108
pattern C_EMPTY                 = 109
pattern C_DESTINATION_MISMATCH  = 110
pattern C_INVALID_ATTRIBUTES    = 111
pattern C_NOTEMPTY              = 112
pattern C_NOTDIR                = 113
pattern C_ID_CLASH              = 114
pattern C_LOGS_SECTION_MISSING  = 115
pattern C_CHECKSUM_MISMATCH     = 116
pattern C_COND_WRITE_NOT_READY  = 117
pattern C_COND_WRITE_FAILED     = 118
pattern C_FILE_OPEN             = 200
pattern C_FILE_READ             = 201
pattern C_LOCAL_LOG_STORE_WRITE = 300
pattern C_CAUGHT_UP             = 301
pattern C_UNTIL_LSN_REACHED     = 302
pattern C_WINDOW_END_REACHED    = 303
pattern C_BYTE_LIMIT_REACHED    = 304
pattern C_MALFORMED_RECORD      = 305
pattern C_LOCAL_LOG_STORE_READ  = 306
pattern C_SHADOW_DISABLED       = 401
pattern C_SHADOW_UNCONFIGURED   = 402
pattern C_SHADOW_FAILED         = 403
pattern C_SHADOW_BUSY           = 404
pattern C_SHADOW_LOADING        = 405
pattern C_SHADOW_SKIP           = 406
pattern C_VERSION_MISMATCH      = 500
pattern C_SOURCE_STATE_MISMATCH = 501
pattern C_CONDITION_MISMATCH    = 502
pattern C_MAINTENANCE_CLASH     = 600
pattern C_WRITE_STREAM_UNKNOWN  = 700
pattern C_WRITE_STREAM_BROKEN   = 701
pattern C_WRITE_STREAM_IGNORED  = 702

-------------------------------------------------------------------------------

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

pattern C_ACCURACY_STRICT :: Int
pattern C_ACCURACY_STRICT = (#const static_cast<HsInt>(facebook::logdevice::FindKeyAccuracy::STRICT))

pattern C_ACCURACY_APPROXIMATE :: Int
pattern C_ACCURACY_APPROXIMATE = (#const static_cast<HsInt>(facebook::logdevice::FindKeyAccuracy::APPROXIMATE))

data FindKeyAccuracy
  = FindKeyStrict
  | FindKeyApproximate
  deriving (Show)

unFindKeyAccuracy :: FindKeyAccuracy -> Int
unFindKeyAccuracy FindKeyStrict = C_ACCURACY_STRICT
unFindKeyAccuracy FindKeyApproximate = C_ACCURACY_APPROXIMATE
