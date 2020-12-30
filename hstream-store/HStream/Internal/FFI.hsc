{-# LANGUAGE CPP                        #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MagicHash                  #-}
{-# LANGUAGE UnliftedFFITypes           #-}

module HStream.Internal.FFI where

import           Control.Exception     (bracket_)
import           Control.Monad         (forM)
import           Data.Int
import           Data.Word
import           Foreign.C.String
import           Foreign.C.Types
import           Foreign.Marshal.Alloc (free)
import           Foreign.Ptr
import           Foreign.Storable
import           Z.Data.Vector         (Bytes)
import           Z.Foreign             (BA##, MBA##)
import qualified Z.Foreign             as Z

#include "hs_logdevice.h"

-------------------------------------------------------------------------------

newtype TopicID = TopicID C_LogID
  deriving (Show, Eq, Ord, Num, Storable)

instance Bounded TopicID where
  minBound = TopicID c_logid_min
  maxBound = TopicID c_logid_max

newtype SequenceNum = SequenceNum C_LSN
  deriving (Show, Eq, Ord, Num, Storable)

instance Bounded SequenceNum where
  minBound = SequenceNum c_lsn_oldest
  maxBound = SequenceNum c_lsn_max

sequenceNumInvalid :: SequenceNum
sequenceNumInvalid = SequenceNum c_lsn_invalid

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
      return $ DataRecord logid lsn payload
    release = do
      payload_ptr <- (#peek logdevice_data_record_t, payload) ptr'
      free payload_ptr

newtype KeyType = KeyType C_KeyType
  deriving (Eq, Ord, Storable)

instance Show KeyType where
  show t
    | t == keyTypeFindKey = "FINDKEY"
    | t == keyTypeFilterable = "FILTERABLE"
    | otherwise = "UNDEFINED"

keyTypeFindKey :: KeyType
keyTypeFindKey = KeyType c_keytype_findkey

keyTypeFilterable :: KeyType
keyTypeFilterable = KeyType c_keytype_filterable

-------------------------------------------------------------------------------

data LogDeviceClient
data LogDeviceReader
data LogDeviceLogGroup
data LogDeviceLogDirectory
data LogDeviceLogAttributes

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
  c_log_attrs_set_replication_factor
    :: Ptr LogDeviceLogAttributes
    -> CInt
    -> IO ()

-- | Error
type ErrorCode = Word16

foreign import ccall unsafe "hs_logdevice.h show_error_name"
  c_show_error_name :: ErrorCode -> CString

foreign import ccall unsafe "hs_logdevice.h show_error_description"
  c_show_error_description :: ErrorCode -> CString

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

foreign import ccall unsafe "hs_logdevice.h ld_client_make_directory_sync"
  c_ld_client_make_directory_sync
    :: Ptr LogDeviceClient
    -> BA## Word8   -- ^ path
    -> Bool
    -> Ptr LogDeviceLogAttributes
    -> MBA## (Ptr LogDeviceLogDirectory)
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h lg_logdirectory_get_name"
  c_ld_logdirectory_get_name :: Ptr LogDeviceLogDirectory -> CString

foreign import ccall unsafe "hs_logdevice.h free_lodevice_logdirectory"
  c_free_lodevice_logdirectory :: Ptr LogDeviceLogDirectory -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_lodevice_logdirectory"
  c_free_lodevice_logdirectory_fun :: FunPtr (Ptr LogDeviceLogDirectory -> IO ())

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

foreign import ccall safe "hs_logdevice.h free_lodevice_loggroup"
  c_free_lodevice_loggroup :: Ptr LogDeviceLogGroup -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_lodevice_loggroup"
  c_free_lodevice_loggroup_fun :: FunPtr (Ptr LogDeviceLogGroup -> IO ())

-------------------------------------------------------------------------------
-- Client Writer API

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
-- Client Reader API

foreign import ccall unsafe "hs_logdevice.h new_logdevice_reader"
  c_new_logdevice_reader :: Ptr LogDeviceClient
                         -> CSize
                         -> Int64
                         -> IO (Ptr LogDeviceReader)
foreign import ccall unsafe "hs_logdevice.h free_logdevice_reader"
  c_free_logdevice_reader :: Ptr LogDeviceReader -> IO ()
foreign import ccall unsafe "hs_logdevice.h &free_logdevice_reader"
  c_free_logdevice_reader_fun :: FunPtr (Ptr LogDeviceReader -> IO ())

foreign import ccall unsafe "hs_logdevice.h logdevice_reader_start_reading"
  c_logdevice_reader_start_reading :: Ptr LogDeviceReader
                                   -> C_LogID
                                   -> C_LSN   -- ^ start
                                   -> C_LSN   -- ^ until
                                   -> IO CInt

foreign import ccall unsafe "hs_logdevice.h logdevice_reader_is_reading"
  c_logdevice_reader_is_reading :: Ptr LogDeviceReader -> C_LogID -> IO CBool

foreign import ccall unsafe "hs_logdevice.h logdevice_reader_is_reading_any"
  c_logdevice_reader_is_reading_any :: Ptr LogDeviceReader -> IO CBool

foreign import ccall unsafe "hs_logdevice.h logdevice_reader_read_sync"
  c_logdevice_reader_read_sync :: Ptr LogDeviceReader
                               -> CSize
                               -> Ptr DataRecord
                               -> MBA## Int
                               -> IO CInt

foreign import ccall safe "hs_logdevice.h logdevice_reader_read_sync"
  c_logdevice_reader_read_sync_safe :: Ptr LogDeviceReader
                                    -> CSize
                                    -> Ptr DataRecord
                                    -> Ptr Int
                                    -> IO CInt

-------------------------------------------------------------------------------
-- Misc

foreign import ccall unsafe "hs_logdevice.h set_dbg_level_error"
  c_set_dbg_level_error :: IO ()

cbool2bool :: CBool -> Bool
cbool2bool = (/= 0)
