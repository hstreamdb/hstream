{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice
  ( newLDClient
  , getTailLSN
  , getTailLSNSync
  , getMaxPayloadSize
  , setClientSetting
  , getClientSetting
  , trim
  , findTime
  , findKey
  , isLogEmpty

  , module HStream.Store.Internal.LogDevice.Checkpoint
  , module HStream.Store.Internal.LogDevice.Configuration
  , module HStream.Store.Internal.LogDevice.LogAttributes
  , module HStream.Store.Internal.LogDevice.LogConfigTypes
  , module HStream.Store.Internal.LogDevice.Reader
  , module HStream.Store.Internal.LogDevice.VersionedConfigStore
  , module HStream.Store.Internal.LogDevice.Writer
  , module HStream.Store.Internal.LogDevice.LDQuery
  ) where

import           Control.Monad
import           Data.Int
import           Data.Primitive
import           Data.Word
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack
import qualified Z.Data.CBytes                                         as CBytes
import           Z.Data.CBytes                                         (CBytes)
import           Z.Data.Vector                                         (Bytes)
import qualified Z.Foreign                                             as Z
import           Z.Foreign                                             (BA#,
                                                                        MBA#)

import qualified HStream.Store.Exception                               as E
import           HStream.Store.Internal.Types

import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.LogDevice.Checkpoint
import           HStream.Store.Internal.LogDevice.Configuration
import           HStream.Store.Internal.LogDevice.LDQuery
import           HStream.Store.Internal.LogDevice.LogAttributes
import           HStream.Store.Internal.LogDevice.LogConfigTypes
import           HStream.Store.Internal.LogDevice.Reader
import           HStream.Store.Internal.LogDevice.VersionedConfigStore
import           HStream.Store.Internal.LogDevice.Writer

-------------------------------------------------------------------------------
-- Client

-- | Create a new client from config url.
newLDClient :: HasCallStack => CBytes -> IO LDClient
newLDClient config = CBytes.withCBytesUnsafe config $ \config' -> do
  (client', _) <- Z.withPrimUnsafe nullPtr $ \client'' ->
    E.throwStreamErrorIfNotOK $ c_new_logdevice_client config' client''
  newForeignPtr c_free_logdevice_client_fun client'

getTailLSNSync :: LDClient -> C_LogID -> IO LSN
getTailLSNSync client logid =
  withForeignPtr client $ flip c_ld_client_get_tail_lsn_sync logid

-- | Return the sequence number that points to the tail of log `logid`. The
-- returned LSN is guaranteed to be higher or equal than the LSN of any record
-- that was successfully acknowledged as appended prior to this call.
--
-- Note that there can be benign gaps in the numbering sequence of a log. As
-- such, it is not guaranteed that a record was assigned the returned
-- sequencer number.
--
-- One can read the full content of a log by creating a reader to read from
-- LSN_OLDEST until the LSN returned by this method. Note that it is not
-- guaranteed that the full content of the log is immediately available for
-- reading.
getTailLSN
  :: HasCallStack
  => LDClient
  -> C_LogID
  -- ^ the ID of the log for which to get the tail LSN
  -> IO LSN
getTailLSN client logid =
  withForeignPtr client $ \client' -> do
    (errno, lsn, _) <- withAsyncPrimUnsafe2' (0 :: ErrorCode) LSN_INVALID
      (c_ld_client_get_tail_lsn client' logid) E.throwSubmitIfNotOK
    void $ E.throwStreamErrorIfNotOK' errno
    return lsn

-- | Returns the maximum permitted payload size for this client.
--
-- The default is 1MB, but this can be increased via changing the
-- max-payload-size setting.
getMaxPayloadSize :: LDClient -> IO Word
getMaxPayloadSize client = withForeignPtr client c_ld_client_get_max_payload_size

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
setClientSetting :: HasCallStack => LDClient -> CBytes -> CBytes -> IO ()
setClientSetting client key val =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe key $ \key' ->
  CBytes.withCBytesUnsafe val $ \val' -> void $
    E.throwStreamErrorIfNotOK $ c_ld_client_set_settings client' key' val'

getClientSetting :: LDClient -> CBytes -> IO Bytes
getClientSetting client key =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe key $ \key' ->
    Z.fromStdString $ c_ld_client_get_settings client' key'

-- | Ask LogDevice cluster to trim the log up to and including the specified
-- LSN. After the operation successfully completes records with LSNs up to
-- 'lsn' are no longer accessible to LogDevice clients.
trim :: LDClient
  -> C_LogID
  -- ^ logid ID of log to trim
  -> LSN
  -- ^ Trim the log up to this LSN (inclusive), should not be larger than
  -- the LSN of the most recent record available to readers
  -> IO ()
trim client logid lsn =
  withForeignPtr client $ \client' -> do
    void $ E.throwStreamErrorIfNotOK' . fst =<<
      withAsyncPrimUnsafe' (0 :: ErrorCode)
                           (c_ld_client_trim client' logid lsn)
                           E.throwSubmitIfNotOK

-- | Looks for the sequence number that the log was at at the given time.  The
--   most common use case is to read all records since that time, by
--   subsequently calling startReading(result_lsn).
--
-- The result lsn can be smaller than biggest lsn which timestamp is <= given
-- timestamp. With accuracy parameter set to APPROXIMATE this error can be
-- several minutes.
-- Note that even in that case startReading(result_lsn) will read
-- all records at the given timestamp or later, but it may also read some
-- earlier records.
--
-- If the given timestamp is earlier than all records in the log, this returns
-- the LSN after the point to which the log was trimmed.
--
-- If the given timestamp is later than all records in the log, this returns
-- the next sequence number to be issued.  Calling startReading(result_lsn)
-- will read newly written records.
--
-- If the log is empty, this returns LSN_OLDEST.
--
-- All of the above assumes that records in the log have increasing
-- timestamps.  If timestamps are not monotonic, the accuracy of this API
-- may be affected.  This may be the case if the sequencer's system clock is
-- changed, or if the sequencer moves and the clocks are not in sync.
--
-- The delivery of a signal does not interrupt the wait.
findTime
  :: HasCallStack
  => LDClient
  -> C_LogID
  -- ^ ID of log to query
  -> Int64
  -- ^ timestamp in milliseconds, select the oldest record in this log whose
  -- timestamp is greater or equal to this.
  -> FindKeyAccuracy
  -- ^ Accuracy option specify how accurate the result of
  -- findTime has to be. It allows to choose best
  -- accuracy-speed trade off for each specific use case.
  -> IO LSN
findTime client logid ts accuracy =
  withForeignPtr client $ \client' -> do
    (errno, lsn, _) <- withAsyncPrimUnsafe2' (0 :: ErrorCode) LSN_INVALID
      (c_ld_client_find_time client' logid ts $ unFindKeyAccuracy accuracy) E.throwSubmitIfNotOK
    void $ E.throwStreamErrorIfNotOK' errno
    return lsn

-- | Looks for the sequence number corresponding to the record with the given
-- key for the log.
--
-- FIXME: it seems that the AppendAttributes is not deleted event if the logid
-- is deleted. For example:
--
-- 1. create a loggroup A with logid 1
-- 2. append a record with AppendAttributes(FindKey, "0") to logid 1
-- 3. append a record with AppendAttributes(FindKey, "1") to logid 1
-- 4. call findKey "1", you will get the right result
-- 5. remove the loggroup A
-- 6. repeat step 1-4, you will get a wrong result
--
-- The result provides two LSNs: the first one, lo, is the highest LSN with
-- key smaller than the given key, the second one, hi, is the lowest LSN with
-- key equal or greater than the given key. With accuracy parameter set to
-- APPROXIMATE, the first LSN can be underestimated and the second LSN can be
-- overestimated by a few minutes, in terms of record timestamps.
--
-- It is assumed that keys within the same log are monotonically
-- non-decreasing (when compared lexicographically). If this is not true, the
-- accuracy of this API may be affected.
--
-- The delivery of a signal does not interrupt the wait.
findKey
  :: HasCallStack
  => LDClient
  -> C_LogID
  -- ^ ID of log to query
  -> CBytes
  -- ^ select the oldest record in this log whose
  -- key is greater or equal to _key_, for upper bound of
  -- result; select the newest record in this log whose key
  -- is smaller than _key_, for lower bound.
  -> FindKeyAccuracy
  -> IO (LSN, LSN)
findKey client logid key accuracy =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe key $ \key' -> do
    (errno, lo_lsn, hi_lsn, _) <- withAsyncPrimUnsafe3' (0 :: ErrorCode) LSN_INVALID LSN_INVALID
      (ld_client_find_key client' logid key' $ unFindKeyAccuracy accuracy) E.throwSubmitIfNotOK
    void $ E.throwStreamErrorIfNotOK' errno
    return (lo_lsn, hi_lsn)

-- | Checks wether a particular log is empty. This method is blocking until the
--   state can be determined or an error occurred.
isLogEmpty
  :: HasCallStack
  => LDClient
  -> C_LogID
  -- ^ the ID of the log to check
  -> IO Bool
isLogEmpty client logid =
  withForeignPtr client $ \client' -> do
    let size = isLogEmptyCbDataSize
        peek_data = peekIsLogEmptyCbData
        cfun = c_ld_client_is_log_empty client' logid
    IsLogEmptyCbData errno empty <- withAsync size peek_data cfun
    void $ E.throwStreamErrorIfNotOK' errno
    return . cbool2bool $ empty

foreign import ccall unsafe "hs_logdevice.h new_logdevice_client"
  c_new_logdevice_client :: Z.BA# Word8
                         -> Z.MBA# (Ptr LogDeviceClient)
                         -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_client"
  c_free_logdevice_client_fun :: FunPtr (Ptr LogDeviceClient -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_get_max_payload_size"
  c_ld_client_get_max_payload_size :: Ptr LogDeviceClient -> IO Word

foreign import ccall unsafe "hs_logdevice.h ld_client_get_settings"
  c_ld_client_get_settings
    :: Ptr LogDeviceClient -> Z.BA# Word8 -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h ld_client_set_settings"
  c_ld_client_set_settings
    :: Ptr LogDeviceClient -> Z.BA# Word8 -> Z.BA# Word8 -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_get_tail_lsn_sync"
  c_ld_client_get_tail_lsn_sync :: Ptr LogDeviceClient -> C_LogID -> IO LSN

foreign import ccall unsafe "hs_logdevice.h ld_client_get_tail_lsn"
  c_ld_client_get_tail_lsn :: Ptr LogDeviceClient
                           -> C_LogID
                           -> StablePtr PrimMVar -> Int
                           -> MBA# ErrorCode
                           -> MBA# LSN
                           -> IO Int

foreign import ccall unsafe "hs_logdevice.h ld_client_is_log_empty"
  c_ld_client_is_log_empty :: Ptr LogDeviceClient
                           -> C_LogID
                           -> StablePtr PrimMVar -> Int
                           -> Ptr IsLogEmptyCbData
                           -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_trim"
  c_ld_client_trim :: Ptr LogDeviceClient
                   -> C_LogID
                   -> LSN
                   -> StablePtr PrimMVar -> Int
                   -> MBA# ErrorCode
                   -> IO Int

foreign import ccall unsafe "hs_logdevice.h ld_client_find_time"
  c_ld_client_find_time :: Ptr LogDeviceClient
                        -> C_LogID
                        -> Int64
                        -> Int
                        -> StablePtr PrimMVar -> Int
                        -> MBA# ErrorCode
                        -> MBA# LSN
                        -> IO Int

foreign import ccall unsafe "hs_logdevice.h ld_client_find_key"
  ld_client_find_key
    :: Ptr LogDeviceClient
    -> C_LogID -> BA# Word8 -> Int
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode -> MBA# LSN -> MBA# LSN
    -> IO Int
