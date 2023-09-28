{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
-- Notes for ghc-9.2.8:
--
-- We need this ghc option to force ghci use -fobject-code. Or ghci will
-- complain "panic".
--
-- Also, using @{-# LANGUAGE UnboxedTuples #-}@ may possible work for ghc-8.10.
--
-- Relatead ghc issues:
-- * https://gitlab.haskell.org/ghc/ghc/-/issues/19733
-- * https://gitlab.haskell.org/ghc/ghc/-/issues/15454
{-# OPTIONS_GHC -fobject-code #-}

module HStream.Store.Internal.LogDevice.LogConfigTypes where

import           Control.Exception                              (finally)
import           Control.Monad                                  (void, when,
                                                                 (<=<))
import qualified Data.Map.Strict                                as Map
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack                                      (HasCallStack,
                                                                 callStack)
import qualified Z.Data.CBytes                                  as CBytes
import           Z.Data.CBytes                                  (CBytes)
import qualified Z.Foreign                                      as Z

import           HStream.Foreign
import qualified HStream.Store.Exception                        as E
import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.LogDevice.LogAttributes
import           HStream.Store.Internal.Types

-------------------------------------------------------------------------------
-- * LogAttributes

getLogAttrsExtra :: LDLogAttrs -> CBytes -> IO (Maybe CBytes)
getLogAttrsExtra attrs key = withForeignPtr attrs $ \attrs' ->
  CBytes.withCBytesUnsafe key $ \key' -> do
    et <- c_exist_log_attrs_extras attrs' (BA# key')
    if et then Just . CBytes.fromBytes <$> Z.fromStdString (c_get_log_attrs_extra attrs' (BA# key'))
          else return Nothing

updateLogAttrsExtrasPtr
  :: Ptr LogDeviceLogAttributes
  -> Map.Map CBytes CBytes
  -> IO LDLogAttrs
updateLogAttrsExtrasPtr attrs' logExtraAttrs = do
  let extras = Map.toList logExtraAttrs
  let ks = map (CBytes.rawPrimArray . fst) extras
      vs = map (CBytes.rawPrimArray . snd) extras
  Z.withPrimArrayListUnsafe ks $ \ks' l ->
    Z.withPrimArrayListUnsafe vs $ \vs' _ -> do
      i <- c_update_log_attrs_extras attrs' l (BAArray# ks') (BAArray# vs')
      newForeignPtr c_free_log_attributes_fun i

getAttrsExtrasFromPtr :: Ptr LogDeviceLogAttributes -> IO (Map.Map CBytes CBytes)
getAttrsExtrasFromPtr attrs = do
  (len, (keys_ptr, (values_ptr, (keys_vec, (values_vec, _))))) <-
    Z.withPrimUnsafe (0 :: CSize) $ \len ->
    Z.withPrimUnsafe nullPtr $ \keys ->
    Z.withPrimUnsafe nullPtr $ \values ->
    Z.withPrimUnsafe nullPtr $ \keys_vec ->
    Z.withPrimUnsafe nullPtr $ \values_vec ->
      c_get_attribute_extras attrs (MBA# len) (MBA# keys) (MBA# values)
                             (MBA# keys_vec) (MBA# values_vec)
  finally
    (buildExtras (fromIntegral len) keys_ptr values_ptr)
    (c_delete_vector_of_string keys_vec <> c_delete_vector_of_string values_vec)
  where
    buildExtras len keys_ptr values_ptr = do
      keys <- peekStdStringToCBytesN len keys_ptr
      values <- peekStdStringToCBytesN len values_ptr
      return . Map.fromList $ zip keys values

getAttrsReplicationFactorFromPtr :: Ptr LogDeviceLogAttributes -> IO Int
getAttrsReplicationFactorFromPtr attrs = fromIntegral <$> c_get_replication_factor attrs

foreign import ccall unsafe "hs_logdevice.h get_attribute_extras"
  c_get_attribute_extras :: Ptr LogDeviceLogAttributes
                         -> MBA# CSize
                         -> MBA# (Ptr StdString)
                         -> MBA# (Ptr StdString)
                         -> MBA# (Ptr (StdVector StdString))
                         -> MBA# (Ptr (StdVector StdString))
                         -> IO ()

foreign import ccall unsafe "hs_logdevice.h get_replication_factor"
  c_get_replication_factor :: Ptr LogDeviceLogAttributes -> IO CInt

foreign import ccall unsafe "hs_logdevice.h new_log_attributes"
  c_new_log_attributes
    :: CInt
    -> Int -> BAArray# Word8 -> BAArray# Word8
    -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h update_log_attrs_extras"
  c_update_log_attrs_extras
    :: Ptr LogDeviceLogAttributes
    -> Int -> BAArray# Word8 -> BAArray# Word8
    -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h exist_log_attrs_extras"
  c_exist_log_attrs_extras
    :: Ptr LogDeviceLogAttributes
    -> BA# Word8
    -> IO Bool

foreign import ccall unsafe "hs_logdevice.h get_log_attrs_extra"
  c_get_log_attrs_extra
    :: Ptr LogDeviceLogAttributes
    -> BA# Word8
    -> IO (Ptr StdString)

foreign import ccall unsafe "hs_logdevice.h free_log_attributes"
  c_free_log_attributes :: Ptr LogDeviceLogAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_attributes"
  c_free_log_attributes_fun :: FunPtr (Ptr LogDeviceLogAttributes -> IO ())

-------------------------------------------------------------------------------
-- LogHeadAttributes

-- | Return current attributes of the head of the log.
getLogHeadAttrs :: LDClient -> C_LogID -> IO LDLogHeadAttrs
getLogHeadAttrs client logid = withForeignPtr client $ \client' -> do
  let cfun = c_get_head_attributes client' logid
  LogHeadAttrsCbData errno attributes <-
    withAsync logHeadAttrsCbDataSize peekLogHeadAttrsCbData cfun
  void $ E.throwStreamErrorIfNotOK' errno
  newForeignPtr c_free_log_head_attributes_fun attributes

-- | Trim point of the log. Set to LSN_INVALID if log was never trimmed.
getLogHeadAttrsTrimPoint :: LDLogHeadAttrs -> IO LSN
getLogHeadAttrsTrimPoint = flip withForeignPtr c_get_trim_point

-- | Approximate timestamp of the next record after trim point. Return timestamp in millisecond
getLogHeadAttrsTrimPointTimestamp :: LDLogHeadAttrs -> IO C_Timestamp
getLogHeadAttrsTrimPointTimestamp = flip withForeignPtr c_get_trim_point_timestamp

foreign import ccall unsafe "hs_logdevice.h get_head_attributes"
 c_get_head_attributes
   :: Ptr LogDeviceClient
   -> C_LogID
   -> StablePtr PrimMVar -> Int
   -> Ptr LogHeadAttrsCbData
   -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h get_trim_point_timestamp"
 c_get_trim_point_timestamp :: Ptr LogDeviceLogHeadAttributes -> IO C_Timestamp

foreign import ccall unsafe "hs_logdevice.h get_trim_point"
 c_get_trim_point :: Ptr LogDeviceLogHeadAttributes -> IO LSN

foreign import ccall unsafe "hs_logdevice.h free_log_head_attributes"
  c_free_log_head_attributes :: Ptr LogDeviceLogHeadAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_head_attributes"
  c_free_log_head_attributes_fun :: FunPtr (Ptr LogDeviceLogHeadAttributes -> IO ())

-------------------------------------------------------------------------------
-- * LogTailAttributes

getLogTailAttrs :: LDClient -> C_LogID -> IO LDLogTailAttrs
getLogTailAttrs client logid = withForeignPtr client $ \client' -> do
  let cfun = c_ld_client_get_tail_attributes client' logid
  LogTailAttrsCbData errno tailAttributes <-
    withAsync logTailAttrsCbDataSize peekLogTailAttrsCbData cfun
  void $ E.throwStreamErrorIfNotOK' errno
  newForeignPtr c_free_log_tail_attributes_fun tailAttributes

getLogTailAttrsLSN :: LDLogTailAttrs -> IO LSN
getLogTailAttrsLSN tailAttrs = withForeignPtr tailAttrs c_ld_client_get_tail_attributes_lsn

-- | Return  the estimated timestamp of record with last_released_real_lsn sequence number. It may be
-- slightly larger than real timestamp of a record with last_released_real_lsn lsn.
getLogTailAttrsLastTimeStamp :: LDLogTailAttrs -> IO C_Timestamp
getLogTailAttrsLastTimeStamp tailAttrs = withForeignPtr tailAttrs c_ld_client_get_tail_attributes_last_timestamp

getLogTailAttrsBytesOffset :: LDLogTailAttrs -> IO Word64
getLogTailAttrsBytesOffset tailAttrs = withForeignPtr tailAttrs c_ld_client_get_tail_attributes_bytes_offset

foreign import ccall unsafe "hs_logdevice.h ld_client_get_tail_attributes"
  c_ld_client_get_tail_attributes
    :: Ptr LogDeviceClient
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> Ptr LogTailAttrsCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_get_tail_attributes_lsn"
  c_ld_client_get_tail_attributes_lsn :: Ptr LogDeviceLogTailAttributes -> IO LSN

foreign import ccall unsafe "hs_logdevice.h ld_client_get_tail_attributes_last_timestamp"
 c_ld_client_get_tail_attributes_last_timestamp :: Ptr LogDeviceLogTailAttributes -> IO C_Timestamp

foreign import ccall unsafe "hs_logdevice.h ld_client_get_tail_attributes_bytes_offset"
 c_ld_client_get_tail_attributes_bytes_offset :: Ptr LogDeviceLogTailAttributes -> IO Word64

foreign import ccall unsafe "hs_logdevice.h free_logdevice_tail_attributes"
  c_free_log_tail_attributes :: Ptr LogDeviceLogTailAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_tail_attributes"
  c_free_log_tail_attributes_fun :: FunPtr (Ptr LogDeviceLogTailAttributes -> IO ())

-------------------------------------------------------------------------------
-- * Directory

getLogDirectory :: HasCallStack => LDClient -> CBytes -> IO LDDirectory
getLogDirectory client path =
  CBytes.withCBytesUnsafe path $ \path' ->
    withForeignPtr client $ \client' -> do
      let cfun = c_ld_client_get_directory client' (BA# path')
      (errno, dir, _) <- withAsyncPrimUnsafe2' (0 :: ErrorCode)
          nullPtr cfun (E.throwSubmitIfNotOK . fromIntegral)
      _ <- E.throwStreamErrorIfNotOK' errno
      newForeignPtr c_free_logdevice_logdirectory_fun dir

logDirectoryGetName :: LDDirectory -> IO CBytes
logDirectoryGetName dir = withForeignPtr dir $
   CBytes.fromCString <=< c_ld_logdirectory_name

logDirectoryGetFullName :: LDDirectory -> IO CBytes
logDirectoryGetFullName dir = withForeignPtr dir $
   CBytes.fromCString <=< c_ld_logdirectory_full_name

logDirectoryGetLogsName :: Bool -> LDDirectory -> IO [CBytes]
logDirectoryGetLogsName recursive dir = withForeignPtr dir $ \dir' -> do
  (len, (names_ptr, stdvec_ptr)) <-
    Z.withPrimUnsafe 0 $ \len' ->
    Z.withPrimUnsafe nullPtr $ \names' ->
    fst <$> Z.withPrimUnsafe nullPtr (\p ->
              c_ld_logdirectory_get_logs_name dir' recursive (MBA# len') (MBA# names') (MBA# p))
  finally (peekStdStringToCBytesN len names_ptr) (c_delete_vector_of_string stdvec_ptr)

logDirectoryGetVersion :: LDDirectory -> IO C_LogsConfigVersion
logDirectoryGetVersion dir = withForeignPtr dir c_ld_logdirectory_get_version

makeLogDirectory
  :: LDClient
  -> CBytes
  -> LogAttributes
  -> Bool
  -> IO LDDirectory
makeLogDirectory client path attrs mkParent = do
  logAttrs <- pokeLogAttributes attrs
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        let cfun = c_ld_client_make_directory client' (BA# path') mkParent attrs'
        MakeDirectoryCbData errno directory _ <-
          withAsync makeDirectoryCbDataSize peekMakeDirectoryCbData cfun
        void $ E.throwStreamErrorIfNotOK' errno
        newForeignPtr c_free_logdevice_logdirectory_fun directory

removeLogDirectory :: LDClient -> CBytes -> Bool -> IO C_LogsConfigVersion
removeLogDirectory client path recursive =
  CBytes.withCBytesUnsafe path $ \path' ->
    withForeignPtr client $ \client' -> do
      let size = logsConfigStatusCbDataSize
          peek_data = peekLogsConfigStatusCbData
          cfun = c_ld_client_remove_directory client' (BA# path') recursive
      LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
      void $ E.throwStreamErrorIfNotOK' errno
      return version

logDirChildrenNames :: LDDirectory -> IO [CBytes]
logDirChildrenNames dir = withForeignPtr dir $ \dir' -> do
  (len, (raw_ptr, names_ptr)) <-
    Z.withPrimUnsafe @Int 0 $ \len' ->
    Z.withPrimUnsafe nullPtr $ \names' ->
      c_ld_logdir_children_keys dir' (MBA# len') (MBA# names')
  finally (peekStdStringToCBytesN len names_ptr) (c_delete_vector_of_string raw_ptr)

logDirLogsNames :: LDDirectory -> IO [CBytes]
logDirLogsNames dir = withForeignPtr dir $ \dir' -> do
  (len, (raw_ptr, names_ptr)) <-
    Z.withPrimUnsafe @Int 0 $ \len' ->
    Z.withPrimUnsafe nullPtr $ \names' ->
      c_ld_logdir_logs_keys dir' (MBA# len') (MBA# names')
  finally (peekStdStringToCBytesN len names_ptr) (c_delete_vector_of_string raw_ptr)

logDirChildFullName :: LDDirectory -> CBytes -> IO CBytes
logDirChildFullName dir name =
  withForeignPtr dir $ \dir' ->
    CBytes.withCBytesUnsafe name $ \name' ->
      CBytes.fromCString =<< c_ld_logdir_child_full_name dir' (BA# name')

logDirLogFullName :: LDDirectory -> CBytes -> IO CBytes
logDirLogFullName dir name =
  withForeignPtr dir $ \dir' ->
    CBytes.withCBytesUnsafe name $ \name' ->
      CBytes.fromCString =<< c_ld_logdir_log_full_name dir' (BA# name')

-- Note that this pointer is only valid if LogDirectory is valid.
logDirectoryGetAttrsPtr :: LDDirectory -> IO (Ptr LogDeviceLogAttributes)
logDirectoryGetAttrsPtr dir = withForeignPtr dir c_ld_logdirectory_get_attrs

logDirectoryGetAttrs :: LDDirectory -> IO LogAttributes
logDirectoryGetAttrs dir =
  withForeignPtr dir $ peekLogAttributes <=< c_ld_logdirectory_get_attrs

foreign import ccall unsafe "hs_logdevice.h ld_logdir_child_full_name"
  c_ld_logdir_child_full_name
    :: Ptr LogDeviceLogDirectory
    -> BA# Word8
    -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdir_log_full_name"
  c_ld_logdir_log_full_name
    :: Ptr LogDeviceLogDirectory
    -> BA# Word8
    -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdir_children_keys"
  c_ld_logdir_children_keys
    :: Ptr LogDeviceLogDirectory
    -> MBA# Int -> MBA# (Ptr (StdVector StdString))
    -> IO (Ptr StdString)

foreign import ccall unsafe "hs_logdevice.h ld_logdir_logs_keys"
  c_ld_logdir_logs_keys
    :: Ptr LogDeviceLogDirectory
    -> MBA# Int -> MBA# (Ptr (StdVector StdString))
    -> IO (Ptr StdString)

foreign import ccall unsafe "hs_logdevice.h ld_client_make_directory_sync"
  c_ld_client_make_directory_sync
    :: Ptr LogDeviceClient
    -> BA# Word8   -- ^ path
    -> Bool
    -> Ptr LogDeviceLogAttributes
    -> MBA# (Ptr LogDeviceLogDirectory)
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_make_directory"
  c_ld_client_make_directory
    :: Ptr LogDeviceClient
    -> BA# Word8   -- ^ path
    -> Bool
    -> Ptr LogDeviceLogAttributes
    -> StablePtr PrimMVar -> Int
    -> Ptr MakeDirectoryCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h free_logdevice_logdirectory"
  c_free_logdevice_logdirectory :: Ptr LogDeviceLogDirectory -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_logdirectory"
  c_free_logdevice_logdirectory_fun :: FunPtr (Ptr LogDeviceLogDirectory -> IO ())

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_directory"
  c_ld_client_remove_directory
    :: Ptr LogDeviceClient
    -> BA# Word8   -- ^ path
    -> Bool
    -> StablePtr PrimMVar -> Int
    -> Ptr LogsConfigStatusCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_name"
  c_ld_logdirectory_name :: Ptr LogDeviceLogDirectory -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_full_name"
  c_ld_logdirectory_full_name :: Ptr LogDeviceLogDirectory -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_version"
  c_ld_logdirectory_get_version :: Ptr LogDeviceLogDirectory -> IO Word64

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_attrs"
  c_ld_logdirectory_get_attrs :: Ptr LogDeviceLogDirectory -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h ld_client_get_directory"
  c_ld_client_get_directory :: Ptr LogDeviceClient
                            -> BA# Word8
                            -> StablePtr PrimMVar -> Int
                            -> MBA# ErrorCode
                            -> MBA# (Ptr LogDeviceLogDirectory)
                            -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_logs_name"
  c_ld_logdirectory_get_logs_name
    :: Ptr LogDeviceLogDirectory
    -> Bool     -- ^ recursive
    -> MBA# CSize -> MBA# (Ptr StdString)
    -> MBA# (Ptr (StdVector StdString))
    -> IO ()

-------------------------------------------------------------------------------
-- * LogGroup

-- | Creates a log group under a specific directory path.
--
-- Note that, even after this method returns success, it may take some time
-- for the update to propagate to all servers, so the new log group may not
-- be usable for a few seconds (appends may fail with NOTFOUND or
-- NOTINSERVERCONFIG). Same applies to all other logs config update methods,
-- e.g. setAttributes().
makeLogGroupSync
  :: HasCallStack
  => LDClient
  -> CBytes
  -> C_LogID
  -> C_LogID
  -> LogAttributes
  -> Bool
  -> IO LDLogGroup
makeLogGroupSync client path start end attrs mkParent = do
  logAttrs <- pokeLogAttributes attrs
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
          void $ E.throwStreamErrorIfNotOK $
            c_ld_client_make_loggroup_sync client' (BA# path') start end attrs' mkParent (MBA# group'')
        newForeignPtr c_free_logdevice_loggroup_fun group'

makeLogGroup
  :: HasCallStack
  => LDClient
  -> CBytes
  -> C_LogID -> C_LogID
  -> LogAttributes
  -> Bool
  -> IO LDLogGroup
makeLogGroup client path start end attrs mkParent = do
  logAttrs <- pokeLogAttributes attrs
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        let cfun = c_ld_client_make_loggroup client' (BA# path') start end attrs' mkParent
        MakeLogGroupCbData errno group _ <-
          withAsync makeLogGroupCbDataSize peekMakeLogGroupCbData cfun
        void $ E.throwStreamErrorIfNotOK' errno
        when (group == nullPtr) $ E.throwStoreError "null loggroup" callStack
        newForeignPtr c_free_logdevice_loggroup_fun group

getLogGroup :: HasCallStack => LDClient -> CBytes -> IO LDLogGroup
getLogGroup client path =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe path $ \path' -> do
    let cfun = c_ld_client_get_loggroup client' (BA# path')
    (errno, group_ptr, _) <- withAsyncPrimUnsafe2 (0 :: ErrorCode) nullPtr cfun
    void $ E.throwStreamErrorIfNotOK' errno
    newForeignPtr c_free_logdevice_loggroup_fun group_ptr

getLogGroupByID :: HasCallStack => LDClient -> C_LogID -> IO LDLogGroup
getLogGroupByID client logid = withForeignPtr client $ \client' -> do
  (errno, group_ptr, _) <- withAsyncPrimUnsafe2 (0 :: ErrorCode) nullPtr (c_ld_client_get_loggroup_by_id client' logid)
  void $ E.throwStreamErrorIfNotOK' errno
  newForeignPtr c_free_logdevice_loggroup_fun group_ptr

logIdHasGroup :: HasCallStack => LDClient -> C_LogID -> IO Bool
logIdHasGroup client logid = withForeignPtr client $ \client' -> do
  (errno, cbool, _) <-
    withAsyncPrimUnsafe2 (0 :: ErrorCode) 0 (ld_client_logid_has_group client' logid)
  case errno of
    C_OK       -> return $ cbool2bool cbool
    C_NOTFOUND -> return False
    code       -> E.throwStreamError code callStack

-- | Rename the leaf of the supplied path. This does not move entities in the
-- tree it only renames the last token in the path supplies.
--
-- The new path is the full path of the destination, it must not exist,
-- otherwise you will receive status of E::EXISTS
--
-- Throw one of the following exceptions on failure:
--
-- * E::ID_CLASH - the ID range clashes with existing log group.
-- * E::INVALID_ATTRIBUTES - After applying the parent attributes and the supplied
--                           attributes, the resulting attributes are not valid.
-- * E::NOTFOUND - source path doesn't exist.
-- * E::NOTDIR - if the parent of destination path doesn't exist and mk_intermediate_dirs is false.
-- * E::EXISTS the destination path already exists!
-- * E::TIMEDOUT Operation timed out.
-- * E::ACCESS you don't have permissions to mutate the logs configuration.
renameLogGroup
  :: HasCallStack
  => LDClient
  -> CBytes
  -- ^ The source path to rename
  -> CBytes
  -- ^ The new path you are renaming to
  -> IO C_LogsConfigVersion
  -- ^ Return the version of the logsconfig at which the path got renamed
renameLogGroup client from_path to_path =
  CBytes.withCBytesUnsafe from_path $ \from_path_ ->
    CBytes.withCBytesUnsafe to_path $ \to_path_ ->
      withForeignPtr client $ \client' -> do
        let size = logsConfigStatusCbDataSize
            peek_data = peekLogsConfigStatusCbData
            cfun = c_ld_client_rename client' (BA# from_path_) (BA# to_path_)
        LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
        void $ E.throwStreamErrorIfNotOK' errno
        return version

-- | Removes a logGroup defined at path
--
-- Throw one of the following exceptions on failure:
--
-- * NOTFOUND - source path doesn't exist.
-- * TIMEDOUT Operation timed out.
-- * ACCESS you don't have permissions to mutate the logs configuration.
removeLogGroup
  :: HasCallStack
  => LDClient
  -> CBytes
  -- ^ The path of loggroup to remove
  -> IO C_LogsConfigVersion
  -- ^ Return the version of the logsconfig at which the log
  -- group got removed
removeLogGroup client path =
  CBytes.withCBytesUnsafe path $ \path_ ->
    withForeignPtr client $ \client' -> do
      let size = logsConfigStatusCbDataSize
          peek_data = peekLogsConfigStatusCbData
          cfun = c_ld_client_remove_loggroup client' (BA# path_)
      LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
      void $ E.throwStreamErrorIfNotOK' errno
      return version

logGroupGetRange :: LDLogGroup -> IO C_LogRange
logGroupGetRange group =
  withForeignPtr group $ \group' -> do
    (start_ret, (end_ret, _)) <-
      Z.withPrimUnsafe C_LOGID_MIN_INVALID $ \start' ->
      Z.withPrimUnsafe C_LOGID_MIN_INVALID $ \end' ->
        c_ld_loggroup_get_range group' (MBA# start') (MBA# end')
    return (start_ret, end_ret)
{-# INLINE logGroupGetRange #-}

logGroupGetName :: LDLogGroup -> IO CBytes
logGroupGetName group =
  withForeignPtr group $ CBytes.fromCString <=< c_ld_loggroup_get_name
{-# INLINE logGroupGetName #-}

logGroupGetFullName :: LDLogGroup -> IO CBytes
logGroupGetFullName group =
  withForeignPtr group $ CBytes.fromCString <=< c_ld_loggroup_get_fully_qualified_name
{-# INLINE logGroupGetFullName #-}

-- Note that this pointer **may** only valid if LogGroup is valid.
logGroupGetAttrsPtr :: LDLogGroup -> IO (Ptr LogDeviceLogAttributes)
logGroupGetAttrsPtr group = withForeignPtr group c_ld_loggroup_get_attrs
{-# INLINE logGroupGetAttrsPtr #-}

logGroupGetAttrs :: LDLogGroup -> IO LogAttributes
logGroupGetAttrs group =
  withForeignPtr group $ peekLogAttributes <=< c_ld_loggroup_get_attrs
{-# INLINE logGroupGetAttrs #-}

logGroupGetExtraAttr :: LDLogGroup -> CBytes -> IO (Maybe CBytes)
logGroupGetExtraAttr group key = withForeignPtr group $ \group' ->
  CBytes.withCBytesUnsafe key $ \key' -> do
    attrs' <- c_ld_loggroup_get_attrs group'
    et <- c_exist_log_attrs_extras attrs' (BA# key')
    if et then Just . CBytes.fromBytes <$> Z.fromStdString (c_get_log_attrs_extra attrs' (BA# key'))
          else return Nothing
{-# INLINE logGroupGetExtraAttr #-}

logGroupUpdateExtraAttrs
  :: HasCallStack
  => LDClient -> LDLogGroup -> Map.Map CBytes CBytes -> IO ()
logGroupUpdateExtraAttrs client group extraAttrs =
  withForeignPtr client $ \client' ->
  withForeignPtr group $ \group' -> do
    let extras = Map.toList extraAttrs
    let ks = map (CBytes.rawPrimArray . fst) extras
        vs = map (CBytes.rawPrimArray . snd) extras
    Z.withPrimArrayListUnsafe ks $ \ks' l ->
      Z.withPrimArrayListUnsafe vs $ \vs' _ -> do
        let size = logsConfigStatusCbDataSize
            peek_data = peekLogsConfigStatusCbData
            cfun = c_ld_loggroup_update_extra_attrs client' group' l (BAArray# ks') (BAArray# vs')
        LogsConfigStatusCbData errno version _failure_reason <- withAsync size peek_data cfun
        void $ E.throwStreamErrorIfNotOK' errno
        syncLogsConfigVersion client version
{-# INLINE logGroupUpdateExtraAttrs #-}

logGroupGetVersion :: LDLogGroup -> IO C_LogsConfigVersion
logGroupGetVersion group = withForeignPtr group c_ld_loggroup_get_version
{-# INLINE logGroupGetVersion #-}

-- | This sets the log group range to the supplied new range.
--
-- Throw one of the following exceptions on failure:
--
-- * E::ID_CLASH - the ID range clashes with existing log group.
-- * E::NOTFOUND if the path doesn't exist or it's pointing to a directory
-- * E::INVALID_ATTRIBUTES the range you supplied is invalid or reserved for system-logs.
-- * E::TIMEDOUT Operation timed out.
-- * E::ACCESS you don't have permissions to mutate the logs configuration.
logGroupSetRange
  :: HasCallStack
  => LDClient
  -> CBytes
  -- ^ The path to the log group
  -> C_LogRange
  -- ^ The new range to be set
  -> IO C_LogsConfigVersion
  -- ^ Return the version of the logsconfig at which the range got updated
logGroupSetRange client path (start, end) =
  withForeignPtr client $ \client' ->
    CBytes.withCBytesUnsafe path $ \path' -> do
    let size = logsConfigStatusCbDataSize
        peek_data = peekLogsConfigStatusCbData
        cfun = c_ld_client_set_log_group_range client' (BA# path') start end
    LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
    void $ E.throwStreamErrorIfNotOK' errno
    return version

foreign import ccall unsafe "hs_logdevice.h ld_client_set_log_group_range"
  c_ld_client_set_log_group_range
    :: Ptr LogDeviceClient
    -> BA# Word8
    -> C_LogID
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> Ptr LogsConfigStatusCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_make_loggroup"
  c_ld_client_make_loggroup
    :: Ptr LogDeviceClient
    -> BA# Word8
    -> C_LogID
    -> C_LogID
    -> Ptr LogDeviceLogAttributes
    -> Bool
    -> StablePtr PrimMVar -> Int
    -> Ptr MakeLogGroupCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_make_loggroup_sync"
  c_ld_client_make_loggroup_sync
    :: Ptr LogDeviceClient
    -> BA# Word8
    -> C_LogID
    -> C_LogID
    -> Ptr LogDeviceLogAttributes
    -> Bool
    -> MBA# (Ptr LogDeviceLogGroup) -- ^ result, can be nullptr
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_get_loggroup"
  c_ld_client_get_loggroup
    :: Ptr LogDeviceClient
    -> BA# Word8
    -> StablePtr PrimMVar -> Int
    -- results
    -> MBA# ErrorCode
    -> MBA# (Ptr LogDeviceLogGroup)
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_client_get_loggroup_by_id"
  c_ld_client_get_loggroup_by_id
    :: Ptr LogDeviceClient
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> MBA# (Ptr LogDeviceLogGroup)
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_client_logid_has_group"
  ld_client_logid_has_group
    :: Ptr LogDeviceClient
    -> C_LogID
    -> StablePtr PrimMVar -> Int
    -> MBA# ErrorCode
    -> MBA# CBool
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup"
  c_ld_client_remove_loggroup :: Ptr LogDeviceClient
                              -> BA# Word8
                              -> StablePtr PrimMVar -> Int
                              -> Ptr LogsConfigStatusCbData
                              -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_range"
  c_ld_loggroup_get_range :: Ptr LogDeviceLogGroup
                          -> MBA# C_LogID    -- ^ returned value, start logid
                          -> MBA# C_LogID    -- ^ returned value, end logid
                          -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_name"
  c_ld_loggroup_get_name :: Ptr LogDeviceLogGroup -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_fully_qualified_name"
  c_ld_loggroup_get_fully_qualified_name :: Ptr LogDeviceLogGroup -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_attrs"
  c_ld_loggroup_get_attrs :: Ptr LogDeviceLogGroup -> IO (Ptr LogDeviceLogAttributes)

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_get_version"
  c_ld_loggroup_get_version :: Ptr LogDeviceLogGroup -> IO Word64

foreign import ccall unsafe "hs_logdevice.h ld_loggroup_update_extra_attrs"
  c_ld_loggroup_update_extra_attrs
    :: Ptr LogDeviceClient
    -> Ptr LogDeviceLogGroup
    -> Int -> BAArray# Word8 -> BAArray# Word8
    -> StablePtr PrimMVar -> Int -> Ptr LogsConfigStatusCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h free_logdevice_loggroup"
  c_free_logdevice_loggroup :: Ptr LogDeviceLogGroup -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_loggroup"
  c_free_logdevice_loggroup_fun :: FunPtr (Ptr LogDeviceLogGroup -> IO ())

-------------------------------------------------------------------------------
-- * Misc

-- | This waits (blocks) until this Client's local view of LogsConfig catches up
-- to the given version or higher, or until the timeout has passed.
-- Doesn't wait for config propagation to servers.
--
-- This guarantees that subsequent get*() calls (getDirectory(), getLogGroup()
-- etc) will get an up-to-date view.
-- Does *not* guarantee that subsequent append(), makeDirectory(),
-- 'makeLogGroup', etc, will have an up-to-date view.
syncLogsConfigVersion
  :: HasCallStack
  => LDClient
  -> C_LogsConfigVersion
  -- ^ The minimum version you need to sync LogsConfig to
  -> IO ()
syncLogsConfigVersion client version =
  withForeignPtr client $ \client' -> void $ E.throwStreamErrorIfNotOK $
    c_ld_client_sync_logsconfig_version client' version
{-# INLINE syncLogsConfigVersion #-}

ldWriteAttributes
  :: HasCallStack
  => LDClient
  -> CBytes
  -> Ptr LogDeviceLogAttributes
  -> IO C_LogsConfigVersion
ldWriteAttributes client path attrs' =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe path $ \path' -> do
    let size = logsConfigStatusCbDataSize
        peek_data = peekLogsConfigStatusCbData
        cfun = c_ld_client_set_attributes client' (BA# path') attrs'
    LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
    void $ E.throwStreamErrorIfNotOK' errno
    return version
{-# INLINE ldWriteAttributes #-}

foreign import ccall safe "hs_logdevice.h ld_client_sync_logsconfig_version"
  c_ld_client_sync_logsconfig_version
    :: Ptr LogDeviceClient
    -> Word64
    -- ^ The minimum version you need to sync LogsConfig to
    -> IO ErrorCode
    -- ^ Return TIMEDOUT on timeout or OK on successful.

foreign import ccall unsafe "hs_logdevice.h ld_client_set_attributes"
  c_ld_client_set_attributes
    :: Ptr LogDeviceClient
    -> BA# Word8
    -> Ptr LogDeviceLogAttributes
    -> StablePtr PrimMVar -> Int
    -> Ptr LogsConfigStatusCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_rename"
  c_ld_client_rename :: Ptr LogDeviceClient
                     -> BA# Word8    -- ^ from_path
                     -> BA# Word8    -- ^ to_path
                     -> StablePtr PrimMVar -> Int
                     -> Ptr LogsConfigStatusCbData
                     -> IO ErrorCode
