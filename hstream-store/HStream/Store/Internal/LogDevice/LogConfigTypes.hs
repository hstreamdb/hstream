{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
-- Note that we need this UnboxedTuples to force ghci use -fobject-code for all
-- related modules. Or ghci will complain "panic".
--
-- Also, manual add @{-# OPTIONS_GHC -fobject-code #-}@ is possible, but need
-- to add all imported local modules. :(
--
-- Relatead ghc issues:
-- * https://gitlab.haskell.org/ghc/ghc/-/issues/19733
-- * https://gitlab.haskell.org/ghc/ghc/-/issues/15454
{-# LANGUAGE UnboxedTuples #-}

module HStream.Store.Internal.LogDevice.LogConfigTypes where

import           Control.Exception              (finally)
import           Control.Monad                  (void, (<=<))
import qualified Data.Map.Strict                as Map
import           Data.Word
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack                      (HasCallStack)
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.CBytes                  as CBytes
import           Z.Foreign                      (BA#, BAArray#, MBA#)
import qualified Z.Foreign                      as Z

import qualified HStream.Store.Exception        as E
import           HStream.Store.Internal.Foreign (peekStdStringToCBytesN,
                                                 withAsync,
                                                 withAsyncPrimUnsafe2)
import           HStream.Store.Internal.Types

-------------------------------------------------------------------------------
-- * LogAttributes

hsLogAttrsToLDLogAttrs :: HsLogAttrs -> IO LDLogAttrs
hsLogAttrsToLDLogAttrs HsLogAttrs{..} = do
  let extras = Map.toList logExtraAttrs
  let ks = map (CBytes.rawPrimArray . fst) extras
      vs = map (CBytes.rawPrimArray . snd) extras
  Z.withPrimArrayListUnsafe ks $ \ks' l ->
    Z.withPrimArrayListUnsafe vs $ \vs' _ -> do
      i <- c_new_log_attributes (fromIntegral logReplicationFactor) l ks' vs'
      newForeignPtr c_free_log_attributes_fun i

getLogAttrsExtra :: LDLogAttrs -> CBytes -> IO CBytes
getLogAttrsExtra attrs key =
  withForeignPtr attrs $ \attrs' ->
  CBytes.withCBytesUnsafe key $ \key' ->
    CBytes.fromBytes <$> Z.fromStdString (c_get_log_attrs_extra attrs' key')

updateLogAttrsExtrasPtr
  :: Ptr LogDeviceLogAttributes
  -> Map.Map CBytes CBytes
  -> IO (Ptr LogDeviceLogAttributes)
updateLogAttrsExtrasPtr attrs' logExtraAttrs = do
  let extras = Map.toList logExtraAttrs
  let ks = map (CBytes.rawPrimArray . fst) extras
      vs = map (CBytes.rawPrimArray . snd) extras
  Z.withPrimArrayListUnsafe ks $ \ks' l ->
    Z.withPrimArrayListUnsafe vs $ \vs' _ -> do
      c_update_log_attrs_extras attrs' l ks' vs'

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

foreign import ccall unsafe "hs_logdevice.h get_log_attrs_extra"
  c_get_log_attrs_extra
    :: Ptr LogDeviceLogAttributes
    -> BA# Word8
    -> IO (Ptr Z.StdString)

foreign import ccall unsafe "hs_logdevice.h free_log_attributes"
  c_free_log_attributes :: Ptr LogDeviceLogAttributes -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_log_attributes"
  c_free_log_attributes_fun :: FunPtr (Ptr LogDeviceLogAttributes -> IO ())

-------------------------------------------------------------------------------
-- * Directory

getLogDirectory :: LDClient -> CBytes -> IO LDDirectory
getLogDirectory client path =
  CBytes.withCBytesUnsafe path $ \path' ->
    withForeignPtr client $ \client' -> do
      let cfun = c_ld_client_get_directory client' path'
      (errno, dir, _) <- withAsyncPrimUnsafe2 (0 :: ErrorCode) nullPtr cfun
      _ <- E.throwStreamErrorIfNotOK' errno
      newForeignPtr c_free_logdevice_logdirectory_fun dir

logDirectoryGetName :: LDDirectory -> IO CBytes
logDirectoryGetName dir = withForeignPtr dir $
   CBytes.fromCString <=< c_ld_logdirectory_get_name

logDirectoryGetLogsName :: Bool -> LDDirectory -> IO [CBytes]
logDirectoryGetLogsName recursive dir = withForeignPtr dir $ \dir' -> do
  (len, (names_ptr, stdvec_ptr)) <-
    Z.withPrimUnsafe 0 $ \len' ->
    Z.withPrimUnsafe nullPtr $ \names' ->
    fst <$> (Z.withPrimUnsafe nullPtr $ \stdvec' ->
      c_ld_logdirectory_get_logs_name dir' recursive len' names' stdvec')
  if names_ptr == nullPtr
     then return []
     else finally (peekStdStringToCBytesN len names_ptr) (c_free_logs_name stdvec_ptr)

logDirectoryGetVersion :: LDDirectory -> IO C_LogsConfigVersion
logDirectoryGetVersion dir = withForeignPtr dir c_ld_logdirectory_get_version

makeLogDirectory
  :: LDClient
  -> CBytes
  -> LogAttrs
  -> Bool
  -> IO LDDirectory
makeLogDirectory client path attrs mkParent = do
  logAttrs <- case attrs of
                LogAttrs val  -> hsLogAttrsToLDLogAttrs val
                LogAttrsPtr p -> return p
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        let cfun = c_ld_client_make_directory client' path' mkParent attrs'
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
          cfun = c_ld_client_remove_directory client' path' recursive
      LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
      void $ E.throwStreamErrorIfNotOK' errno
      return version

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

foreign import ccall unsafe "hs_logdevice.h ld_client_get_directory_sync"
  c_ld_client_get_directory_sync :: Ptr LogDeviceClient
                                 -> BA# Word8
                                 -> MBA# (Ptr LogDeviceLogDirectory)
                                 -> IO ErrorCode

foreign import ccall safe "hs_logdevice.h ld_client_remove_directory_sync"
  c_ld_client_remove_directory_sync_safe
    :: Ptr LogDeviceClient
    -> Ptr Word8   -- ^ path
    -> Bool
    -> Ptr Word64
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_directory"
  c_ld_client_remove_directory
    :: Ptr LogDeviceClient
    -> BA# Word8   -- ^ path
    -> Bool
    -> StablePtr PrimMVar -> Int
    -> Ptr LogsConfigStatusCbData
    -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_name"
  c_ld_logdirectory_get_name :: Ptr LogDeviceLogDirectory -> IO CString

foreign import ccall unsafe "hs_logdevice.h ld_logdirectory_get_version"
  c_ld_logdirectory_get_version :: Ptr LogDeviceLogDirectory -> IO Word64

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
    -> MBA# CSize -> MBA# (Ptr Z.StdString)
    -> MBA# (Ptr a)
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h free_logs_name"
  c_free_logs_name :: Ptr a -> IO ()

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
  -> LogAttrs
  -> Bool
  -> IO LDLogGroup
makeLogGroupSync client path start end attrs mkParent = do
  logAttrs <- case attrs of
                LogAttrs val  -> hsLogAttrsToLDLogAttrs val
                LogAttrsPtr p -> return p
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
          void $ E.throwStreamErrorIfNotOK $
            c_ld_client_make_loggroup_sync client' path' start end attrs' mkParent group''
        newForeignPtr c_free_logdevice_loggroup_fun group'

makeLogGroup
   :: HasCallStack
   => LDClient
   -> CBytes
   -> C_LogID
   -> C_LogID
   -> LogAttrs
   -> Bool
   -> IO LDLogGroup
makeLogGroup client path start end attrs mkParent = do
  logAttrs <- case attrs of
                LogAttrs val  -> hsLogAttrsToLDLogAttrs val
                LogAttrsPtr p -> return p
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        let cfun = c_ld_client_make_loggroup client' path' start end attrs' mkParent
        MakeLogGroupCbData errno group _ <-
          withAsync makeLogGroupCbDataSize peekMakeLogGroupCbData cfun
        void $ E.throwStreamErrorIfNotOK' errno
        newForeignPtr c_free_logdevice_loggroup_fun group

getLogGroupSync :: HasCallStack => LDClient -> CBytes -> IO LDLogGroup
getLogGroupSync client path =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
      void $ E.throwStreamErrorIfNotOK $ c_ld_client_get_loggroup_sync client' path' group''
    newForeignPtr c_free_logdevice_loggroup_fun group'

getLogGroup :: HasCallStack => LDClient -> CBytes -> IO LDLogGroup
getLogGroup client path =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe path $ \path' -> do
    let cfun = c_ld_client_get_loggroup client' path'
    (errno, group_ptr, _) <- withAsyncPrimUnsafe2 (0 :: ErrorCode) nullPtr cfun
    void $ E.throwStreamErrorIfNotOK' errno
    newForeignPtr c_free_logdevice_loggroup_fun group_ptr

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
            cfun = c_ld_client_rename client' from_path_ to_path_
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
          cfun = c_ld_client_remove_loggroup client' path_
      LogsConfigStatusCbData errno version _ <- withAsync size peek_data cfun
      void $ E.throwStreamErrorIfNotOK' errno
      return version

logGroupGetRange :: LDLogGroup -> IO C_LogRange
logGroupGetRange group =
  withForeignPtr group $ \group' -> do
    (start_ret, (end_ret, _)) <-
      Z.withPrimUnsafe C_LOGID_MIN_INVALID $ \start' ->
      Z.withPrimUnsafe C_LOGID_MIN_INVALID $ \end' ->
        c_ld_loggroup_get_range group' start' end'
    return (start_ret, end_ret)
{-# INLINE logGroupGetRange #-}

logGroupGetName :: LDLogGroup -> IO CBytes
logGroupGetName group =
  withForeignPtr group $ CBytes.fromCString <=< c_ld_loggroup_get_name
{-# INLINE logGroupGetName #-}

-- Note that this pointer only valiad if LogGroup is valiad.
logGroupGetAttrs :: LDLogGroup -> IO (Ptr LogDeviceLogAttributes)
logGroupGetAttrs group =
  withForeignPtr group $ \group' -> c_ld_loggroup_get_attrs group'
{-# INLINE logGroupGetAttrs #-}

logGroupGetExtraAttr :: LDLogGroup -> CBytes -> IO CBytes
logGroupGetExtraAttr group key =
  withForeignPtr group $ \group' ->
  CBytes.withCBytesUnsafe key $ \key' -> do
    attrs_ptr <- c_ld_loggroup_get_attrs group'
    CBytes.fromBytes <$> Z.fromStdString (c_get_log_attrs_extra attrs_ptr key')
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
            cfun = c_ld_loggroup_update_extra_attrs client' group' l ks' vs'
        LogsConfigStatusCbData errno version _failure_reason <- withAsync size peek_data cfun
        void $ E.throwStreamErrorIfNotOK' errno
        syncLogsConfigVersion client version

{-# INLINE logGroupUpdateExtraAttrs #-}

logGroupGetVersion :: LDLogGroup -> IO C_LogsConfigVersion
logGroupGetVersion group = withForeignPtr group c_ld_loggroup_get_version
{-# INLINE logGroupGetVersion #-}

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
  c_ld_client_get_loggroup :: Ptr LogDeviceClient
                           -> BA# Word8
                           -> StablePtr PrimMVar -> Int
                           -> MBA# ErrorCode
                           -> MBA# (Ptr LogDeviceLogGroup)
                           -> IO ()

foreign import ccall unsafe "hs_logdevice.h ld_client_get_loggroup_sync"
  c_ld_client_get_loggroup_sync :: Ptr LogDeviceClient
                                -> BA# Word8
                                -> MBA# (Ptr LogDeviceLogGroup)
                                -> IO ErrorCode

foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup_sync"
  c_ld_client_remove_loggroup_sync :: Ptr LogDeviceClient
                                    -> BA# Word8
                                    -> Ptr Word64
                                    -> IO ErrorCode
foreign import ccall unsafe "hs_logdevice.h ld_client_remove_loggroup_sync"
  c_ld_client_remove_loggroup_sync' :: Ptr LogDeviceClient
                                    -> BA# Word8
                                    -> MBA# Word64
                                    -> IO ErrorCode

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

foreign import ccall safe "hs_logdevice.h free_logdevice_loggroup"
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
  => LDClient -> CBytes -> Ptr LogDeviceLogAttributes -> IO ()
ldWriteAttributes client path attrs' =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe path $ \path' -> do
    let size = logsConfigStatusCbDataSize
        peek_data = peekLogsConfigStatusCbData
        cfun = c_ld_client_set_attributes client' path' attrs'
    LogsConfigStatusCbData errno _ _ <- withAsync size peek_data cfun
    void $ E.throwStreamErrorIfNotOK' errno
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
