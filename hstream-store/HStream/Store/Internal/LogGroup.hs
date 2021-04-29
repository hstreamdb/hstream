module HStream.Store.Internal.LogGroup where

import           Control.Monad                  (void, (<=<))
import qualified Data.Map.Strict                as Map
import           Data.Word                      (Word64)
import           Foreign.ForeignPtr             (newForeignPtr, withForeignPtr)
import           Foreign.Ptr                    (nullPtr)
import           GHC.Stack                      (HasCallStack)
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.CBytes                  as CBytes
import qualified Z.Foreign                      as Z

import qualified HStream.Store.Exception        as E
import qualified HStream.Store.Internal.FFI     as FFI
import qualified HStream.Store.Internal.Foreign as FFI
import qualified HStream.Store.Internal.Types   as FFI

-------------------------------------------------------------------------------

-- | Creates a log group under a specific directory path.
--
-- Note that, even after this method returns success, it may take some time
-- for the update to propagate to all servers, so the new log group may not
-- be usable for a few seconds (appends may fail with NOTFOUND or
-- NOTINSERVERCONFIG). Same applies to all other logs config update methods,
-- e.g. setAttributes().
makeLogGroup
  :: HasCallStack
  => FFI.LDClient
  -> CBytes
  -> FFI.C_LogID
  -> FFI.C_LogID
  -> FFI.LogAttrs
  -> Bool
  -> IO FFI.LDLogGroup
makeLogGroup client path start end attrs mkParent = do
  logAttrs <- case attrs of
                FFI.LogAttrs val  -> hsLogAttrsToLogDevice val
                FFI.LogAttrsPtr p -> return p
  withForeignPtr client $ \client' ->
    withForeignPtr logAttrs $ \attrs' ->
      CBytes.withCBytesUnsafe path $ \path' -> do
        (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
          void $ E.throwStreamErrorIfNotOK $
            -- TODO: use async version
            FFI.c_ld_client_make_loggroup_sync client' path' start end attrs' mkParent group''
        newForeignPtr FFI.c_free_logdevice_loggroup_fun group'

getLogGroup :: HasCallStack => FFI.LDClient -> CBytes -> IO FFI.LDLogGroup
getLogGroup client path =
  withForeignPtr client $ \client' ->
  CBytes.withCBytesUnsafe path $ \path' -> do
    (group', _) <- Z.withPrimUnsafe nullPtr $ \group'' ->
      -- TODO: use async version
      void $ E.throwStreamErrorIfNotOK $ FFI.c_ld_client_get_loggroup_sync client' path' group''
    newForeignPtr FFI.c_free_logdevice_loggroup_fun group'

-- | Rename the leaf of the supplied path. This does not move entities in the
--   tree it only renames the last token in the path supplies.
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
renameTopicGroup
  :: FFI.LDClient
  -> CBytes
  -- ^ The source path to rename
  -> CBytes
  -- ^ The new path you are renaming to
  -> IO Word64
  -- ^ Return the version of the logsconfig at which the path got renamed
renameTopicGroup client from_path to_path =
  CBytes.withCBytesUnsafe from_path $ \from_path_ ->
    CBytes.withCBytesUnsafe to_path $ \to_path_ ->
      withForeignPtr client $ \client' -> do
        let size = FFI.logsconfigStatusCbDataSize
            peek_data = FFI.peekLogsconfigStatusCbData
            cfun = FFI.c_ld_client_rename client' from_path_ to_path_
        FFI.LogsconfigStatusCbData errno version _ <- FFI.withAsync size peek_data cfun
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
  :: FFI.LDClient
  -> CBytes
  -- ^ The path of loggroup to remove
  -> IO FFI.C_LogsConfigVersion
  -- ^ Return the version of the logsconfig at which the log
  -- group got removed
removeLogGroup client path =
  CBytes.withCBytesUnsafe path $ \path_ ->
    withForeignPtr client $ \client' -> do
      let size = FFI.logsconfigStatusCbDataSize
          peek_data = FFI.peekLogsconfigStatusCbData
          cfun = FFI.c_ld_client_remove_loggroup client' path_
      FFI.LogsconfigStatusCbData errno version _ <- FFI.withAsync size peek_data cfun
      void $ E.throwStreamErrorIfNotOK' errno
      return version

logGroupGetRange :: FFI.LDLogGroup -> IO FFI.C_LogRange
logGroupGetRange group =
  withForeignPtr group $ \group' -> do
    (start_ret, (end_ret, _)) <-
      Z.withPrimUnsafe (FFI.unTopicID FFI.TOPIC_ID_INVALID) $ \start' ->
      Z.withPrimUnsafe (FFI.unTopicID FFI.TOPIC_ID_INVALID) $ \end' ->
        FFI.c_ld_loggroup_get_range group' start' end'
    return (start_ret, end_ret)
{-# INLINE logGroupGetRange #-}

logGroupGetName :: FFI.LDLogGroup -> IO CBytes
logGroupGetName group =
  withForeignPtr group $ CBytes.fromCString <=< FFI.c_ld_loggroup_get_name
{-# INLINE logGroupGetName #-}

-- Note that this pointer only valiad if LogGroup is valiad.
logGroupGetAttrs :: FFI.LDLogGroup -> IO (Z.Ptr FFI.LogDeviceLogAttributes)
logGroupGetAttrs group =
  withForeignPtr group $ \group' -> FFI.c_ld_loggroup_get_attrs group'
{-# INLINE logGroupGetAttrs #-}

logGroupGetExtraAttr :: FFI.LDLogGroup -> CBytes -> IO CBytes
logGroupGetExtraAttr group key =
  withForeignPtr group $ \group' ->
  CBytes.withCBytesUnsafe key $ \key' -> do
    attrs_ptr <- FFI.c_ld_loggroup_get_attrs group'
    CBytes.fromBytes <$> Z.fromStdString (FFI.c_get_log_attrs_extra attrs_ptr key')
{-# INLINE logGroupGetExtraAttr #-}

logGroupSetExtraAttr :: FFI.LDLogGroup -> CBytes -> CBytes -> IO ()
logGroupSetExtraAttr group key val =
  withForeignPtr group $ \group' ->
  CBytes.withCBytesUnsafe key $ \key' ->
  CBytes.withCBytesUnsafe val $ \val' -> do
    attrs_ptr <- FFI.c_ld_loggroup_get_attrs group'
    FFI.c_set_log_attrs_extra attrs_ptr key' val'
{-# INLINE logGroupSetExtraAttr #-}

logGroupGetVersion :: FFI.LDLogGroup -> IO FFI.C_LogsConfigVersion
logGroupGetVersion group = withForeignPtr group FFI.c_ld_loggroup_get_version
{-# INLINE logGroupGetVersion #-}

-------------------------------------------------------------------------------
-- LogAttributes

hsLogAttrsToLogDevice :: FFI.HsLogAttrs -> IO FFI.LDLogAttrs
hsLogAttrsToLogDevice FFI.HsLogAttrs{..} = do
  let extras = Map.toList extraTopicAttrs
  let ks = map (CBytes.rawPrimArray . fst) extras
      vs = map (CBytes.rawPrimArray . snd) extras
  Z.withPrimArrayListUnsafe ks $ \ks' l ->
    Z.withPrimArrayListUnsafe vs $ \vs' _ -> do
      i <- FFI.c_new_log_attributes (fromIntegral replicationFactor) l ks' vs'
      newForeignPtr FFI.c_free_log_attributes_fun i

getLogExtraAttr :: FFI.LDLogAttrs -> CBytes -> IO CBytes
getLogExtraAttr attrs key =
  withForeignPtr attrs $ \attrs' ->
  CBytes.withCBytesUnsafe key $ \key' ->
    CBytes.fromBytes <$> Z.fromStdString (FFI.c_get_log_attrs_extra attrs' key')

setLogExtraAttr :: FFI.LDLogAttrs -> CBytes -> CBytes -> IO ()
setLogExtraAttr attrs key val =
  withForeignPtr attrs $ \attrs' ->
  CBytes.withCBytesUnsafe key $ \key' ->
  CBytes.withCBytesUnsafe val $ \val' -> FFI.c_set_log_attrs_extra attrs' key' val'

-------------------------------------------------------------------------------

-- | This waits (blocks) until this Client's local view of LogsConfig catches up
-- to the given version or higher, or until the timeout has passed.
-- Doesn't wait for config propagation to servers.
--
-- This guarantees that subsequent get*() calls (getDirectory(), getLogGroup()
-- etc) will get an up-to-date view.
-- Does *not* guarantee that subsequent append(), makeDirectory(),
-- 'makeLogGroup', etc, will have an up-to-date view.
syncTopicConfigVersion
  :: HasCallStack
  => FFI.LDClient
  -> FFI.C_LogsConfigVersion
  -- ^ The minimum version you need to sync LogsConfig to
  -> IO ()
syncTopicConfigVersion client version =
  withForeignPtr client $ \client' -> void $ E.throwStreamErrorIfNotOK $
    FFI.c_ld_client_sync_logsconfig_version client' version
