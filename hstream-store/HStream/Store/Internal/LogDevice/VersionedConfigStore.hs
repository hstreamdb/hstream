{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice.VersionedConfigStore where

import           Data.Int
import           Data.Word
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Foreign.StablePtr
import           GHC.Conc
import           GHC.Stack
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.CBytes                  as CBytes
import           Z.Data.Vector                  (Bytes)
import           Z.Foreign                      (BA#)
import qualified Z.Foreign                      as Z

import qualified HStream.Store.Exception        as E
import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.Types
import qualified HStream.Store.Logger           as Log

-------------------------------------------------------------------------------

data VcsCondition
  = VcsCondVersion VcsConfigVersion
  | VcsCondOverwrite
  | VcsCondIfNotExists

-- | Condition mode
--
-- * condition_mode 1 : VERSION
-- * condition_mode 2 : OVERWRITE
-- * condition_mode 3 : IF_NOT_EXISTS
--
-- If condition_mode is 2 or 3, then the version will be ignored.
toCVcsConditionMode :: VcsCondition -> (Int, VcsConfigVersion)
toCVcsConditionMode (VcsCondVersion ver) = (1, ver)
toCVcsConditionMode VcsCondOverwrite     = (2, 0)
toCVcsConditionMode VcsCondIfNotExists   = (3, 0)

newRsmBasedVcs :: LDClient -> C_LogID -> Int64 -> IO LDVersionedConfigStore
newRsmBasedVcs client logid stopTimeout =
  withForeignPtr client $ \clientPtr -> do
    i <- c_new_rsm_based_vcs clientPtr logid stopTimeout
    newForeignPtr c_free_rsm_based_vcs_fun i

vcsGetConfig
  :: HasCallStack
  => LDVersionedConfigStore
  -> CBytes
  -> Maybe VcsConfigVersion
  -> Int
  -- ^ Retries when 'C_AGAIN' returned. Negative means retry forever.
  -> IO Bytes
vcsGetConfig vcs key m_base_version auto_retries =
  withForeignPtr vcs $ \vcs' -> CBytes.withCBytesUnsafe key $ \key' ->
    go vcs' key' auto_retries
  where
    go vcs' key' retries = do
      cbData <- run vcs' key'
      let st = vcsValCallbackSt cbData
      case st of
        C_OK -> return $ vcsValCallbackVal cbData
        C_AGAIN
          | retries == 0 -> E.throwStreamError st callStack
          | retries < 0 -> threadDelay 5000 >> (go vcs' key' $! (-1))
          | retries > 0 -> threadDelay 5000 >> (go vcs' key' $! (retries - 1))
        _ -> E.throwStreamError st callStack
    run vcs' key' =
      case m_base_version of
        Just bv -> snd <$> (Z.withPrimUnsafe bv $ \ver_ -> fst <$> f (c_logdevice_vcs_get_config vcs' key' (unsafeFreezeBA# ver_)))
        Nothing -> fst <$> f (c_logdevice_vcs_get_config' vcs' key' nullPtr)
    f = withAsync' vcsValueCallbackDataSize peekVcsValueCallbackData pure

vcsGetConfig'
  :: HasCallStack
  => LDVersionedConfigStore
  -> CBytes
  -> Maybe VcsConfigVersion
  -> IO Bytes
vcsGetConfig' vcs key m_base_version = vcsGetConfig vcs key m_base_version (-1)

ldVcsGetLatestConfig
  :: HasCallStack
  => LDVersionedConfigStore
  -> CBytes
  -> Int
  -> IO Bytes
ldVcsGetLatestConfig vcs key auto_retries =
  withForeignPtr vcs $ \vcs' -> CBytes.withCBytesUnsafe key $ \key' ->
    go vcs' key' auto_retries
  where
    go vcs' key' retries = do
      (cbData, _) <- withAsync' vcsValueCallbackDataSize peekVcsValueCallbackData pure
                                (c_logdevice_vcs_get_latest_config vcs' key')
      let st = vcsValCallbackSt cbData
      case st of
        C_OK -> return $ vcsValCallbackVal cbData
        C_AGAIN
          | retries == 0 -> E.throwStreamError st callStack
          | retries < 0 -> threadDelay 5000 >> (go vcs' key' $! (- 1))
          | retries > 0 -> threadDelay 5000 >> (go vcs' key' $! (retries - 1))
        _ -> E.throwStreamError st callStack

ldVcsGetLatestConfig'
  :: HasCallStack
  => LDVersionedConfigStore
  -> CBytes
  -> IO Bytes
ldVcsGetLatestConfig' vcs key = ldVcsGetLatestConfig vcs key (-1)

-- | VersionedConfigStore provides strict conditional update semantics--it
-- will only update the value for a key if the base_version matches the latest
-- version in the store.
--
-- logdevice cb:
--   callback void(Status, version_t, std::string value) that will be invoked
--   if the status is one of:
--     OK
--     NOTFOUND // only possible when base_version.hasValue()
--     VERSION_MISMATCH
--     ACCESS
--     AGAIN
--     BADMSG // see implementation notes below
--     INVALID_PARAM // see implementation notes below
--     INVALID_CONFIG // see implementation notes below
--     SHUTDOWN
--   If status is OK, cb will be invoked with the version of the newly written
--   config. If status is VERSION_MISMATCH, cb will be invoked with the
--   version that caused the mismatch as well as the existing config, if
--   available (i.e., always check in the callback whether version is
--   EMPTY_VERSION). Otherwise, the version and value parameter(s) are
--   meaningless (default-constructed).
ldVcsUpdateConfig
  :: HasCallStack
  => LDVersionedConfigStore
  -> CBytes
  -- ^ key of the config
  -> Bytes
  -- value to be stored. Note that the callsite need not guarantee the
  -- validity of the underlying buffer till callback is invoked.
  -> VcsCondition
  -> Int
  -> IO (VcsConfigVersion, Bytes)
ldVcsUpdateConfig vcs key val cond auto_retries =
  withForeignPtr vcs $ \vcs' ->
  CBytes.withCBytesUnsafe key $ \key' ->
    Z.withPrimVectorUnsafe val $ \val' offset len ->
      go vcs' key' val' offset len auto_retries
  where
    go vcs' key' val' offset len retries = do
      (cbData, _) <- withAsync' vcsWriteCallbackDataSize peekVcsWriteCallbackData pure
                                (c_logdevice_vcs_update_config vcs' key' val' offset len cond_mode cond_ver_)
      let st = vcsWriteCallbackSt cbData
      case st of
        C_OK -> return (vcsWriteCallbackVersion cbData, vcsWriteCallbackValue cbData)
        C_AGAIN
          | retries == 0 -> E.throwStreamError st callStack
          | retries < 0 -> threadDelay 5000 >> (go vcs' key' val' offset len $! (- 1))
          | retries > 0 -> threadDelay 5000 >> (go vcs' key' val' offset len $! (retries - 1))
        C_VERSION_MISMATCH -> do
          Log.warning "VersoinedConfigStore update config: VERSION_MISMATCH" >> Log.flushDefaultLogger
          return (vcsWriteCallbackVersion cbData, vcsWriteCallbackValue cbData)
        _ -> E.throwStreamError st callStack
    (cond_mode, cond_ver_) = toCVcsConditionMode cond

-------------------------------------------------------------------------------

foreign import ccall unsafe "hs_logdevice.h new_rsm_based_vcs"
  c_new_rsm_based_vcs
    :: Ptr LogDeviceClient
    -> C_LogID
    -> Int64          -- ^ stop_timeout, milliseconds
    -> IO (Ptr LogDeviceVersionedConfigStore)

foreign import ccall unsafe "hs_logdevice.h free_logdevice_vcs"
  c_free_rsm_based_vcs :: Ptr LogDeviceVersionedConfigStore -> IO ()

foreign import ccall unsafe "hs_logdevice.h &free_logdevice_vcs"
  c_free_rsm_based_vcs_fun :: FunPtr (Ptr LogDeviceVersionedConfigStore -> IO ())

foreign import ccall unsafe "hs_logdevice.h logdevice_vcs_get_config"
  c_logdevice_vcs_get_config
    :: Ptr LogDeviceVersionedConfigStore
    -> BA# Word8        -- ^ key
    -> BA# Word64       -- ^ base_version: Just version
    -> StablePtr PrimMVar -> Int
    -> Ptr VcsValueCallbackData
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h logdevice_vcs_get_config"
  c_logdevice_vcs_get_config'
    :: Ptr LogDeviceVersionedConfigStore
    -> BA# Word8        -- ^ key
    -> Ptr Word64       -- ^ base_version: Nothing, must be NULL
    -> StablePtr PrimMVar -> Int
    -> Ptr VcsValueCallbackData
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h logdevice_vcs_get_latest_config"
  c_logdevice_vcs_get_latest_config
    :: Ptr LogDeviceVersionedConfigStore
    -> BA# Word8        -- ^ key
    -> StablePtr PrimMVar -> Int
    -> Ptr VcsValueCallbackData
    -> IO ()

foreign import ccall unsafe "hs_logdevice.h logdevice_vcs_update_config"
  c_logdevice_vcs_update_config
    :: Ptr LogDeviceVersionedConfigStore
    -> BA# Word8                    -- ^ key
    -> BA# Word8 -> Int -> Int      -- ^ value_ptr, offset, length
    -> Int
    -- ^ condition_mode, must be 1, 2 or 3
    --
    -- - condition_mode 1 : VERSION
    -- - condition_mode 2 : OVERWRITE
    -- - condition_mode 3 : IF_NOT_EXISTS
    --
    -- If condition_mode is 2 or 3, then the version will be ignored.
    -> VcsConfigVersion     -- ^ condition_mode base_version
    -> StablePtr PrimMVar -> Int
    -> Ptr VcsWriteCallbackData
    -> IO ()
