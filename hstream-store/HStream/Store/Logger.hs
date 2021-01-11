{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Logger
  ( -- * LogDevice debug level
    FFI.C_DBG_LEVEL
  , pattern FFI.C_DBG_CRITICAL
  , pattern FFI.C_DBG_ERROR
  , pattern FFI.C_DBG_WARNING
  , pattern FFI.C_DBG_NOTIFY
  , pattern FFI.C_DBG_INFO
  , pattern FFI.C_DBG_DEBUG
  , pattern FFI.C_DBG_SPEW

  , setLogDeviceDbgLevel
  , logDeviceDbgUseFD
  ) where

import           Foreign.C.Types      (CInt)

import qualified HStream.Internal.FFI as FFI

type FD = CInt

setLogDeviceDbgLevel :: FFI.C_DBG_LEVEL -> IO ()
setLogDeviceDbgLevel = FFI.c_set_dbg_level

logDeviceDbgUseFD :: FD -> IO FD
logDeviceDbgUseFD = FFI.c_dbg_use_fd
