{-# LANGUAGE PatternSynonyms #-}

module HStream.Store.Logger
  ( -- * LogDevice debug level
    LDLogLevel (..)
  , FFI.C_DBG_LEVEL
  , pattern FFI.C_DBG_CRITICAL
  , pattern FFI.C_DBG_ERROR
  , pattern FFI.C_DBG_WARNING
  , pattern FFI.C_DBG_NOTIFY
  , pattern FFI.C_DBG_INFO
  , pattern FFI.C_DBG_DEBUG
  , pattern FFI.C_DBG_SPEW

  , setLogDeviceDbgLevel
  , setLogDeviceDbgLevel'
  , logDeviceDbgUseFD
  ) where

import           Foreign.C.Types              (CInt)
import qualified HStream.Store.Internal.Types as FFI
import qualified Text.Read                    as Read

type FD = CInt

newtype LDLogLevel = LDLogLevel {unLDLogLevel :: FFI.C_DBG_LEVEL}

instance Read LDLogLevel where
  readPrec = do
    Read.lexP >>= \case
      Read.Ident "critical"        -> return $ LDLogLevel FFI.C_DBG_CRITICAL
      Read.Ident "error"           -> return $ LDLogLevel FFI.C_DBG_ERROR
      Read.Ident "warning"         -> return $ LDLogLevel FFI.C_DBG_WARNING
      Read.Ident "notify"          -> return $ LDLogLevel FFI.C_DBG_NOTIFY
      Read.Ident "info"            -> return $ LDLogLevel FFI.C_DBG_INFO
      Read.Ident "debug"           -> return $ LDLogLevel FFI.C_DBG_DEBUG
      Read.Ident "spew"            -> return $ LDLogLevel FFI.C_DBG_SPEW
      x -> errorWithoutStackTrace $ "cannot parse value: " <> show x

instance Show LDLogLevel where
  show (LDLogLevel FFI.C_DBG_CRITICAL) = "critical"
  show (LDLogLevel FFI.C_DBG_ERROR)    = "error"
  show (LDLogLevel FFI.C_DBG_NOTIFY)   = "notify"
  show (LDLogLevel FFI.C_DBG_WARNING)  = "warning"
  show (LDLogLevel FFI.C_DBG_INFO)     = "info"
  show (LDLogLevel FFI.C_DBG_DEBUG)    = "debug"
  show (LDLogLevel FFI.C_DBG_SPEW)     = "spew"
  show _                               = "unknown log level"

setLogDeviceDbgLevel :: FFI.C_DBG_LEVEL -> IO ()
setLogDeviceDbgLevel = FFI.c_set_dbg_level

setLogDeviceDbgLevel' :: LDLogLevel -> IO ()
setLogDeviceDbgLevel' = setLogDeviceDbgLevel . unLDLogLevel

logDeviceDbgUseFD :: FD -> IO FD
logDeviceDbgUseFD = FFI.c_dbg_use_fd
