{-# LANGUAGE MagicHash #-}

module HStream.Store.Internal.LogDevice.Configuration where

import           Data.Word
import           Foreign.C.Types
import           Foreign.Ptr
import           Z.Data.CBytes                  (CBytes)
import qualified Z.Data.CBytes                  as CBytes
import qualified Z.Foreign                      as Z

import           HStream.Store.Internal.Foreign

isInternalLog :: Word64 -> Bool
isInternalLog = cbool2bool . c_isInternalLog

getInternalLogName :: Word64 -> IO CBytes
getInternalLogName = CBytes.fromStdString . c_getInternalLogName

foreign import ccall unsafe "hs_logdevice.h isInternalLog"
  c_isInternalLog :: Word64 -> CBool

foreign import ccall unsafe "hs_logdevice.h getInternalLogName"
  c_getInternalLogName :: Word64 -> IO (Ptr Z.StdString)
