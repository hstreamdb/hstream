module HStream.Internal.FFI
  ( new_logdevice_client
  , free_logdevice_client
  ) where

import           Foreign.C.String (CString)
import           Foreign.Ptr      (Ptr)

#include "hs_logdevice.h"

data LogDeviceClient

foreign import ccall unsafe "hs_logdevice.h new_logdevice_client"
  new_logdevice_client :: CString -> IO (Ptr LogDeviceClient)

foreign import ccall unsafe "hs_logdevice.h free_logdevice_client"
  free_logdevice_client :: Ptr LogDeviceClient -> IO ()
