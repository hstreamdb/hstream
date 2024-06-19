module HStream.Store.Internal.LogDevice.HealthCheck
 ( LdChecker
 , newLdChecker
 , isLdClusterHealthy
 )
where

import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Ptr

import           HStream.Store.Internal.Foreign
import           HStream.Store.Internal.Types

type LdChecker = ForeignPtr Checker

newLdChecker :: LDClient -> IO LdChecker
newLdChecker client = do
  checker <- withForeignPtr client $ \client' -> new_ld_checker client'
  newForeignPtr delete_ld_checker checker

-- Check the health status of the cluster.
-- If the total number of unhealthy nodes is greater than the specified limit,
-- it means that the cluster is not available and will return False.
isLdClusterHealthy
  :: LdChecker
  -> Int
  -- ^ The upper limit of unhealthy nodes
  -> IO Bool
  -- ^ Return True if the cluster is healthy, otherwise False
isLdClusterHealthy checker unhealthyLimit = withForeignPtr checker $ \c -> do
   cbool2bool <$> ld_checker_check c (fromIntegral unhealthyLimit)

data Checker

foreign import ccall unsafe "new_ld_checker"
  new_ld_checker :: Ptr LogDeviceClient -> IO (Ptr Checker)

foreign import ccall unsafe "&delete_ld_checker"
  delete_ld_checker :: FunPtr (Ptr Checker -> IO ())

foreign import ccall unsafe "ld_checker_check"
  ld_checker_check :: Ptr Checker -> CInt -> IO CBool
