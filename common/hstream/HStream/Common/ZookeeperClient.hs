module HStream.Common.ZookeeperClient
  ( ZookeeperClient
  , withZookeeperClient
  , unsafeGetZHandle
  ) where

import           Data.Word
import           Foreign.C.Types
import           Foreign.ForeignPtr
import           Foreign.Ptr
import           Unsafe.Coerce      (unsafeCoerce)
import           Z.Data.CBytes      (CBytes, withCBytes)
import           ZooKeeper.Types    (ZHandle)

newtype ZookeeperClient = ZookeeperClient (Ptr CZookeeperClient)

withZookeeperClient :: CBytes -> CInt -> (ZookeeperClient -> IO a) -> IO a
withZookeeperClient quorum timeout f = do
  fp <- newZookeeperClient quorum timeout
  withForeignPtr fp $ f . ZookeeperClient

newZookeeperClient :: CBytes -> CInt -> IO (ForeignPtr CZookeeperClient)
newZookeeperClient quorum timeout = do
  withCBytes quorum $ \quorum' -> do
    client <- new_zookeeper_client quorum' timeout
    newForeignPtr delete_zookeeper_client client

-- It's safe to use unsafeGetZHandle in hstream since we donot free the
-- ZookeeperClient in the server lifetime.
unsafeGetZHandle :: ZookeeperClient -> IO ZHandle
unsafeGetZHandle (ZookeeperClient ptr) =
  -- TODO: let zoovisitor exports the ZHandle constructor
  --
  -- It's safe to use unsafeCoerce here because the ZHandle is a newtype.
  unsafeCoerce <$> get_underlying_handle ptr

data CZookeeperClient

foreign import ccall safe "new_zookeeper_client"
  new_zookeeper_client :: Ptr Word8 -> CInt -> IO (Ptr CZookeeperClient)

foreign import ccall unsafe "&delete_zookeeper_client"
  delete_zookeeper_client :: FunPtr (Ptr CZookeeperClient -> IO ())

foreign import ccall unsafe "get_underlying_handle"
  get_underlying_handle :: Ptr CZookeeperClient -> IO (Ptr ())
