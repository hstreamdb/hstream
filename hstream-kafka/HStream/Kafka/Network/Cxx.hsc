{-# LANGUAGE CPP          #-}
{-# LANGUAGE MagicHash    #-}

module HStream.Kafka.Network.Cxx
  ( Request (..)
  , Response (..)
  , ProcessorCallback
  , withProcessorCallback
  , ConnContext (..)
  , ConnContextCallback
  , withConnContextCallback
    --
  , new_kafka_server
  , run_kafka_server
  , stop_kafka_server
  , release_lock
    --
  , getSystemEventManager'
  , withFdEventNotification

    -- * Re-export
  , Lifetime (..)
  , Fd (..)

    -- * Internal
  , mkProcessorCallback
  , mkConnContextCallback
  ) where

import           Control.Exception
import           Control.Monad
import           Data.ByteString        (ByteString)
import           Data.ByteString.Short  (ShortByteString)
import qualified Data.ByteString.Short  as BSS
import qualified Data.ByteString.Unsafe as BS
import           Data.Word
import           Foreign.C.Error        (throwErrno)
import           Foreign.C.String
import           Foreign.C.Types
import           Foreign.Marshal
import           Foreign.Ptr
import           Foreign.StablePtr
import           Foreign.Storable
import           GHC.Event              (EventManager, Lifetime (..), evtRead,
                                         getSystemEventManager, registerFd,
                                         unregisterFd)
import           System.Posix.IO        (closeFd)
import           System.Posix.Types     (Fd (..))

#include "hs_kafka_server.h"

data CppLock

data Request = Request
  { requestPayload :: ByteString
  , requestLock    :: Ptr CppLock
  } deriving (Show)

instance Storable Request where
  sizeOf _ = (#size server_request_t)
  alignment _ = (#alignment server_request_t)
  peek ptr = do
    data_ptr <- (#peek server_request_t, data) ptr
    data_size <- (#peek server_request_t, data_size) ptr :: IO Word64
    -- NOTE: This value will have no finalizer associated with it, and will not
    -- be garbage collected by Haskell.
    --
    -- Also, the memory is live in the C++ stack.
    --
    -- If the original CStringLen is later modified, this change will be
    -- reflected in the resulting ByteString. However, we'll never modify the
    -- original CStringLen, so this is fine.
    --
    -- BS.unsafePackCStringLen (nullPtr, 0) === ""
    payload <- BS.unsafePackCStringLen (data_ptr, fromIntegral data_size)
    lock <- (#peek server_request_t, lock) ptr
    return $ Request{ requestPayload = payload
                    , requestLock = lock
                    }
  poke _ptr _req = error "Request is not pokeable"

data Response = Response
  { responseData :: Maybe ByteString
  } deriving (Show)

instance Storable Response where
  sizeOf _ = (#size server_response_t)
  alignment _ = (#alignment server_response_t)
  peek ptr = do
    data_ptr <- (#peek server_response_t, data) ptr
    data_size <- (#peek server_response_t, data_size) ptr :: IO Word64
    payload <- if data_ptr == nullPtr
                  then pure Nothing
                  else Just <$> BS.unsafePackCStringLen (data_ptr, fromIntegral data_size)
    return $ Response{ responseData = payload
                     }
  poke ptr Response{..} = do
    (data_ptr, data_size) <- mallocFromMaybeByteString responseData
    (#poke server_response_t, data) ptr data_ptr
    (#poke server_response_t, data_size) ptr data_size

data ConnContext = ConnContext
  { peerHost :: ShortByteString
  } deriving (Show)

instance Storable ConnContext where
  sizeOf _ = (#size conn_context_t)
  alignment _ = (#alignment conn_context_t)
  peek ptr = do
    peer_host_ptr <- (#peek conn_context_t, peer_host) ptr
    peer_host_ptr_size <- (#peek conn_context_t, peer_host_size) ptr :: IO Word64
    -- NOTE: This need to do a copy, because the original memory is live in the
    -- C++ stack, and we will access it later.
    peerHost <- BSS.packCStringLen (peer_host_ptr, fromIntegral peer_host_ptr_size)
    return $ ConnContext{ peerHost = peerHost
                        }
  poke _ptr _req = error "ConnContext is not pokeable"

type HsConnCtx a = StablePtr (a, ConnContext)

type ProcessorCallback a = HsConnCtx a -> Ptr Request -> Ptr Response -> IO ()

foreign import ccall "wrapper"
  mkProcessorCallback
    :: ProcessorCallback a -> IO (FunPtr (ProcessorCallback a))

withProcessorCallback
  :: ProcessorCallback a
  -> (FunPtr (ProcessorCallback a) -> IO b)
  -> IO b
withProcessorCallback cb = bracket (mkProcessorCallback cb) freeHaskellFunPtr

type ConnContextCallback a = Ptr ConnContext -> IO (HsConnCtx a)

foreign import ccall "wrapper"
  mkConnContextCallback
    :: ConnContextCallback a -> IO (FunPtr (ConnContextCallback a))

withConnContextCallback
  :: ConnContextCallback a
  -> (FunPtr (ConnContextCallback a) -> IO b)
  -> IO b
withConnContextCallback cb =
  bracket (mkConnContextCallback cb) freeHaskellFunPtr

-------------------------------------------------------------------------------

data CppKafkaServer

foreign import ccall unsafe "new_kafka_server"
  new_kafka_server :: IO (Ptr CppKafkaServer)

foreign import ccall safe "run_kafka_server"
  run_kafka_server
    :: Ptr CppKafkaServer
    -> Ptr CChar
    -- ^ host
    -> Word16
    -- ^ port
    -> FunPtr (ProcessorCallback a)
    -- ^ haskell handler
    -> FunPtr (ConnContextCallback a)
    -- ^ haskell context
    -> CInt  -- ^ fd onStarted
    -> IO ()

foreign import ccall safe "stop_kafka_server"
  stop_kafka_server :: Ptr CppKafkaServer -> IO ()

foreign import ccall unsafe "release_lock"
  release_lock :: Ptr CppLock -> IO ()

-------------------------------------------------------------------------------
-- Copy from foreign: HsForeign.String
--
-- TODO: import from HsForeign.String

mallocFromMaybeByteString :: Maybe ByteString -> IO (CString, Int)
mallocFromMaybeByteString (Just bs) = mallocFromByteString bs
mallocFromMaybeByteString Nothing   = return (nullPtr, 0)
{-# INLINE mallocFromMaybeByteString #-}

mallocFromByteString :: ByteString -> IO (CString, Int)
mallocFromByteString bs =
  BS.unsafeUseAsCStringLen bs $ \(src, len) -> do
    buf <- mallocBytes len
    copyBytes buf src len
    return (buf, len)
{-# INLINE mallocFromByteString #-}

-------------------------------------------------------------------------------
-- Copy from hs-grpc: HsGrpc.Common.Utils
--
-- TODO: import from

-- Exception to throw when unable to get the event manager
getSystemEventManager' :: Exception e => e -> IO EventManager
getSystemEventManager' err = maybe (throw err) return =<< getSystemEventManager

-- | Uses a file descriptor and GHC's event manager to run a callback
-- once the file descriptor is written to. Is less expensive than a new FFI
-- call from C++ back into Haskell.
--
-- NOTE: if you want to put the Fd to haskell ffi, it should be a safe ffi.
withFdEventNotification
  :: EventManager
  -> Maybe (IO ())  -- ^ The callback to run on fd write
  -> Lifetime       -- ^ OneShot or MultiShot
  -> (Fd -> IO a)   -- ^ Action to run with the file descriptor to write to
  -> IO a
withFdEventNotification _ Nothing _ action = action (Fd (-1))
withFdEventNotification evm (Just callback) lifetime action =
  withEventFd $ \fd -> do
    bracket (registerFd evm (\_ _ -> callback) fd evtRead lifetime)
            (unregisterFd evm)
            (const $ action fd)

withEventFd :: (Fd -> IO a) -> IO a
withEventFd = bracket
  (do fd <- c_eventfd 0 0
      when (fd == -1) $ throwErrno "eventFd"
      return $ Fd fd)
  closeFd

foreign import ccall unsafe "eventfd"
  c_eventfd :: CInt -> CInt -> IO CInt
