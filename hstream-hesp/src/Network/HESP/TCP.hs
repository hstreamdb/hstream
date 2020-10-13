{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Network.HESP.TCP
  ( -- * Server
    runTCPServer
  , runTCPServerG
  , runTCPServer'
  , runTCPServerG'
    -- * Client
  , connect

    -- * Send & Recv messages
  , recvMsgs
  , sendMsg
  , sendMsgs

    -- * Utils
  , send
  , sendLazy
  , close
  , gracefulClose
  , setDefaultServerSO
  , setDefaultClientSO

    -- * Connection
  , createTcpConnectionPool
  , withTcpConnection
  , simpleCreateTcpConnPool
  ) where

import           Control.Concurrent             (forkFinally)
import qualified Control.Concurrent.Lifted      as L
import           Control.Exception              (SomeException, bracket)
import qualified Control.Exception.Safe         as Ex
import           Control.Monad                  (forever, void)
import           Control.Monad.IO.Class         (MonadIO, liftIO)
import           Control.Monad.Trans.Control    (MonadBaseControl, liftBaseOp)
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Lazy           as LBS
import qualified Data.ByteString.Lazy.Internal  as LBS
import           Data.Pool                      (Pool)
import qualified Data.Pool                      as Pool
import           Data.Time                      (NominalDiffTime)
import           Data.Vector                    (Vector)
import           Network.Socket                 (SockAddr, Socket)
import qualified Network.Socket                 as NS
import qualified Network.Socket.ByteString      as NS
import qualified Network.Socket.ByteString.Lazy as NSL
import qualified System.IO                      as IO

import           Network.HESP.Protocol          (deserializeWithMaybe,
                                                 serialize)
import qualified Network.HESP.Types             as T

-------------------------------------------------------------------------------

runTCPServer
  :: NS.HostName
  -> NS.ServiceName
  -> ((Socket, SockAddr) -> IO a)
  -> IO b
runTCPServer host port =
  let x = "Network.HESP.TCP.runTCPServer: Synchronous exception happened: "
   in runTCPServer' host port setDefaultServerSO (uncurry . flip $ clean x)

runTCPServer'
  :: NS.HostName
  -> NS.ServiceName
  -> (Socket -> IO ()) -- ^ set socket options
  -> ((Either SomeException a, Socket) -> IO ())
  -> ((Socket, SockAddr) -> IO a)
  -> IO b
runTCPServer' host port setOpts release server = do
  addr <- resolveServer host port
  bracket (openServer addr setOpts) close (acceptConc server release)

-- | Generalized version of 'runTCPServer'.
runTCPServerG
  :: (MonadBaseControl IO m, MonadIO m)
  => NS.HostName
  -> NS.ServiceName
  -> ((Socket, SockAddr) -> m ())
  -> m a
runTCPServerG host port =
  let x = "Network.HESP.TCP.runTCPServerG: Synchronous exception happened: "
      clean' = liftIO . uncurry (flip $ clean x)
   in runTCPServerG' host port setDefaultServerSO clean'

runTCPServerG'
  :: (MonadBaseControl IO m, MonadIO m)
  => NS.HostName
  -> NS.ServiceName
  -> (Socket -> IO ())   -- ^ set socket options
  -> ((Either SomeException (), Socket) -> m ())
  -> ((Socket, SockAddr) -> m ())
  -> m a
runTCPServerG' host port setOpts release server = do
  addr <- resolveServer host port
  gbracket (openServer addr setOpts) close (acceptConc' server release)

connect
  :: (MonadIO m, Ex.MonadMask m)
  => NS.HostName      -- ^ Server hostname or IP address.
  -> NS.ServiceName   -- ^ Server service port name or number.
  -> ((NS.Socket, NS.SockAddr) -> m r)
  -- ^ Computation taking the communication socket and the server address.
  -> m r
connect host port = Ex.bracket (connectSock host port) (close . fst)

-- FIXME: more elegantly
recvMsgs :: MonadIO m => Socket -> Int -> m (Vector (Either String T.Message))
recvMsgs sock bytes = deserializeWithMaybe (recv sock bytes) ""

sendMsg :: MonadIO m => Socket -> T.Message -> m ()
sendMsg sock = send sock . serialize

sendMsgs :: (MonadIO m, Traversable t) => Socket -> t T.Message -> m ()
sendMsgs sock = sendLazy sock . fromChunks . fmap serialize

simpleCreateTcpConnPool
  :: NS.HostName
  -> NS.ServiceName
  -> IO (Pool (Socket, SockAddr))
simpleCreateTcpConnPool h p = createTcpConnectionPool h p 1 10 20

createTcpConnectionPool
  :: NS.HostName
  -> NS.ServiceName
  -> Int
  -- ^ The number of stripes (distinct sub-pools) to maintain.
  -- The smallest acceptable value is 1.
  -> NominalDiffTime
  -- ^ Amount of time for which an unused resource is kept open.
  -- The smallest acceptable value is 0.5 seconds.
  --
  -- The elapsed time before destroying a resource may be a little longer
  -- than requested, as the reaper thread wakes at 1-second intervals.
  -> Int
  -- ^ Maximum number of resources to keep open per stripe.
  -- The smallest acceptable value is 1.
  --
  -- Requests for resources will block if this limit is reached on a
  -- single stripe, even if other stripes have idle resources available.
  -> IO (Pool (Socket, SockAddr))
createTcpConnectionPool host port =
  let r = connectSock host port
   in Pool.createPool r (close . fst)

withTcpConnection
  :: MonadBaseControl IO m
  => Pool (Socket, SockAddr)
  -> ((Socket, SockAddr) -> m a)
  -> m a
withTcpConnection = Pool.withResource

-------------------------------------------------------------------------------

connectSock :: MonadIO m => NS.HostName -> NS.ServiceName -> m (Socket, SockAddr)
connectSock host port = do
  addr <- resolveClient host port
  sock <- openClient addr setDefaultClientSO
  return (sock, NS.addrAddress addr)

acceptConc
  :: ((Socket, SockAddr) -> IO a)
  -> ((Either SomeException a, Socket) -> IO ())
  -> Socket
  -> IO b
acceptConc server release sock = forever $ do
  (conn, peer) <- NS.accept sock
  void $ forkFinally (server (conn, peer)) (\r -> release (r, conn))

-- | Generalized version of 'acceptConc'.
acceptConc'
  :: (MonadBaseControl IO m, MonadIO m)
  => ((Socket, SockAddr) -> m ())
  -> ((Either SomeException (), Socket) -> m ())
  -> Socket
  -> m a
acceptConc' server release sock = forever $ do
  (conn, peer) <- liftIO $ NS.accept sock
  void $ L.forkFinally (server (conn, peer)) (\r -> release (r, conn))

resolveServer :: MonadIO m => NS.HostName -> NS.ServiceName -> m NS.AddrInfo
resolveServer host port =
  let hints = NS.defaultHints { NS.addrFlags = [NS.AI_PASSIVE]
                              , NS.addrSocketType = NS.Stream
                              }
   in liftIO $ head <$> NS.getAddrInfo (Just hints) (Just host) (Just port)

resolveClient :: MonadIO m => NS.HostName -> NS.ServiceName -> m NS.AddrInfo
resolveClient host port =
  let hints = NS.defaultHints { NS.addrFlags = [NS.AI_ADDRCONFIG]
                              , NS.addrSocketType = NS.Stream
                              }
   in liftIO $ head <$> NS.getAddrInfo (Just hints) (Just host) (Just port)

openServer :: MonadIO m => NS.AddrInfo -> (Socket -> IO ()) -> m Socket
openServer NS.AddrInfo{..} setSocketOption = liftIO $ do
  sock <- NS.socket addrFamily addrSocketType addrProtocol
  setSocketOption sock
  NS.bind sock addrAddress
  NS.listen sock (max 2048 NS.maxListenQueue)
  return sock

openClient :: MonadIO m => NS.AddrInfo -> (Socket -> IO ()) -> m Socket
openClient NS.AddrInfo{..} setSocketOption = liftIO $ do
  sock <- NS.socket addrFamily addrSocketType addrProtocol
  setSocketOption sock
  NS.connect sock addrAddress
  return sock

send :: MonadIO m => Socket -> BS.ByteString -> m ()
send sock bytes = liftIO $ NS.sendAll sock bytes
{-# INLINABLE send #-}

sendLazy :: MonadIO m => Socket -> LBS.ByteString -> m ()
sendLazy sock lbytes = liftIO $ NSL.sendAll sock lbytes
{-# INLINABLE sendLazy #-}

recv :: MonadIO m => Socket -> Int -> m (Maybe BS.ByteString)
recv sock nbytes = liftIO $ do
  bs <- liftIO (NS.recv sock nbytes)
  if BS.null bs then return Nothing else return (Just bs)
{-# INLINABLE recv #-}

-- | Shuts down and closes the 'Socket', silently ignoring any synchronous
-- exception that might happen.
close :: MonadIO m => Socket -> m ()
close s = liftIO $
  Ex.catch (Ex.finally (NS.shutdown s NS.ShutdownBoth)
                       (NS.close s))
           (\(_ :: Ex.SomeException) -> pure ())

gracefulClose :: MonadIO m => Socket -> m ()
gracefulClose conn = liftIO $ NS.gracefulClose conn 5000

-- | Graceful close the 'Socket' with printing errors if 'SomeException'
-- happened.
clean :: String -> Socket -> Either SomeException a -> IO ()
clean label lsock = \case
  Left e  -> err e >> gracefulClose lsock
  Right _ -> gracefulClose lsock
  where
    err :: SomeException -> IO ()
    err e = IO.hPutStrLn IO.stderr (label ++ show e)

setDefaultServerSO :: Socket -> IO ()
setDefaultServerSO sock = do
  NS.setSocketOption sock NS.ReuseAddr 1
  NS.setSocketOption sock NS.NoDelay 1
  NS.setSocketOption sock NS.KeepAlive 1
  NS.withFdSocket sock NS.setCloseOnExecIfNeeded

setDefaultClientSO :: Socket -> IO ()
setDefaultClientSO sock = do
  NS.setSocketOption sock NS.NoDelay 1
  NS.setSocketOption sock NS.KeepAlive 1

-------------------------------------------------------------------------------

-- | /O(c)/ Convert a bunch of strict 'ByteString' into a lazy 'ByteString'.
fromChunks :: Foldable t => t BS.ByteString -> LBS.ByteString
fromChunks = foldr LBS.chunk LBS.Empty
{-# INLINABLE fromChunks #-}

gbracket :: MonadBaseControl IO m => IO a -> (a -> IO b) -> (a -> m c) -> m c
gbracket acquire release = liftBaseOp (bracket acquire release)
{-# INLINABLE gbracket #-}
