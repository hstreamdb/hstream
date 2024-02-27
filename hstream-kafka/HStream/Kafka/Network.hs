{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}
-- Indeed, some constraints are needed but ghc thinks not.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module HStream.Kafka.Network
  ( -- * Server
    ServerOptions (..)
  , SaslOptions (..)
  , defaultServerOpts
  , runHsServer
  , runCppServer

    -- * Client
  , ClientOptions (..)
  , defaultClientOptions
  , ClientHandler
  , withClient
  , sendAndRecv
  ) where

import           Control.Concurrent
import qualified Control.Concurrent.Async          as Async
import qualified Control.Exception                 as E
import           Control.Monad
import           Control.Monad.IO.Class            (liftIO)
import qualified Control.Monad.State               as State
import           Data.ByteString                   (ByteString)
import qualified Data.ByteString                   as BS
import qualified Data.ByteString.Lazy              as BL
import           Data.Int
import           Data.List                         (find, intersperse)
import           Data.Maybe                        (fromMaybe, isJust,
                                                    isNothing)
import qualified Data.Text                         as Text
import           Data.Word
import           Foreign.C.String                  (newCString)
import           Foreign.Ptr                       (nullPtr)
import           Foreign.StablePtr                 (castStablePtrToPtr,
                                                    deRefStablePtr,
                                                    newStablePtr)
import           Foreign.Storable                  (Storable (..))
import qualified Network.Socket                    as N
import qualified Network.Socket.ByteString         as N
import qualified Network.Socket.ByteString.Lazy    as NL
import           Numeric                           (showHex, showInt)

import qualified HStream.Kafka.Common.Metrics      as M
import qualified HStream.Kafka.Network.Cxx         as Cxx
import qualified HStream.Kafka.Network.IO          as KIO
import qualified HStream.Kafka.Network.Security    as Security
import           HStream.Kafka.Server.Config.Types (SaslOptions (..))
import           HStream.Kafka.Server.Types        (ServerContext (..),
                                                    initConnectionContext)
import qualified HStream.Logger                    as Log
import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message
import           Kafka.Protocol.Service

-------------------------------------------------------------------------------
-- Server

-- TODO
data SslOptions

data ServerOptions = ServerOptions
  { serverHost        :: !String
  , serverPort        :: !Word16
  , serverSslOptions  :: !(Maybe SslOptions)
  , serverSaslOptions :: !(Maybe SaslOptions)
  , serverOnStarted   :: !(Maybe (IO ()))
  , ioContextPoolSize :: !Word64
    -- ^ The number of io_contexts in the pool. The default value is 1.
    -- Only for c++ server.
  }

defaultServerOpts :: ServerOptions
defaultServerOpts = ServerOptions
  { serverHost        = "0.0.0.0"
  , serverPort        = 9092
  , serverSslOptions  = Nothing
  , serverSaslOptions = Nothing
  , serverOnStarted   = Nothing
  , ioContextPoolSize = 1
  }

-------------------------------------------------------------------------------
-- Haskell Server

-- TODO: This server primarily serves as a demonstration, and there is
-- certainly room for enhancements and refinements.
runHsServer
  :: ServerOptions
  -> ServerContext
  -> (ServerContext -> [ServiceHandler])
  -> (ServerContext -> [ServiceHandler])
  -> IO ()
runHsServer opts sc_ mkPreAuthedHandlers mkAuthedHandlers =
  startTCPServer opts $ \(s, peer) -> do
    sc <- initConnectionContext sc_
    -- Decide if we require SASL authentication
    case (serverSaslOptions opts) of
      Nothing -> do
        void $ State.execStateT (talk (peer, mkAuthedHandlers sc) s) ""
      Just _  -> do
        void $ (`State.execStateT` "") $ do
          doAuth sc peer s >>= \case
            Security.SaslStateComplete ->
              talk (peer, mkAuthedHandlers sc) s
            ss -> do
              liftIO $ Log.fatal $ "[SASL] authenticate failed with state " <> Log.buildString' ss
  where
    doAuth sc peer s = do
      let recv = KIO.recvKafkaMsgBS peer Nothing s
          send = KIO.sendKafkaMsgBS s
      Security.authenticate sc
                            (runHandler peer (mkPreAuthedHandlers sc))
                            recv
                            send
                            Security.SaslStateHandshakeOrVersions
                            Nothing

    talk (peer, hds) s = do
      KIO.recvKafkaMsgBS peer Nothing s >>= \case
        Nothing -> return ()
        Just (reqHeader, reqBs) -> do
          respBs <- liftIO $ runHandler peer hds reqHeader reqBs
          liftIO $ KIO.sendKafkaMsgBS s respBs
          talk (peer, hds) s

    runHandler peer handlers reqHeader@RequestHeader{..} reqBs = do
      Log.debug $ "Received request header: " <> Log.buildString' reqHeader
      M.incCounter M.totalRequests
      let ServiceHandler{..} = findHandler handlers requestApiKey requestApiVersion
      case rpcHandler of
        UnaryHandler rpcHandler' -> do
          M.observeWithLabel
            M.handlerLatencies
            (Text.pack $ show requestApiKey) $
              doUnaryHandler reqBs reqHeader rpcHandler' peer

    doUnaryHandler l reqHeader@RequestHeader{..} rpcHandler' peer = do
      (req, left) <- runGet' l
      when (not . BS.null $ left) $
        Log.warning $ "Leftover bytes: " <> Log.buildString' left
      Log.debug $ "Received request "
               <> Log.buildString' requestApiKey
               <> ":v" <> Log.build requestApiVersion
               <> " from " <> Log.buildString' peer
               <> ", payload: " <> Log.buildString' req
      let reqContext =
            RequestContext
              { clientId = requestClientId
              , clientHost = showSockAddrHost peer
              , apiVersion = requestApiVersion
              }
      resp <- rpcHandler' reqContext req
      Log.debug $ "Server response: " <> Log.buildString' resp
      return $ KIO.packKafkaMsgBs reqHeader resp

startTCPServer :: ServerOptions -> ((N.Socket, N.SockAddr) -> IO a) -> IO a
startTCPServer ServerOptions{..} server = do
  addr <- resolve
  E.bracket (open addr) N.close loop
  where
    resolve = do
      let hints = N.defaultHints
            { N.addrFlags = [N.AI_PASSIVE]
            , N.addrSocketType = N.Stream
            }
      head <$> N.getAddrInfo (Just hints) (Just serverHost) (Just $ show serverPort)
    open addr = E.bracketOnError (N.openSocket addr) N.close $ \sock -> do
      N.setSocketOption sock N.ReuseAddr 1
      N.withFdSocket sock N.setCloseOnExecIfNeeded
      N.bind sock $ N.addrAddress addr
      N.listen sock 1024
      case serverOnStarted of
        Just onStarted -> onStarted >> pure sock
        Nothing        -> pure sock
    loop sock = forever $ E.bracketOnError (N.accept sock) (N.close . fst)
      $ \(conn, peer) -> void $ do
        -- 'forkFinally' alone is unlikely to fail thus leaking @conn@,
        -- but 'E.bracketOnError' above will be necessary if some
        -- non-atomic setups (e.g. spawning a subprocess to handle
        -- @conn@) before proper cleanup of @conn@ is your case
        forkFinally (server (conn, peer)) $ \e -> do
          case e of
            Left err -> do Log.fatal (Log.buildString' err)
                           N.gracefulClose conn 5000
            Right _  -> pure ()

-------------------------------------------------------------------------------
-- C++ Server

-- TODO:
--
-- - PreAuthedHandlers
runCppServer
  :: ServerOptions
  -> ServerContext
  -> (ServerContext -> [ServiceHandler])
  -> IO ()
runCppServer opts sc_ mkAuthedHandlers =
  Cxx.withProcessorCallback processorCallback $ \cb ->
  Cxx.withConnContextCallback newConnectionContext $ \connCb -> do
    -- FIXME: does userError make sense here?
    evm <- Cxx.getSystemEventManager' $ userError "failed to get event manager"
    Cxx.withFdEventNotification evm opts.serverOnStarted Cxx.OneShot $
      \(Cxx.Fd cfdOnStarted) -> do
        hostPtr <- newCString opts.serverHost  -- Freed by C++ code
        -- FIXME: we donot delete the memory here(it's OK since the process will
        -- exit soon).
        -- Because after the server(by Cxx.new_kafka_server) is deleted the
        -- 'io_context' inside it will be invalid. And there are potential
        -- haskell threads that are still using it. For example, the
        -- 'Cxx.release_lock' in 'processorCallback'. Which will cause a crash.
        server <- Cxx.new_kafka_server (fromIntegral opts.ioContextPoolSize)
        let start = Cxx.run_kafka_server server hostPtr opts.serverPort
                                         cb connCb cfdOnStarted
            stop a = Cxx.stop_kafka_server server >> Async.wait a
         in E.bracket (Async.async start) stop Async.wait
  where
    newConnectionContext conn_ctx_ptr = do
      -- Cpp per-connection context
      conn <- peek conn_ctx_ptr
      -- Haskell per-connection context
      sc <- initConnectionContext sc_
      newStablePtr (sc, conn)  -- Freed by C++ code

    processorCallback :: Cxx.ProcessorCallback ServerContext
    processorCallback sptr request_ptr response_ptr =
      -- nullPtr means client closed the connection
      when (castStablePtrToPtr sptr /= nullPtr) $ void $ forkIO $ do
        (sc, conn) <- deRefStablePtr sptr
        let handlers = mkAuthedHandlers sc
        req <- peek request_ptr
        -- respBs: Nothing means some error occurred, and the server will close
        -- the connection
        respBs <- E.catch
          (Just <$> handleKafkaMsg conn handlers req.requestPayload)
          (\err -> do Log.fatal $ Log.buildString' (err :: E.SomeException)
                      pure Nothing)
        poke response_ptr Cxx.Response{responseData = respBs}
        Cxx.release_lock req.requestLock

-------------------------------------------------------------------------------
-- Server misc

handleKafkaMsg
  :: Cxx.ConnContext -> [ServiceHandler] -> ByteString -> IO ByteString
handleKafkaMsg conn handlers bs = do
  (header, reqBs) <- runGet' @RequestHeader bs
  let ServiceHandler{..} = findHandler handlers header.requestApiKey header.requestApiVersion
  case rpcHandler of
    UnaryHandler rpcHandler' -> do
      (req, left) <- runGet' reqBs
      Log.debug $ "Received request "
               <> Log.buildString' header.requestApiKey
               <> ":v" <> Log.build header.requestApiVersion
               <> " from " <> Log.buildString' conn.peerHost
               <> ", payload: " <> Log.buildString' req
      when (not . BS.null $ left) $
        Log.warning $ "Leftover bytes: " <> Log.buildString' left
      let reqContext =
            RequestContext
              { clientId = header.requestClientId
              , clientHost = show conn.peerHost
              , apiVersion = header.requestApiVersion
              }
      resp <- rpcHandler' reqContext req
      Log.debug $ "Server response: " <> Log.buildString' resp
      let (_, respHeaderVer) = getHeaderVersion header.requestApiKey header.requestApiVersion
          respHeaderBuilder =
            case respHeaderVer of
              0 -> putResponseHeader $ ResponseHeader header.requestCorrelationId Nothing
              1 -> putResponseHeader $ ResponseHeader header.requestCorrelationId (Just EmptyTaggedFields)
              _ -> error $ "Unknown response header version " <> show respHeaderVer
      pure $ BL.toStrict $ toLazyByteString $ respHeaderBuilder <> put resp

findHandler :: [ServiceHandler] -> ApiKey -> Int16 -> ServiceHandler
findHandler handlers apikey@(ApiKey key) version = do
  let m_handler = find (\ServiceHandler{..} ->
        rpcMethod == (key, version)) handlers
      errmsg = "NotImplemented: " <> show apikey <> ":v" <> show version
  fromMaybe (error errmsg) m_handler
{-# INLINE findHandler #-}

-------------------------------------------------------------------------------
-- Client

data ClientOptions = ClientOptions
  { host         :: !String
  , port         :: !Word16
  , maxRecvBytes :: !Int
  } deriving (Show)

defaultClientOptions :: ClientOptions
defaultClientOptions = ClientOptions
  { host = "127.0.0.1"
  , port = 9092
  , maxRecvBytes = 1024
  }

data ClientHandler = ClientHandler
  { socket       :: !N.Socket
  , maxRecvBytes :: !Int
  }

-- TODO: There is certainly room for enhancements and refinements.
--
-- e.g.
--
-- one thread reads data from a Socket only and the other thread writes
-- data to the Socket only
sendAndRecv
  :: (HasMethod s m, MethodInput s m ~ i, MethodOutput s m ~ o, Show i, Show o)
  => (RPC s m)
  -> Int32 -> (Maybe NullableString) -> (Maybe TaggedFields)
  -> i
  -> ClientHandler
  -> IO (ResponseHeader, o)
sendAndRecv rpc correlationId mClientId mTaggedFields req ClientHandler{..} = do
  let (apiKey, apiVersion) = getRpcMethod rpc
      (reqHeaderVer, respHeaderVer) = getHeaderVersion (ApiKey apiKey) apiVersion
  -- Request
  let reqHeader =
        case reqHeaderVer of
          2 -> if isJust mClientId && isJust mTaggedFields
                  then RequestHeader (ApiKey apiKey) apiVersion
                                     correlationId mClientId mTaggedFields
                  else error "mClientId and mTaggedFields must be Just"
          1 -> if isJust mClientId && isNothing mTaggedFields
                  then RequestHeader (ApiKey apiKey) apiVersion
                                     correlationId mClientId Nothing
                  else error "(mClientId, mTaggedFields) must be (Just, Nothing)"
          0 -> if isNothing mClientId && isNothing mTaggedFields
                  then RequestHeader (ApiKey apiKey) apiVersion
                                     correlationId Nothing Nothing
                  else error "(mClientId, mTaggedFields) must be (Nothing, Nothing)"
          _ -> error $ "Unknown request header version " <> show reqHeaderVer
      reqBs = runPut reqHeader <> runPut req
      reqBs' = runPutLazy @ByteString reqBs

  NL.sendAll socket reqBs'

  let respBsParser = do respLen <- get @Int32
                        takeBytes (fromIntegral respLen)
      respParser = do respHeader <- getResponseHeader respHeaderVer
                      respBody <- get
                      pure (respHeader, respBody)

  result <- runParseIO (N.recv socket maxRecvBytes) respBsParser
  case result of
    -- FIXME: we only send one request per time, so we should not get More here
    (bs, "") -> do r <- runParser' respParser bs
                   case r of
                     (resp, "") -> pure resp
                     _          -> error "FIXME: unexpected result"
    _ -> error "FIXME: unexpected result"

withClient :: ClientOptions -> (ClientHandler -> IO a) -> IO a
withClient ClientOptions{..} client = N.withSocketsDo $ do
  addr <- resolve
  E.bracket (open addr) N.close $ \sock ->
    client (ClientHandler sock maxRecvBytes)
  where
    resolve = do
      let hints = N.defaultHints{ N.addrSocketType = N.Stream }
      head <$> N.getAddrInfo (Just hints) (Just host) (Just $ show port)
    open addr = E.bracketOnError (N.openSocket addr) N.close $ \sock -> do
      N.connect sock $ N.addrAddress addr
      return sock

-------------------------------------------------------------------------------

-- TODO: move to 'Kafka.Protocol.Encoding'
runParseIO :: IO ByteString -> Parser a -> IO (a, ByteString)
runParseIO more parser = more >>= go Nothing
  where
    go _ "" = error "EOF"  -- TODO
    go mf bs = do
      result <- case mf of
                  Nothing    -> runParser parser bs
                  Just moref -> moref bs
      case result of
        Done l r   -> pure (r, l)
        More f     -> do msg <- more
                         go (Just f) msg
        Fail _ err -> E.throwIO $ DecodeError $ "Fail, " <> err

showSockAddrHost :: N.SockAddr -> String
showSockAddrHost (N.SockAddrUnix str)            = str
showSockAddrHost (N.SockAddrInet _port ha)       = showHostAddress ha ""
showSockAddrHost (N.SockAddrInet6 _port _ ha6 _) = showHostAddress6 ha6 ""

-- Taken from network Network/Socket/Info.hsc
showHostAddress :: N.HostAddress -> ShowS
showHostAddress ip =
  let (u3, u2, u1, u0) = N.hostAddressToTuple ip in
  foldr1 (.) . intersperse (showChar '.') $ map showInt [u3, u2, u1, u0]

-- Taken from network Network/Socket/Info.hsc
showHostAddress6 :: N.HostAddress6 -> ShowS
showHostAddress6 ha6@(a1, a2, a3, a4)
    -- IPv4-Mapped IPv6 Address
    | a1 == 0 && a2 == 0 && a3 == 0xffff =
      showString "::ffff:" . showHostAddress a4
    -- IPv4-Compatible IPv6 Address (exclude IPRange ::/112)
    | a1 == 0 && a2 == 0 && a3 == 0 && a4 >= 0x10000 =
        showString "::" . showHostAddress a4
    -- length of longest run > 1, replace it with "::"
    | end - begin > 1 =
        showFields prefix . showString "::" . showFields suffix
    | otherwise =
        showFields fields
  where
    fields =
        let (u7, u6, u5, u4, u3, u2, u1, u0) = N.hostAddress6ToTuple ha6 in
        [u7, u6, u5, u4, u3, u2, u1, u0]
    showFields = foldr (.) id . intersperse (showChar ':') . map showHex
    prefix = take begin fields  -- fields before "::"
    suffix = drop end fields    -- fields after "::"
    begin = end + diff          -- the longest run of zeros
    (diff, end) = minimum $
        scanl (\c i -> if i == 0 then c - 1 else 0) 0 fields `zip` [0..]
