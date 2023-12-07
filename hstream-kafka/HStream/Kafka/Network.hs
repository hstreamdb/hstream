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
  , runServer
    -- * Client
  , ClientOptions (..)
  , defaultClientOptions
  , ClientHandler
  , withClient
  , sendAndRecv
  ) where

import           Control.Concurrent
import qualified Control.Exception                   as E
import           Control.Monad
import           Control.Monad.IO.Class              (liftIO)
import qualified Control.Monad.State                 as State
import           Data.ByteString                     (ByteString)
import qualified Data.ByteString                     as BS
import           Data.Int
import           Data.List                           (find, intersperse)
import           Data.Maybe                          (fromMaybe, isJust,
                                                      isNothing)
import qualified Data.Text                           as T
import qualified Network.Socket                      as N
import qualified Network.Socket.ByteString           as N
import qualified Network.Socket.ByteString.Lazy      as NL
import           Numeric                             (showHex, showInt)
import qualified Prometheus                          as P

import           HStream.Kafka.Common.KafkaException (ErrorCodeException (..))
import           HStream.Kafka.Common.OffsetManager  (initOffsetReader)
import           HStream.Kafka.Common.Utils          (observeWithLabel)
import           HStream.Kafka.Metrics.ServerStats   (handlerLatencies,
                                                      totalRequests)
import qualified HStream.Kafka.Network.IO            as KIO
import qualified HStream.Kafka.Network.Security      as Security
import           HStream.Kafka.Server.Config.Types   (SaslOptions (..))
import           HStream.Kafka.Server.Types          (ServerContext (..))
import qualified HStream.Logger                      as Log
import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message
import           Kafka.Protocol.Service

-------------------------------------------------------------------------------
-- Server

-- TODO
data SslOptions

data ServerOptions = ServerOptions
  { serverHost        :: !String
  , serverPort        :: !Int
  , serverSslOptions  :: !(Maybe SslOptions)
  , serverSaslOptions :: !(Maybe SaslOptions)
  , serverOnStarted   :: !(Maybe (IO ()))
  }

defaultServerOpts :: ServerOptions
defaultServerOpts = ServerOptions
  { serverHost           = "0.0.0.0"
  , serverPort           = 9092
  , serverSslOptions     = Nothing
  , serverSaslOptions    = Nothing
  , serverOnStarted      = Nothing
  }

-- TODO: This server primarily serves as a demonstration, and there is
-- certainly room for enhancements and refinements.
runServer
  :: ServerOptions
  -> ServerContext
  -> (ServerContext -> [ServiceHandler])
  -> (ServerContext -> [ServiceHandler])
  -> IO ()
runServer opts sc mkPreAuthedHandlers mkAuthedHandlers =
  startTCPServer opts $ \(s, peer) -> do
    -- Since the Reader is thread-unsafe, for each connection we create a new
    -- Reader.
    om <- initOffsetReader $ scOffsetManager sc
    let sc' = sc{scOffsetManager = om}

    -- Decide if we require SASL authentication
    case (serverSaslOptions opts) of
      Nothing -> do
        void $ State.execStateT (talk (peer, mkAuthedHandlers sc') s) ""
      Just _  -> do
        void $ (`State.execStateT` "") $ do
          doAuth sc' peer s >>= \case
            Security.SaslStateComplete ->
              talk (peer, mkAuthedHandlers sc') s
            ss -> do
              liftIO $ Log.fatal $ "[SASL] authenticate failed with state " <> Log.buildString' ss
  where
    doAuth sc_ peer s = do
      let recv = KIO.recvKafkaMsgBS peer Nothing s
          send = KIO.sendKafkaMsgBS s
      Security.authenticate sc_
                            (runHandler peer (mkPreAuthedHandlers sc_))
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
      P.incCounter totalRequests
      let ServiceHandler{..} = findHandler handlers requestApiKey requestApiVersion
      case rpcHandler of
        UnaryHandler rpcHandler' -> do
          observeWithLabel handlerLatencies (T.pack $ show requestApiKey) $ doUnaryHandler reqBs reqHeader rpcHandler' peer

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

    findHandler handlers apikey@(ApiKey key) version = do
      let m_handler = find (\ServiceHandler{..} ->
            rpcMethod == (key, version)) handlers
          errmsg = "NotImplemented: " <> show apikey <> ":v" <> show version
      fromMaybe (error errmsg) m_handler

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
-- Client

data ClientOptions = ClientOptions
  { host         :: !String
  , port         :: !Int
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
