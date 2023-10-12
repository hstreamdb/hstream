{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}
-- Indeed, some constraints are needed but ghc thinks not.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module HStream.Kafka.Network
  ( -- * Server
    ServerOptions (..)
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
import qualified Control.Exception                  as E
import           Control.Monad
import           Data.ByteString                    (ByteString)
import qualified Data.ByteString.Lazy               as BSL
import           Data.Int
import           Data.List                          (find)
import           Data.Maybe                         (fromMaybe, isJust,
                                                     isNothing)
import qualified Network.Socket                     as N
import qualified Network.Socket.ByteString          as N
import qualified Network.Socket.ByteString.Lazy     as NL

import           HStream.Kafka.Common.OffsetManager (initOffsetReader)
import           HStream.Kafka.Server.Types         (ServerContext (..))
import qualified HStream.Logger                     as Log
import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message
import           Kafka.Protocol.Service

-- TODO
data SslOptions

data ServerOptions = ServerOptions
  { serverHost       :: !String
  , serverPort       :: !Int
  , serverSslOptions :: !(Maybe SslOptions)
  , serverOnStarted  :: !(Maybe (IO ()))
  }

defaultServerOpts :: ServerOptions
defaultServerOpts = ServerOptions
  { serverHost           = "0.0.0.0"
  , serverPort           = 9092
  , serverSslOptions     = Nothing
  , serverOnStarted      = Nothing
  }

-- TODO: This server primarily serves as a demonstration, and there is
-- certainly room for enhancements and refinements.
runServer
  :: ServerOptions
  -> ServerContext
  -> (ServerContext -> [ServiceHandler])
  -> IO ()
runServer opts sc mkHandlers =
  startTCPServer opts $ \s -> do
    -- Since the Reader is thread-unsafe, for each connection we create a new
    -- Reader.
    om <- initOffsetReader $ scOffsetManager sc
    let sc' = sc{scOffsetManager = om}
    i <- N.recv s 1024
    talk (mkHandlers sc') i Nothing s
  where
    talk _ "" _ _ = pure ()  -- client exit
    talk hds i m_more s = do
      reqBsResult <- case m_more of
                       Nothing -> runParser @ByteString get i
                       Just mf -> mf i
      case reqBsResult of
        Done "" reqBs -> do respBs <- runHandler hds reqBs
                            NL.sendAll s respBs
                            msg <- N.recv s 1024
                            talk hds msg Nothing s
        Done l reqBs -> do respBs <- runHandler hds reqBs
                           NL.sendAll s respBs
                           talk hds l Nothing s
        More f -> do msg <- N.recv s 1024
                     talk hds msg (Just f) s
        Fail _ err -> E.throwIO $ DecodeError $ "Fail, " <> err

    runHandler handlers reqBs = do
      headerResult <- runParser @RequestHeader get reqBs
      case headerResult of
        Done l RequestHeader{..} -> do
          let ServiceHandler{..} = findHandler handlers requestApiKey requestApiVersion
          case rpcHandler of
            UnaryHandler rpcHandler' -> do
              req <- runGet l
              Log.debug $ "Received request "
                       <> Log.buildString' requestApiKey
                       <> ":v" <> Log.build requestApiVersion
                       <> ", payload: " <> Log.buildString' req
              resp <- rpcHandler' RequestContext req
              Log.debug $ "Server response: " <> Log.buildString' resp
              let respBs = runPutLazy resp
                  (_, respHeaderVer) = getHeaderVersion requestApiKey requestApiVersion
                  respHeaderBs =
                    case respHeaderVer of
                      0 -> runPutResponseHeaderLazy $ ResponseHeader requestCorrelationId Nothing
                      1 -> runPutResponseHeaderLazy $ ResponseHeader requestCorrelationId (Just EmptyTaggedFields)
                      _ -> error $ "Unknown response header version " <> show respHeaderVer
              let len = BSL.length (respHeaderBs <> respBs)
                  lenBs = runPutLazy @Int32 (fromIntegral len)
              pure $ lenBs <> respHeaderBs <> respBs
        Fail _ err -> E.throwIO $ DecodeError $ "Fail, " <> err
        More _ -> E.throwIO $ DecodeError $ "More"

    findHandler handlers apikey@(ApiKey key) version = do
      let m_handler = find (\ServiceHandler{..} ->
            rpcMethod == (key, version)) handlers
          errmsg = "NotImplemented: " <> show apikey <> ":v" <> show version
      fromMaybe (error errmsg) m_handler

startTCPServer :: ServerOptions -> (N.Socket -> IO a) -> IO a
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
        Log.info $ "Recv client connection: " <> Log.buildString' peer
        -- 'forkFinally' alone is unlikely to fail thus leaking @conn@,
        -- but 'E.bracketOnError' above will be necessary if some
        -- non-atomic setups (e.g. spawning a subprocess to handle
        -- @conn@) before proper cleanup of @conn@ is your case
        forkFinally (server conn) $ \e -> do
          case e of
            Left err -> print err >> N.gracefulClose conn 5000
            Right _  -> pure ()

-------------------------------------------------------------------------------

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
