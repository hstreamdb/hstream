module Kafka.Server
  ( ServerOptions
  , defaultServerOpts
  , runServer
  ) where

import           Control.Concurrent
import qualified Control.Exception              as E
import           Control.Monad
import           Data.ByteString                (ByteString)
import qualified Data.ByteString.Lazy           as BSL
import           Data.Int
import           Data.List                      (find)
import           Data.Maybe                     (fromMaybe)
import qualified Network.Socket                 as N
import qualified Network.Socket.ByteString      as N
import qualified Network.Socket.ByteString.Lazy as NL

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Message
import           Kafka.Protocol.Service

data ServerOptions = ServerOptions
  { serverHost           :: !String
  , serverPort           :: !Int
  , serverOnStarted      :: !(Maybe (IO ()))
  , serverBufferChanSize :: !Word
  }

defaultServerOpts :: ServerOptions
defaultServerOpts = ServerOptions
  { serverHost           = "127.0.0.1"
  , serverPort           = 9092
  , serverOnStarted      = Nothing
  , serverBufferChanSize = 64
  }

-- TODO: This server primarily serves as a demonstration, and there is
-- certainly room for enhancements and refinements.
runServer :: ServerOptions -> [ServiceHandler] -> IO a
runServer ServerOptions{..} handlers =
  startTCPServer (Just serverHost) (show serverPort) (talk "" Nothing)
  where
    talk i m_more s = do
      reqBsResult <- case m_more of
                       Nothing -> runParser @ByteString get i
                       Just mf -> mf i
      case reqBsResult of
        Done "" reqBs -> do respBs <- runHandler reqBs
                            NL.sendAll s respBs
                            msg <- N.recv s 1024
                            talk msg Nothing s
        Done l reqBs -> do respBs <- runHandler reqBs
                           NL.sendAll s respBs
                           talk l Nothing s
        More f -> do msg <- N.recv s 1024
                     talk msg (Just f) s
        Fail _ err -> E.throwIO $ DecodeError $ "Fail, " <> err

    runHandler reqBs = do
      headerResult <- runParser @RequestHeader get reqBs
      case headerResult of
        Done l requestHeader -> do
          let ServiceHandler{..} = findHandler requestHeader
          case rpcHandler of
            UnaryHandler rpcHandler' -> do
              req <- runGet l
              resp <- rpcHandler' RequestContext req
              let respBs = runPutLazy resp
                  respHeaderBs = runPutLazy $ ResponseHeader (requestCorrelationId requestHeader)
              let len = BSL.length (respHeaderBs <> respBs)
                  lenBs = runPutLazy @Int32 (fromIntegral len)
              pure $ lenBs <> respHeaderBs <> respBs
        Fail _ err -> E.throwIO $ DecodeError $ "Fail, " <> err
        More _ -> E.throwIO $ DecodeError $ "More"

    findHandler RequestHeader{..} = do
      let m_handler = find (\ServiceHandler{..} ->
            rpcMethod == (fromIntegral requestApiKey, requestApiVersion)) handlers
          errmsg = "NotImplemented: " <> show requestApiKey <> ":v" <> show requestApiVersion
      fromMaybe (error errmsg) m_handler

-- from the "network-run" package.
startTCPServer :: Maybe N.HostName -> N.ServiceName -> (N.Socket -> IO a) -> IO a
startTCPServer mhost port server = do
  addr <- resolve
  E.bracket (open addr) N.close loop
  where
    resolve = do
      let hints = N.defaultHints
            { N.addrFlags = [N.AI_PASSIVE]
            , N.addrSocketType = N.Stream
            }
      head <$> N.getAddrInfo (Just hints) mhost (Just port)
    open addr = E.bracketOnError (N.openSocket addr) N.close $ \sock -> do
      N.setSocketOption sock N.ReuseAddr 1
      N.withFdSocket sock N.setCloseOnExecIfNeeded
      N.bind sock $ N.addrAddress addr
      N.listen sock 1024
      return sock
    loop sock = forever $ E.bracketOnError (N.accept sock) (N.close . fst)
      $ \(conn, _peer) -> void $
        -- 'forkFinally' alone is unlikely to fail thus leaking @conn@,
        -- but 'E.bracketOnError' above will be necessary if some
        -- non-atomic setups (e.g. spawning a subprocess to handle
        -- @conn@) before proper cleanup of @conn@ is your case
        forkFinally (server conn) $ \e -> do
          case e of
            Left err -> print err >> N.gracefulClose conn 5000
            Right _  -> pure ()
