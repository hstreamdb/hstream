{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE TypeFamilies          #-}
-- Indeed, some constraints are needed but ghc thinks not.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module HStream.Kafka.Network.IO
  ( recvKafkaMsgBS
  , sendKafkaMsgBS
  , packKafkaMsgBs
  ) where

import qualified Control.Exception              as E
import           Control.Monad.IO.Class
import qualified Control.Monad.State.Class      as State
import           Data.ByteString                (ByteString)
import qualified Data.ByteString                as BS
import qualified Data.ByteString.Lazy           as BSL
import           Data.Int
import qualified Network.Socket                 as N
import qualified Network.Socket.ByteString      as N
import qualified Network.Socket.ByteString.Lazy as NL

import           Kafka.Protocol.Encoding
import           Kafka.Protocol.Error
import           Kafka.Protocol.Message

-- | Receive a kafka message with its request header from socket.
--   If the socket is closed, return Nothing.
--   The "state" is the left bytes in TCP channel.
recvKafkaMsgBS :: (MonadIO m, State.MonadState ByteString m)
               => N.SockAddr
               -> Maybe (ByteString -> IO (Result ByteString))
               -> N.Socket
               -> m (Maybe (RequestHeader, ByteString))
recvKafkaMsgBS peer m_more s = do
  -- Note: this decides if we should recv more data from socket.
  --       If tried but still got nothing, return Nothing, which means
  --       the socket is closed.
  i <- do i_0 <- State.get
          if BS.null i_0
            then do i' <- liftIO $ N.recv s 1024
                    if BS.null i' then return mempty else do
                      State.put i'
                      return i'
            else return i_0

  if BS.null i then return Nothing else do
    reqBsResult <- case m_more of
                     Nothing -> liftIO $ runParser @ByteString get i
                     Just mf -> liftIO $ mf i
    case reqBsResult of
      Done "" reqBs -> do
        State.put ""
        headerResult <- liftIO $ runParser @RequestHeader get reqBs
        case headerResult of
          Done l h   -> return $ Just (h, l)
          Fail _ err -> E.throw $ DecodeError $ (CORRUPT_MESSAGE, "Fail, " <> err)
          More _     -> E.throw $ DecodeError $ (CORRUPT_MESSAGE, "More")
      Done l reqBs -> do
        State.put l
        headerResult <- liftIO $ runParser @RequestHeader get reqBs
        case headerResult of
          Done l' h  -> return $ Just (h, l')
          Fail _ err -> E.throw $ DecodeError $ (CORRUPT_MESSAGE, "Fail, " <> err)
          More _     -> E.throw $ DecodeError $ (CORRUPT_MESSAGE, "More")
      More f -> do
        i_new <- liftIO $ N.recv s 1024
        State.put i_new
        recvKafkaMsgBS peer (Just f) s
      Fail _ err -> liftIO . E.throwIO $ DecodeError $ (CORRUPT_MESSAGE, "Fail, " <> err)

-- | Send a kafka message to socket. Note the message should be packed
--   with its response header.
sendKafkaMsgBS :: MonadIO m => N.Socket -> BSL.ByteString -> m ()
sendKafkaMsgBS s bs =
  liftIO $ NL.sendAll s bs

-- | Pack a kafka response with its response header. It receives a request
--   header to get the correlation id and header version.
packKafkaMsgBs :: (Serializable a)
               => RequestHeader
               -> a
               -> BSL.ByteString
packKafkaMsgBs RequestHeader{..} resp = do
  let respBs = runPutLazy resp
      (_, respHeaderVer) = getHeaderVersion requestApiKey requestApiVersion
      respHeaderBs =
        case respHeaderVer of
          0 -> runPutResponseHeaderLazy $ ResponseHeader requestCorrelationId Nothing
          1 -> runPutResponseHeaderLazy $ ResponseHeader requestCorrelationId (Just EmptyTaggedFields)
          _ -> error $ "Unknown response header version " <> show respHeaderVer
   in let len = BSL.length (respHeaderBs <> respBs)
          lenBs = runPutLazy @Int32 (fromIntegral len)
       in lenBs <> respHeaderBs <> respBs
