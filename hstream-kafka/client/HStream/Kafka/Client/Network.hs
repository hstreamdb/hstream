module HStream.Kafka.Client.Network where

import           Control.Exception
import           Control.Monad
import qualified Data.ByteString                as BS
import           Data.Int
import qualified Data.Vector                    as V
import qualified Kafka.Protocol.Encoding        as K
import qualified Kafka.Protocol.Message         as K
import qualified Network.Socket                 as NW
import qualified Network.Socket.ByteString      as NW
import           Options.Applicative            as AP

import           HStream.Kafka.Client.CliParser

defaultAddr = "127.0.0.1"
defaultPort = 46721

withSock :: Options -> (NW.Socket -> IO a) -> IO a
withSock opts talk = do
  addr <- resolve
  bracket (open addr) NW.close talk
  where
    resolve = do
      let hints = NW.defaultHints
            { NW.addrFlags      = [NW.AI_PASSIVE]
            , NW.addrSocketType = NW.Stream
            }
      head <$> NW.getAddrInfo (Just hints) (Just defaultAddr) (Just $ show defaultPort)

    open addr = do
      sock <- NW.socket (NW.addrFamily addr) (NW.addrSocketType addr) (NW.addrProtocol addr)
      NW.connect sock $ NW.addrAddress addr
      pure sock

recvSizeLimit = 1024

recvByteString :: NW.Socket -> IO BS.ByteString
recvByteString sock = do
  i <- NW.recv sock recvSizeLimit
  ret <- K.runParser @BS.ByteString K.get i
  h ret
  where
  h ret = case ret of
      K.Done l bs -> pure bs
      K.More k    -> do
        i <- NW.recv sock recvSizeLimit
        h =<< k i
      K.Fail _ err -> error $ show err

recvResp :: forall a. K.Serializable a => NW.Socket -> IO a
recvResp sock = do
  bs <- recvByteString sock
  ret <- K.runParser K.getResponseHeaderV0 bs
  (l, header) <- case ret of
    K.Done l header -> pure (l, header)
    K.More k        -> undefined
    K.Fail _ err    -> undefined
  ret' <- K.runParser @a K.get l
  resp <- case ret' of
    K.Done l resp -> pure resp -- FIXME: left
    K.More k      -> undefined
    K.Fail _ err  -> error $ show err
  pure resp

sendAndRecv :: forall req resp header.
  ( K.Serializable req
  , K.Serializable resp
  , K.Serializable header
  ) => Options -> header -> req -> IO resp
sendAndRecv opts reqHeader reqBody = withSock opts $ \sock -> do
  let reqBs' = K.runPut reqHeader <> K.runPut @req reqBody
      reqLen = K.runPut @Int32 $ fromIntegral (BS.length reqBs')
      reqBs  = reqLen <> reqBs'
  NW.sendAll sock reqBs
  recvResp @resp sock
