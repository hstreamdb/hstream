{-# LANGUAGE OverloadedStrings #-}

module Test.TCPSpec (spec) where

import           Control.Concurrent (forkIO, newEmptyMVar, putMVar, takeMVar,
                                     threadDelay)
import qualified Data.Vector        as V
import           Test.Hspec

import qualified Network.HESP       as P

spec :: Spec
spec = do
  runClientException
  parallel smoke

smoke :: Spec
smoke = describe "SmokeTest" $ do
  it "send and recv" $ do
    let source = P.mkBulkString "hello"
    response <- newEmptyMVar
    -- FIXME: choose an unused port automatically
    let host = "localhost"
        port = "6561"  -- NOTE: do NOT use 6560, since it may already have a server running on default 6560 port.
    _ <- forkIO $ P.runTCPServer host port $ \(sock, _) -> do
      msgs <- P.recvMsgs sock 1024
      case V.head msgs of
        Left _err -> P.sendMsg sock $ P.mkBulkString "err"
        Right msg -> P.sendMsg sock msg
    -- FIXME: Here we sleep 1 second to wait the server start,
    -- a better way is there should an event that indicate server started.
    threadDelay 1000000
    P.connect host port $ \(sock, _) -> do
      P.sendMsg sock source
      msgs <- P.recvMsgs sock 1024
      putMVar response $ V.head msgs
    takeMVar response `shouldReturn` Right source

runClientException :: Spec
runClientException = describe "RunClientException" $
  it "connect to a not exist port should throw exception" $
    P.connect "localhost" "0" undefined `shouldThrow` anyIOException
