{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.AppenderSpec (spec) where

import           Control.Exception
import           Test.Hspec
import           Z.Data.Vector        (Bytes, packASCII)

import qualified HStream.Store.Logger as S
import qualified HStream.Store.Stream as S

spec :: Spec
spec = describe "HStream.Store.Stream" $ do
  it "append and read" $
    (do _ <- S.setLogDeviceDbgLevel S.C_DBG_ERROR
        let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        _ <- S.appendSync client topicid "hello" Nothing
        readLastPayload client topicid
    ) `shouldReturn` "hello"

  it "get default payload size for this client" $
    (do client <- S.newStreamClient "/data/store/logdevice.conf"
        S.getMaxPayloadSize client
    ) `shouldReturn` (1024 * 1024)    -- 1MB

  it "modify default payload size for this client" $
    (do client <- S.newStreamClient "/data/store/logdevice.conf"
        S.setClientSettings client "max-payload-size" "1024" -- minimum value: 16
        S.getMaxPayloadSize client
    ) `shouldReturn` 1024

  it "cannot write exceed 1024 bytes "  $
    (do client <- S.newStreamClient "/data/store/logdevice.conf"
        S.setClientSettings client "max-payload-size" "1024" -- minimum value: 16
        let topicid = S.mkTopicID 1
        v <- try $ S.appendSync client topicid (packASCII $ replicate 1024 'a') Nothing
        r1 <- case v of
          Left (_ :: SomeException) -> return False
          Right _                   -> return  True
        v1 <- try $ S.appendSync client topicid (packASCII $ replicate 1025 'a') Nothing
        r2 <- case v1 of
          Left (_ :: SomeException) -> return False
          Right _                   -> return  True
        return (r1, r2)
    ) `shouldReturn` (True, False)

  it "get tail sequenceNum" $
    (do let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        seqNum0 <- S.appendSync client topicid "hello" Nothing
        seqNum1 <- S.getTailSequenceNum client topicid
        return $ seqNum0 == seqNum1
    ) `shouldReturn` True

readLastPayload :: S.StreamClient -> S.TopicID -> IO Bytes
readLastPayload client topicid = do
  sn <- S.getTailSequenceNum client topicid
  reader <- S.newStreamReader client 1 (-1)
  S.readerStartReading reader topicid sn sn
  xs <- S.readerRead reader 10
  return $ S.recordPayload $ head xs
