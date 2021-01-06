{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HStream.Store.AppenderSpec (spec) where

import           Control.Exception
import           Control.Monad.IO.Class
import qualified HStream.Store.Exception as E
import qualified HStream.Store.Stream    as S
import           Test.Hspec
import           Z.Data.Vector           (packASCII)

spec :: Spec
spec = describe "HStream.Store.Stream" $ do
  it "append and read" $
    (do _ <- S.setLoggerlevelError
        let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        S.appendSync client topicid "hello" Nothing
        readLastPayload client topicid
    ) `shouldReturn` "hello"

  it "get default payload size for this client" $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
        S.getMaxPayloadSize client
    ) `shouldReturn` (1024 ^ 2)

  it "modify default payload size for this client" $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
        S.setClientSettings client "max-payload-size" "1024" -- minimum value: 16
        S.getMaxPayloadSize client
    ) `shouldReturn` 1024

  it "cannot write exceed 1024 bytes "  $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
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

  it "create topic directory" $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
        at <- S.newTopicAttributes
        sg <- S.makeTopicDirectory client "a/a" at True
        S.topicDirectoryGetName sg
    ) `shouldReturn` "a"

  it "create topic group sync" $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
        at <- S.newTopicAttributes
        S.setTopicReplicationFactor at 3
        let st = S.mkTopicID 1000
            end = S.mkTopicID 1000
        gs <- S.makeTopicGroupSync client "a/a/topic" st end at True
        (a,b) <- S.topicGroupGetRange gs
        name <- S.topicGroupGetName gs
        return (a,b,name)
    ) `shouldReturn` (S.mkTopicID 1000, S.mkTopicID 1000, "topic")

  it "remove topic group sync" $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
        at <- S.newTopicAttributes
        S.setTopicReplicationFactor at 3
        let st = S.mkTopicID 1001
            end = S.mkTopicID 1001
        gs <- S.makeTopicGroupSync client "a/a/topic1" st end at True
        (a,b) <- S.topicGroupGetRange gs
        name <- S.topicGroupGetName gs
        S.removeTopicGroupSync client "a/a/topic1"
        Left (e :: SomeException) <- try $ S.getTopicGroupSync client "a/a/topic1"
        return (a,b,name)
    ) `shouldReturn` (S.mkTopicID 1001, S.mkTopicID 1001, "topic1")

readLastPayload :: S.StreamClient -> S.TopicID -> IO S.Bytes
readLastPayload client topicid = do
  sn <- S.getTailSequenceNum client topicid
  reader <- S.newStreamReader client 1 (-1)
  S.readerStartReading reader topicid sn sn
  xs <- S.readerRead reader 10
  return $ S.recordPayload $ head xs
