{-# LANGUAGE OverloadedStrings #-}

module HStream.Store.AppenderSpec (spec) where

import qualified HStream.Store.Exception as E
import qualified HStream.Store.Stream    as S
import           Test.Hspec

spec :: Spec
spec = describe "" $ do
  it "" $
    (do _ <- S.setLoggerlevelError
        let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        S.appendSync client topicid "hello" Nothing
        readLastPayload client topicid
    ) `shouldReturn` "hello"

readLastPayload :: S.StreamClient -> S.TopicID -> IO S.Bytes
readLastPayload client topicid = do
  sn <- S.getTailSequenceNum client topicid
  reader <- S.newStreamReader client 1 (-1)
  S.readerStartReading reader topicid sn sn
  xs <- S.readerRead reader 10
  return $ S.recordPayload $ head xs
