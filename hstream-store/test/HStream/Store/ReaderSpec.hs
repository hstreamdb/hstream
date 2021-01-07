{-# LANGUAGE OverloadedStrings #-}

module HStream.Store.ReaderSpec (spec) where

import           Control.Monad  (void)
import           System.Timeout (timeout)
import           Test.Hspec

import qualified HStream.Store  as S

-- TODO
spec :: Spec
spec = describe "HStream.Store.Stream.Reader" $ do
  it "reader timeout" $
    (do _ <- S.setLoggerlevelError
        let topicid = S.mkTopicID 1
        client <- S.newStreamClient "/data/store/logdevice.conf"
        readerTimeout client topicid
    ) `shouldReturn` True

readerTimeout :: S.StreamClient -> S.TopicID -> IO Bool
readerTimeout client topicid = do
  reader <- S.newStreamReader client 1 (-1)
  sn <- S.getTailSequenceNum client topicid
  S.readerStartReading reader topicid (sn + 1) maxBound
  void $ S.readerSetTimeout reader 100
  result <- timeout 1000000 (S.readerRead reader 1)
  case result of
    Just _ -> return True
    _      -> return False
