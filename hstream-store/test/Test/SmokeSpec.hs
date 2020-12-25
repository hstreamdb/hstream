{-# LANGUAGE OverloadedStrings #-}

module Test.SmokeSpec (spec) where

import qualified HStream.Store           as S
import qualified HStream.Store.Exception as E
import           Test.Hspec

spec :: Spec
spec = describe "SmokeTest" $ do
  it "Append Something should return a valid sequence number." $
    (do _ <- S.setLoggerlevelError
        client <- S.newStreamClient "/data/store/logdevice.conf"
        let logid = S.mkTopicID 1
        S.append client logid "hello"
    ) `shouldNotReturn` S.sequenceNumInvalid
